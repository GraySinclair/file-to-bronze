from __future__ import annotations

import json
import re
from collections.abc import Mapping

import notebookutils
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, lit, row_number
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from .bronze_load_config import BronzeLoadConfig
from .column_normalization import normalize_columns


class BronzeLoader:
    """Load landed files into Bronze Delta tables.

    append and upsert use an available-now stream so the checkpoint
    records which files have already been processed.

    snapshot uses a batch read of all files currently in the source data
    path. It does not use a checkpoint, so every load represents the full
    current snapshot.

    Source schemas are loaded from:

        Files/{source_system}/_misc/schemas/{table_name}.json

    unless a StructType is explicitly passed to load().
    """

    IS_DELETED = "_is_deleted"

    _DELETE_FLAG = "_delete_flag"
    _ROW_NUMBER = "_row_number"

    _VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(
        self,
        spark: SparkSession,
        *,
        bronze_lakehouse: str = "Bronze",
        files_root: str = "Files",
        file_format: str = "json",
        reader_options: Mapping[str, str] | None = None,
        enable_cdf: bool = True,
        allow_schema_evolution: bool = True,
    ) -> None:
        self.spark = spark

        self.bronze_lakehouse = self._validate_identifier(bronze_lakehouse, "bronze_lakehouse")

        self.files_root = files_root.rstrip("/")

        self.file_format = file_format
        self.reader_options = dict(reader_options or {})
        self.enable_cdf = enable_cdf
        self.allow_schema_evolution = allow_schema_evolution

        if allow_schema_evolution:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        if enable_cdf:
            spark.conf.set(
                "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
                "true",
            )

    def load(
        self,
        config: BronzeLoadConfig,
        *,
        source_path: str | None = None,
        schema: StructType | None = None,
    ) -> None:
        """Process one configured table.

        Schema resolution order:

        1. Explicit schema passed to load().
        2. Files/{source_system}/_misc/schemas/{table_name}.json.

        Column names referenced by BronzeLoadConfig must use the final
        normalized Bronze column names.
        """
        self._validate_config(config)

        source_path = source_path or self._default_source_path(config)
        schema = schema or self._load_schema(config)

        if config.load_mode == "snapshot":
            self._load_snapshot(config, source_path, schema)
        else:
            self._load_incremental(config, source_path, schema)

    def _load_incremental(
        self,
        config: BronzeLoadConfig,
        source_path: str,
        schema: StructType,
    ) -> None:
        # Create a missing target before entering foreachBatch.
        # Fabric can lose the active catalog session when saveAsTable()
        # creates a managed table from inside the Python callback.
        self._ensure_namespace(config)

        target_table = self._target_table(config)

        if not self.spark.catalog.tableExists(target_table):
            bootstrap_df = self.spark.createDataFrame([], schema)
            bootstrap_df = self._prepare_source(bootstrap_df)
            bootstrap_df = bootstrap_df.withColumn(self._DELETE_FLAG, lit(False))

            soft_delete = config.soft_delete and config.delete_column is not None

            self._create_target(bootstrap_df, target_table, soft_delete=soft_delete)

            self._set_cdf_property(target_table)

        source_df = (
            self.spark.readStream
            .format(self.file_format)
            .schema(schema)
            .options(**self.reader_options)
            .load(source_path)
        )

        source_df = self._prepare_source(source_df)

        def process_batch(batch_df: DataFrame, _: int) -> None:
            if batch_df.isEmpty():
                return

            self._write_batch(config, batch_df, snapshot=False)

        query = (
            source_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", self._checkpoint_path(config))
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

    def _load_snapshot(
        self,
        config: BronzeLoadConfig,
        source_path: str,
        schema: StructType,
    ) -> None:
        source_df = (
            self.spark.read
            .format(self.file_format)
            .schema(schema)
            .options(**self.reader_options)
            .load(source_path)
        )

        source_df = self._prepare_source(source_df)

        self._write_batch(config, source_df, snapshot=True)

    def _write_batch(
        self,
        config: BronzeLoadConfig,
        source_df: DataFrame,
        *,
        snapshot: bool,
    ) -> None:
        self._ensure_namespace(config)

        if config.load_mode == "append":
            self._append(config, source_df)
            return

        self._merge(config, source_df, snapshot=snapshot)

    def _append(self, config: BronzeLoadConfig, source_df: DataFrame) -> None:
        target_table = self._target_table(config)

        writer = source_df.write.format("delta").mode("append")

        if self.allow_schema_evolution:
            writer = writer.option("mergeSchema", "true")

        writer.saveAsTable(target_table)

        self._set_cdf_property(target_table)

    def _merge(self, config: BronzeLoadConfig, source_df: DataFrame, *, snapshot: bool) -> None:
        target_table = self._target_table(config)

        merge_keys = config.merge_keys
        sequence_column = config.sequence_column
        delete_column = config.delete_column

        soft_delete = config.soft_delete and (snapshot or delete_column is not None)

        self._require_columns(
            source_df,
            [
                *merge_keys,
                *([sequence_column] if sequence_column else []),
                *([delete_column] if delete_column else []),
            ],
        )

        source_df = self._deduplicate(source_df, merge_keys, sequence_column)

        source_df = source_df.withColumn(
            self._DELETE_FLAG,
            (
                coalesce(col(delete_column).cast("boolean"), lit(False))
                if delete_column
                else lit(False)
            ),
        )

        if not self.spark.catalog.tableExists(target_table):
            self._create_target(source_df, target_table, soft_delete=soft_delete)

        if soft_delete:
            self._ensure_soft_delete_columns(target_table)

        self._set_cdf_property(target_table)

        merge_condition = " AND ".join(f"t.{key} = s.{key}" for key in merge_keys)

        source_columns = [name for name in source_df.columns if name != self._DELETE_FLAG]

        active_values = {name: f"s.{name}" for name in source_columns}

        if soft_delete:
            active_values[self.IS_DELETED] = "false"

        builder = (
            DeltaTable
            .forName(self.spark, target_table)
            .alias("t")
            .merge(source_df.alias("s"), merge_condition)
        )

        if delete_column:
            if soft_delete:
                builder = builder.whenMatchedUpdate(
                    condition=f"s.{self._DELETE_FLAG} = true",
                    set={self.IS_DELETED: "true"},
                )
            else:
                builder = builder.whenMatchedDelete(condition=f"s.{self._DELETE_FLAG} = true")

            builder = (
                builder
                .whenMatchedUpdate(condition=f"s.{self._DELETE_FLAG} = false", set=active_values)
                .whenNotMatchedInsert(
                    condition=f"s.{self._DELETE_FLAG} = false",
                    values=active_values,
                )
            )

        else:
            builder = (
                builder
                .whenMatchedUpdate(set=active_values)
                .whenNotMatchedInsert(values=active_values)
            )

        if snapshot:
            if soft_delete:
                builder = builder.whenNotMatchedBySourceUpdate(set={self.IS_DELETED: "true"})
            else:
                builder = builder.whenNotMatchedBySourceDelete()

        builder.execute()

    def _create_target(self, source_df: DataFrame, target_table: str, *, soft_delete: bool) -> None:
        initial_df = source_df.filter(col(self._DELETE_FLAG) == lit(False)).drop(self._DELETE_FLAG)

        if soft_delete:
            initial_df = initial_df.withColumn(self.IS_DELETED, lit(False))

        initial_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

    def _prepare_source(self, source_df: DataFrame) -> DataFrame:
        return normalize_columns(source_df)

    def _deduplicate(
        self,
        source_df: DataFrame,
        merge_keys: tuple[str, ...],
        sequence_column: str | None,
    ) -> DataFrame:
        if sequence_column:
            window = Window.partitionBy(*merge_keys).orderBy(col(sequence_column).desc_nulls_last())

            return (
                source_df
                .withColumn(self._ROW_NUMBER, row_number().over(window))
                .filter(col(self._ROW_NUMBER) == 1)
                .drop(self._ROW_NUMBER)
            )

        has_duplicates = bool(
            source_df
            .groupBy(*merge_keys)
            .count()
            .filter(col("count") > 1)
            .take(1)
        )

        if has_duplicates:
            raise ValueError(
                "Duplicate merge keys were found. "
                "Set sequence_column so the newest source row "
                "can be selected deterministically."
            )

        return source_df

    def _load_schema(self, config: BronzeLoadConfig) -> StructType:
        """Load the table's Spark StructType schema from Lakehouse Files."""
        schema_path = self._schema_path(config)

        if not notebookutils.fs.exists(schema_path):
            raise FileNotFoundError(
                "No Bronze schema was found for "
                f"{config.source_system}.{config.table_name}. "
                f"Expected schema at: {schema_path}"
            )

        try:
            schema_json = json.loads(notebookutils.fs.head(schema_path))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON schema at: {schema_path}") from exc

        try:
            return StructType.fromJson(schema_json)
        except (TypeError, ValueError, KeyError) as exc:
            raise ValueError(
                "Schema file does not contain a valid Spark "
                f"StructType definition: {schema_path}"
            ) from exc

    def _ensure_namespace(self, config: BronzeLoadConfig) -> None:
        self.spark.sql(
            "CREATE SCHEMA IF NOT EXISTS "
            f"{self.bronze_lakehouse}.{config.source_system}"
        )

    def _ensure_soft_delete_columns(self, target_table: str) -> None:
        existing = set(self.spark.table(target_table).columns)

        missing = []

        if self.IS_DELETED not in existing:
            missing.append(f"{self.IS_DELETED} BOOLEAN")

        if missing:
            self.spark.sql(f"ALTER TABLE {target_table} " f"ADD COLUMNS ({', '.join(missing)})")

    def _set_cdf_property(self, target_table: str) -> None:
        if self.enable_cdf:
            self.spark.sql(
                f"ALTER TABLE {target_table} "
                "SET TBLPROPERTIES "
                "(delta.enableChangeDataFeed = 'true')"
            )

    def _validate_config(self, config: BronzeLoadConfig) -> None:
        self._validate_identifier(config.source_system, "source_system")

        self._validate_identifier(config.table_name, "table_name")

        if config.load_mode not in {"append", "upsert", "snapshot"}:
            raise ValueError("load_mode must be append, upsert, or snapshot.")

        if config.load_mode in {"upsert", "snapshot"} and not config.merge_keys:
            raise ValueError(f"{config.load_mode} mode requires merge_keys.")

        if config.load_mode == "append" and config.delete_column:
            raise ValueError("delete_column is not supported for append mode.")

    @staticmethod
    def _require_columns(source_df: DataFrame, required_columns: list[str]) -> None:
        missing = sorted(set(required_columns).difference(source_df.columns))

        if missing:
            raise ValueError("Source data is missing required columns: " + ", ".join(missing))

    def _target_table(self, config: BronzeLoadConfig) -> str:
        return f"{self.bronze_lakehouse}.{config.source_system}.{config.table_name}"

    def _default_source_path(self, config: BronzeLoadConfig) -> str:
        return f"{self.files_root}/{config.source_system}/{config.table_name}/data"

    def _schema_path(self, config: BronzeLoadConfig) -> str:
        return f"{self.files_root}/{config.source_system}/_misc/schemas/{config.table_name}.json"

    def _checkpoint_path(self, config: BronzeLoadConfig) -> str:
        return f"{self.files_root}/{config.source_system}/{config.table_name}/checkpoint"

    def _validate_identifier(self, value: str, name: str) -> str:
        if not self._VALID_IDENTIFIER.fullmatch(value):
            raise ValueError(
                f"{name} must contain only letters, numbers, "
                "and underscores and cannot begin with a number."
            )

        return value
