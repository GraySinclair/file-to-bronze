from __future__ import annotations

import json
import re
from collections.abc import Mapping

import notebookutils
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from .column_normalization import normalize_columns


_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_inferred_dataframe(
    spark: SparkSession,
    *,
    source_system: str,
    table_name: str,
    source_path: str | None = None,
    files_root: str = "Files",
    file_format: str = "json",
    reader_options: Mapping[str, str] | None = None,
) -> DataFrame:
    """Load landed files with inferred data types and normalized columns.

    This function is intended for initial schema development.

    Spark first reads the raw source using schema inference. The returned
    DataFrame then has its top-level column names normalized using the
    shared column normalization logic.

    Default source path:

        Files/{source_system}/{table_name}/data
    """
    _validate_identifier(source_system, "source_system")
    _validate_identifier(table_name, "table_name")

    raw_df = _read_inferred_dataframe(
        spark,
        source_system=source_system,
        table_name=table_name,
        source_path=source_path,
        files_root=files_root,
        file_format=file_format,
        reader_options=reader_options,
    )

    return normalize_columns(raw_df)


def save_schema(
    df: DataFrame,
    *,
    source_system: str,
    table_name: str,
    source_path: str | None = None,
    files_root: str = "Files",
    schema_root: str = "Files/_schemas",
    file_format: str = "json",
    reader_options: Mapping[str, str] | None = None,
    overwrite: bool = True,
) -> str:
    """Save a raw-source read schema using the DataFrame's desired types.

    The supplied DataFrame is expected to have normalized column names.
    Its current data types are treated as the desired Bronze data types.

    The raw source is read again with schema inference so the original
    source column names can be recovered. Those raw names are paired with
    the desired data types from ``df`` and serialized as a Spark StructType.

    This is necessary because BronzeLoader applies the explicit schema
    while reading the raw files, before column normalization occurs.

    Schema is written to:

        Files/_schemas/{source_system}/{table_name}.json
    """
    _validate_identifier(source_system, "source_system")
    _validate_identifier(table_name, "table_name")

    raw_df = _read_inferred_dataframe(
        df.sparkSession,
        source_system=source_system,
        table_name=table_name,
        source_path=source_path,
        files_root=files_root,
        file_format=file_format,
        reader_options=reader_options,
    )

    normalized_raw_df = normalize_columns(raw_df)

    desired_fields = {
        field.name: field
        for field in df.schema.fields
    }

    expected_columns = set(normalized_raw_df.columns)
    actual_columns = set(df.columns)

    missing = sorted(expected_columns - actual_columns)
    extra = sorted(actual_columns - expected_columns)

    if missing or extra:
        details = []

        if missing:
            details.append(
                "missing normalized source columns: "
                + ", ".join(missing)
            )

        if extra:
            details.append(
                "unexpected columns: "
                + ", ".join(extra)
            )

        raise ValueError(
            "The DataFrame columns no longer match the normalized "
            "source schema; "
            + "; ".join(details)
        )

    raw_fields = []

    for raw_field, normalized_field in zip(
        raw_df.schema.fields,
        normalized_raw_df.schema.fields,
    ):
        desired_field = desired_fields[normalized_field.name]

        raw_fields.append(
            StructField(
                name=raw_field.name,
                dataType=desired_field.dataType,
                nullable=desired_field.nullable,
                metadata=desired_field.metadata,
            )
        )

    schema = StructType(raw_fields)

    schema_root = schema_root.rstrip("/")
    schema_directory = f"{schema_root}/{source_system}"
    schema_path = f"{schema_directory}/{table_name}.json"

    notebookutils.fs.mkdirs(schema_directory)

    notebookutils.fs.put(
        schema_path,
        json.dumps(
            schema.jsonValue(),
            indent=2,
        ),
        overwrite=overwrite,
    )

    return schema_path


def _read_inferred_dataframe(
    spark: SparkSession,
    *,
    source_system: str,
    table_name: str,
    source_path: str | None,
    files_root: str,
    file_format: str,
    reader_options: Mapping[str, str] | None,
) -> DataFrame:
    files_root = files_root.rstrip("/")

    source_path = source_path or (
        f"{files_root}/"
        f"{source_system}/"
        f"{table_name}/data"
    )

    return (
        spark.read
        .format(file_format)
        .options(**dict(reader_options or {}))
        .load(source_path)
    )


def _validate_identifier(
    value: str,
    name: str,
) -> str:
    if not _VALID_IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{name} must contain only letters, numbers, "
            "and underscores and cannot begin with a number."
        )

    return value
