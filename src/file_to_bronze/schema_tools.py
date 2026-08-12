from __future__ import annotations

import json
import re
from collections.abc import Mapping

import notebookutils
from pyspark.sql import DataFrame, SparkSession

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
    """Load landed files using Spark-inferred data types.

    This is intended for initial schema development before BronzeLoader
    is used with an explicit schema.

    The returned DataFrame has normalized top-level column names using
    the shared column normalization logic.

    Default source path:

        Files/{source_system}/{table_name}/data
    """
    _validate_identifier(source_system, "source_system")
    _validate_identifier(table_name, "table_name")

    files_root = files_root.rstrip("/")

    source_path = source_path or (
        f"{files_root}/"
        f"{source_system}/"
        f"{table_name}/data"
    )

    df = (
        spark.read
        .format(file_format)
        .options(**dict(reader_options or {}))
        .load(source_path)
    )

    return normalize_columns(df)


def save_schema(
    df: DataFrame,
    *,
    source_system: str,
    table_name: str,
    schema_root: str = "Files/_schemas",
    overwrite: bool = True,
) -> str:
    """Save a DataFrame's StructType schema for BronzeLoader.

    The current DataFrame schema is serialized using Spark's native
    StructType JSON representation and written to:

        Files/_schemas/{source_system}/{table_name}

    Adjust or cast the DataFrame columns before calling this function
    if Spark's inferred data types are not the desired Bronze types.
    """
    _validate_identifier(source_system, "source_system")
    _validate_identifier(table_name, "table_name")

    schema_root = schema_root.rstrip("/")

    schema_directory = (
        f"{schema_root}/"
        f"{source_system}"
    )

    schema_path = (
        f"{schema_directory}/"
        f"{table_name}"
    )

    notebookutils.fs.mkdirs(schema_directory)

    schema_json = json.dumps(
        df.schema.jsonValue(),
        indent=2,
    )

    notebookutils.fs.put(
        schema_path,
        schema_json,
        overwrite=overwrite,
    )

    return schema_path


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
