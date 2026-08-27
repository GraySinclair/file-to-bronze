from __future__ import annotations

import json
import re
from collections.abc import Mapping

import notebookutils
from pyspark.sql import DataFrame, SparkSession

from .column_normalization import normalize_columns


_SCHEMA_VERSION = 2
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

    Spark reads the raw source using schema inference. The returned DataFrame
    then has its top-level column names normalized using the shared column
    normalization logic.

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
    file_format: str = "json",
    reader_options: Mapping[str, str] | None = None,
    overwrite: bool = True,
) -> str:
    """Save the physical source schema and desired Bronze schema.

    The supplied DataFrame must use normalized column names. Its current data
    types are stored as the desired Bronze data types.

    The raw source is read again with schema inference so its physical JSON
    types and original source column names are also captured. BronzeLoader
    uses that source schema to parse files, then normalizes and casts to the
    desired Bronze schema afterward.

    Schema is written to:

        Files/{source_system}/_misc/schemas/{table_name}.json
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
    expected_columns = set(normalized_raw_df.columns)
    actual_columns = set(df.columns)

    missing = sorted(expected_columns - actual_columns)
    extra = sorted(actual_columns - expected_columns)

    if missing or extra:
        details = []

        if missing:
            details.append("missing normalized source columns: " + ", ".join(missing))

        if extra:
            details.append("unexpected columns: " + ", ".join(extra))

        raise ValueError(
            "The DataFrame columns no longer match the normalized source schema; "
            + "; ".join(details)
        )

    schema_payload = {
        "version": _SCHEMA_VERSION,
        "source_schema": raw_df.schema.jsonValue(),
        "bronze_schema": df.schema.jsonValue(),
    }

    files_root = files_root.rstrip("/")
    schema_directory = f"{files_root}/{source_system}/_misc/schemas"
    schema_path = f"{schema_directory}/{table_name}.json"

    notebookutils.fs.mkdirs(schema_directory)
    notebookutils.fs.put(
        schema_path,
        json.dumps(schema_payload, indent=2),
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
    source_path = source_path or f"{files_root}/{source_system}/{table_name}/data"

    return (
        spark.read
        .format(file_format)
        .options(**dict(reader_options or {}))
        .load(source_path)
    )


def _validate_identifier(value: str, name: str) -> str:
    if not _VALID_IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{name} must contain only letters, numbers, "
            "and underscores and cannot begin with a number."
        )

    return value
