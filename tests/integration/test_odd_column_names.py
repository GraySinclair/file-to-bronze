"""Verify odd source column names are normalized correctly by BronzeLoader."""

from notebookutils import fs
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

from file_to_bronze import BronzeLoadConfig, BronzeLoader
from tests.fixtures import stage_fixture, stage_schema_fixture


SOURCE_SYSTEM = "test"
TABLE_NAME = "odd_col_names"
TARGET_TABLE = f"Bronze.{SOURCE_SYSTEM}.{TABLE_NAME}"

DATA_PATH = f"Files/{SOURCE_SYSTEM}/{TABLE_NAME}/data"
CHECKPOINT_PATH = (
    f"Files/_checkpoints/file_to_bronze/{SOURCE_SYSTEM}/{TABLE_NAME}"
)
SCHEMA_PATH = f"Files/_schemas/{SOURCE_SYSTEM}/{TABLE_NAME}"

EXPECTED_COLUMNS = [
    "customer_id",
    "customer_name",
    "region_area",
    "account_balance",
    "updated_at_utc",
]


def _remove_if_exists(path: str) -> None:
    if fs.exists(path):
        fs.rm(path, recurse=True)


def _reset_test_state(spark: SparkSession) -> None:
    """Remove prior test artifacts so every run starts clean."""
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    _remove_if_exists(DATA_PATH)
    _remove_if_exists(CHECKPOINT_PATH)

    if fs.exists(SCHEMA_PATH):
        fs.rm(SCHEMA_PATH)


def test_odd_column_names_are_processed_correctly():
    spark = SparkSession.getActiveSession()

    assert spark is not None, "This test must run in an active Fabric Spark session."

    _reset_test_state(spark)

    stage_fixture(
        "odd_column_names/odd_column_names_0001.json",
        source_system=SOURCE_SYSTEM,
        table_name=TABLE_NAME,
    )

    stage_schema_fixture(
        "odd_column_names/schema.json",
        source_system=SOURCE_SYSTEM,
        table_name=TABLE_NAME,
    )

    config = BronzeLoadConfig(
        source_system=SOURCE_SYSTEM,
        table_name=TABLE_NAME,
        load_mode="append",
    )

    BronzeLoader(spark).load(config)

    assert spark.catalog.tableExists(TARGET_TABLE)

    result = spark.table(TARGET_TABLE)

    assert result.columns == EXPECTED_COLUMNS
    assert result.count() == 5

    expected_types = {
        "customer_id": LongType,
        "customer_name": StringType,
        "region_area": StringType,
        "account_balance": DoubleType,
        "updated_at_utc": TimestampType,
    }

    actual_types = {
        field.name: type(field.dataType)
        for field in result.schema.fields
    }

    assert actual_types == expected_types

    rows = {
        row["customer_id"]: row.asDict()
        for row in result.collect()
    }

    assert set(rows) == {101, 102, 103, 104, 105}

    assert rows[101]["customer_name"] == "Customer A"
    assert rows[101]["region_area"] == "North"
    assert rows[101]["account_balance"] == 1250.5

    assert rows[105]["customer_name"] == "Customer E"
    assert rows[105]["region_area"] == "North"
    assert rows[105]["account_balance"] == 9999.99

    assert rows[101]["updated_at_utc"].strftime(
        "%Y-%m-%d %H:%M:%S"
    ) == "2026-08-11 09:15:00"

    assert rows[105]["updated_at_utc"].strftime(
        "%Y-%m-%d %H:%M:%S"
    ) == "2026-08-11 10:15:00"
