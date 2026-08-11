"""Verify normalized column-name collisions are resolved deterministically."""

from notebookutils import fs
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType

from file_to_bronze import BronzeLoadConfig, BronzeLoader
from tests.fixtures import stage_fixture, stage_schema_fixture


SOURCE_SYSTEM = "test"
TABLE_NAME = "duplicate_column_names"
TARGET_TABLE = f"Bronze.{SOURCE_SYSTEM}.{TABLE_NAME}"

DATA_PATH = f"Files/{SOURCE_SYSTEM}/{TABLE_NAME}/data"
CHECKPOINT_PATH = (
    f"Files/_checkpoints/file_to_bronze/{SOURCE_SYSTEM}/{TABLE_NAME}"
)
SCHEMA_PATH = f"Files/_schemas/{SOURCE_SYSTEM}/{TABLE_NAME}"

# Both raw names normalize to "customer_id".
#
# sha256("Customer ID")[:8]  -> 62566c64
# sha256("Customer-ID")[:8]  -> 79ef62a0
EXPECTED_COLUMNS = [
    "customer_id_62566c64",
    "customer_id_79ef62a0",
    "customer_name",
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


def test_duplicate_normalized_column_names_receive_hash_suffixes():
    spark = SparkSession.getActiveSession()

    assert spark is not None, "This test must run in an active Fabric Spark session."

    _reset_test_state(spark)

    stage_fixture(
        "duplicate_column_names/duplicate_column_names_0001.json",
        source_system=SOURCE_SYSTEM,
        table_name=TABLE_NAME,
    )

    stage_schema_fixture(
        "duplicate_column_names/schema.json",
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
        "customer_id_62566c64": LongType,
        "customer_id_79ef62a0": StringType,
        "customer_name": StringType,
    }

    actual_types = {
        field.name: type(field.dataType)
        for field in result.schema.fields
    }

    assert actual_types == expected_types

    rows = {
        row["customer_id_62566c64"]: row.asDict()
        for row in result.collect()
    }

    assert set(rows) == {101, 102, 103, 104, 105}

    assert rows[101]["customer_id_79ef62a0"] == "EXT-9001"
    assert rows[101]["customer_name"] == "Customer A"

    assert rows[105]["customer_id_79ef62a0"] == "EXT-9005"
    assert rows[105]["customer_name"] == "Customer E"

    # The un-hashed normalized name must not survive the collision.
    assert "customer_id" not in result.columns
