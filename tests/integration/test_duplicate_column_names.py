"""Verify normalized column-name collisions are resolved deterministically."""

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType

from file_to_bronze import BronzeLoader


EXPECTED_COLUMNS = [
    "customer_id_62566c64",
    "customer_id_79ef62a0",
    "customer_name",
]


def test_duplicate_normalized_column_names_receive_hash_suffixes(
    spark: SparkSession,
    bronze_loader: BronzeLoader,
    duplicate_column_names_case,
):
    bronze_loader.load(duplicate_column_names_case.config)

    assert spark.catalog.tableExists(duplicate_column_names_case.target_table)

    result = spark.table(duplicate_column_names_case.target_table)

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

    assert "customer_id" not in result.columns
