"""Verify odd source column names are normalized correctly by BronzeLoader."""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

from file_to_bronze import BronzeLoader


EXPECTED_COLUMNS = [
    "customer_id",
    "customer_name",
    "region_area",
    "account_balance",
    "updated_at_utc",
]


def test_odd_column_names_are_processed_correctly(
    spark: SparkSession,
    bronze_loader: BronzeLoader,
    odd_column_names_case,
):
    bronze_loader.load(odd_column_names_case.config)

    assert spark.catalog.tableExists(odd_column_names_case.target_table)

    result = spark.table(odd_column_names_case.target_table)

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

    assert (
        rows[101]["updated_at_utc"].strftime("%Y-%m-%d %H:%M:%S")
        == "2026-08-11 09:15:00"
    )
    assert (
        rows[105]["updated_at_utc"].strftime("%Y-%m-%d %H:%M:%S")
        == "2026-08-11 10:15:00"
    )
