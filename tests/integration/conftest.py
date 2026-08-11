"""Shared pytest fixtures for Fabric integration tests."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest
from notebookutils import fs
from pyspark.sql import SparkSession

from file_to_bronze import BronzeLoadConfig, BronzeLoader

from ..fixtures import stage_fixture, stage_schema_fixture


@dataclass(frozen=True, slots=True)
class BronzeIntegrationCase:
    """A staged test table and the paths that belong to it."""

    config: BronzeLoadConfig
    target_table: str
    data_path: str
    checkpoint_path: str
    schema_path: str


def _remove_if_exists(path: str) -> None:
    if fs.exists(path):
        fs.rm(path, recurse=True)


def _reset_case(spark: SparkSession, case: BronzeIntegrationCase) -> None:
    """Remove table, source data, checkpoint, and schema for one test case."""
    if spark.catalog.tableExists(case.target_table):
        spark.sql(f"DROP TABLE {case.target_table}")

    _remove_if_exists(case.data_path)
    _remove_if_exists(case.checkpoint_path)
    _remove_if_exists(case.schema_path)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Return the active Fabric Spark session."""
    session = SparkSession.getActiveSession()

    assert session is not None, (
        "Fabric integration tests require an active Spark session."
    )

    return session


@pytest.fixture
def bronze_loader(spark: SparkSession) -> BronzeLoader:
    """Return a fresh BronzeLoader for each integration test."""
    return BronzeLoader(spark)


@pytest.fixture
def staged_bronze_case(
    spark: SparkSession,
) -> Iterator[Callable[..., BronzeIntegrationCase]]:
    """
    Return a factory that stages a packaged data/schema pair.

    Each staged case is reset before setup and automatically cleaned up after
    the requesting test, including when the test itself fails.
    """
    staged_cases: list[BronzeIntegrationCase] = []

    def stage(
        *,
        fixture_directory: str,
        data_file: str,
        source_system: str,
        table_name: str,
        load_mode: str = "append",
    ) -> BronzeIntegrationCase:
        case = BronzeIntegrationCase(
            config=BronzeLoadConfig(
                source_system=source_system,
                table_name=table_name,
                load_mode=load_mode,
            ),
            target_table=f"Bronze.{source_system}.{table_name}",
            data_path=f"Files/{source_system}/{table_name}/data",
            checkpoint_path=(
                f"Files/_checkpoints/file_to_bronze/"
                f"{source_system}/{table_name}"
            ),
            schema_path=f"Files/_schemas/{source_system}/{table_name}",
        )

        # Register first so teardown still knows about the case if staging fails.
        staged_cases.append(case)
        _reset_case(spark, case)

        stage_fixture(
            f"{fixture_directory}/{data_file}",
            source_system=source_system,
            table_name=table_name,
        )
        stage_schema_fixture(
            f"{fixture_directory}/schema.json",
            source_system=source_system,
            table_name=table_name,
        )

        return case

    yield stage

    for case in reversed(staged_cases):
        _reset_case(spark, case)


@pytest.fixture
def odd_column_names_case(
    staged_bronze_case: Callable[..., BronzeIntegrationCase],
) -> BronzeIntegrationCase:
    """Stage the odd-column-name integration dataset."""
    return staged_bronze_case(
        fixture_directory="odd_column_names",
        data_file="odd_column_names_0001.json",
        source_system="test",
        table_name="odd_col_names",
    )


@pytest.fixture
def duplicate_column_names_case(
    staged_bronze_case: Callable[..., BronzeIntegrationCase],
) -> BronzeIntegrationCase:
    """Stage the duplicate-normalized-column-name integration dataset."""
    return staged_bronze_case(
        fixture_directory="duplicate_column_names",
        data_file="duplicate_column_names_0001.json",
        source_system="test",
        table_name="duplicate_column_names",
    )
