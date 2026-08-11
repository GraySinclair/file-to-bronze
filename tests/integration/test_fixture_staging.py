"""Fabric integration tests for packaged fixture staging."""

from notebookutils import fs

from ..fixtures import stage_fixture


def test_stage_odd_column_names_fixture():
    destination = stage_fixture(
        "odd_column_names/odd_column_names_0001.json",
        source_system="test",
        table_name="odd_col_names",
    )

    assert destination == (
        "Files/test/odd_col_names/data/odd_column_names_0001.json"
    )
    assert fs.exists(destination)
