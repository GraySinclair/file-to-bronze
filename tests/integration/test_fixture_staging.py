"""Extended validation tests for packaged fixture staging."""

import pytest
from notebookutils import fs

from ..fixtures import stage_fixture


pytestmark = pytest.mark.extended


def test_stage_odd_column_names_fixture():
    destination = stage_fixture(
        "odd_column_names/odd_column_names_0001.json",
        source_system="test",
        table_name="fixture_staging",
    )

    try:
        assert destination == (
            "Files/test/fixture_staging/data/odd_column_names_0001.json"
        )
        assert fs.exists(destination)
    finally:
        if fs.exists("Files/test/fixture_staging"):
            fs.rm("Files/test/fixture_staging", recurse=True)
