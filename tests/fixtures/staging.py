"""Helpers for staging packaged test fixtures into a Fabric Lakehouse."""

from importlib.resources import files

from notebookutils import fs


_FIXTURE_PACKAGE = "tests.fixtures.data"


def stage_fixture(
    fixture: str,
    *,
    source_system: str,
    table_name: str,
    file_name: str | None = None,
    overwrite: bool = True,
) -> str:
    """
    Copy a packaged fixture into the default Fabric Lakehouse Files area.

    Parameters
    ----------
    fixture:
        Relative path beneath ``tests/fixtures/data``.
        Example:
        ``"odd_column_names/odd_column_names_0001.json"``

    source_system:
        Source-system folder used by the production loader.

    table_name:
        Table folder used by the production loader.

    file_name:
        Optional destination filename. If omitted, the fixture's
        original filename is used.

    overwrite:
        Whether to overwrite an existing destination file.

    Returns
    -------
    str
        The Lakehouse-relative destination path.

    Examples
    --------
    >>> stage_fixture(
    ...     "odd_column_names/odd_column_names_0001.json",
    ...     source_system="test",
    ...     table_name="odd_col_names",
    ... )
    'Files/test/odd_col_names/data/odd_column_names_0001.json'
    """
    resource = files(_FIXTURE_PACKAGE).joinpath(fixture)

    if not resource.is_file():
        raise FileNotFoundError(f"Fixture not found: {fixture}")

    destination_name = file_name or resource.name
    destination_dir = f"Files/{source_system}/{table_name}/data"
    destination = f"{destination_dir}/{destination_name}"

    fs.mkdirs(destination_dir)
    fs.put(
        destination,
        resource.read_text(encoding="utf-8"),
        overwrite=overwrite,
    )

    return destination
