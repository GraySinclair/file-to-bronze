"""Helpers for staging packaged fixtures into a Fabric Lakehouse."""

from importlib.resources import files

from notebookutils import fs


_FIXTURE_PACKAGE = f"{__package__}.data"


def _resource_text(fixture: str) -> str:
    resource = files(_FIXTURE_PACKAGE).joinpath(fixture)

    if not resource.is_file():
        raise FileNotFoundError(f"Fixture not found: {fixture}")

    return resource.read_text(encoding="utf-8")


def stage_fixture(
    fixture: str,
    *,
    source_system: str,
    table_name: str,
    file_name: str | None = None,
    overwrite: bool = True,
) -> str:
    """Stage packaged test data into BronzeLoader's normal data path."""
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


def stage_schema_fixture(
    fixture: str,
    *,
    source_system: str,
    table_name: str,
    overwrite: bool = True,
) -> str:
    """Stage a Spark StructType JSON fixture where BronzeLoader expects it."""
    destination_dir = f"Files/_schemas/{source_system}"
    destination = f"{destination_dir}/{table_name}"

    fs.mkdirs(destination_dir)
    fs.put(
        destination,
        _resource_text(fixture),
        overwrite=overwrite,
    )

    return destination
