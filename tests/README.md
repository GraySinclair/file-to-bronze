# Tests

This folder is packaged with the wheel so Microsoft Fabric can run integration
tests against the exact wheel artifact that was built.

## Structure

- `fixtures/` - packaged test data plus low-level staging helpers.
- `integration/conftest.py` - Fabric-specific pytest fixtures.
- `integration/` - tests requiring Fabric, Spark, and a Lakehouse.
- `unit/` - tests that can run without Fabric infrastructure.

Fabric-only imports are intentionally kept under `integration/` so future unit
tests do not require `notebookutils` or an active Spark session just to collect.

## Automatic integration setup

Integration tests request pytest fixtures rather than staging their own files.

For example:

```python
def test_example(spark, bronze_loader, odd_column_names_case):
    bronze_loader.load(odd_column_names_case.config)
    result = spark.table(odd_column_names_case.target_table)
```

`odd_column_names_case` automatically:

1. Removes stale artifacts from a previous run.
2. Stages its packaged data file.
3. Stages its explicit Spark schema.
4. Runs the test.
5. Removes the target table, source data, checkpoint, and schema afterward.

Cleanup is implemented with a `yield` fixture, so teardown also runs after a
test failure.

## Normal tests

The project's pytest configuration excludes tests marked `extended` by default:

```bash
pytest
```

For the packaged wheel in Fabric:

```python
pytest.main([
    "--pyargs",
    "file_to_bronze_tests",
    "-v",
])
```

## Extended tests

`test_fixture_staging.py` validates the fixture-staging helper itself and is
marked `extended`, so it is not part of the normal run.

Run only extended tests:

```bash
pytest -m extended
```

From the packaged wheel in Fabric:

```python
pytest.main([
    "--pyargs",
    "file_to_bronze_tests",
    "-v",
    "-m",
    "extended",
])
```

The production package should never import anything from the test package.
