# Tests

This folder is intentionally packaged with the wheel so Fabric can run
integration tests against the exact wheel artifact that was built.

## Structure

- `fixtures/` - reusable test-only helpers
- `fixtures/data/` - packaged input files
- `integration/` - tests requiring Fabric / Spark / Lakehouse
- `unit/` - tests that can run without external infrastructure

## Stage a fixture

```python
from tests.fixtures import stage_fixture

path = stage_fixture(
    "odd_column_names/odd_column_names_0001.json",
    source_system="test",
    table_name="odd_col_names",
)

print(path)
# Files/test/odd_col_names/data/odd_column_names_0001.json
```

The production package should never import anything from `tests`.
