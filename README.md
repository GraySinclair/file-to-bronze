# file-to-bronze

`file-to-bronze` is a small PySpark package for loading landed files into Bronze Delta tables in Microsoft Fabric.

## Package structure

```text
file-to-bronze/
├── pyproject.toml
├── README.md
└── src/
    └── file_to_bronze/
        ├── __init__.py
        ├── bronze_load_config.py
        └── bronze_loader.py
```

The package exposes the classes:

- BronzeLoadConfig
- BronzeLoader

## Import statement:
```python
from file_to_bronze import BronzeLoadConfig, BronzeLoader
```


## Default paths layout

```text
By default, the loader reads data with this pattern:
    Files/{source_system}/{table_name}/data

The default checkpoint pattern:
    Files/_checkpoints/file_to_bronze/{source_system}/{table_name}

The target Delta table pattern:
    {bronze_lakehouse}.{source_system}.{table_name}
```

## Usage example

The following example loads HubSpot contact JSON files into `Bronze.hubspot.contacts`.

The config uses `upsert` mode, matches rows by `id`, keeps the newest duplicate according to `updatedat`, and treats `archived=true` as a soft delete.

```python
# import
from file_to_bronze import BronzeLoadConfig, BronzeLoader


# configure how this specific table should be loaded
contacts_config = BronzeLoadConfig(
    source_system="hubspot",
    table_name="contacts",
    load_mode="upsert",
    merge_keys=("id",),
    sequence_column="updatedat",
    delete_column="archived",
    soft_delete=True,
)


# instance a loader object
loader = BronzeLoader(
    spark,
    bronze_lakehouse="Bronze",
    files_root="Files",
    checkpoint_root="Files/_checkpoints/file_to_bronze",
    file_format="json",
    reader_options={"multiLine": "false"},
    enable_cdf=True,
    allow_schema_evolution=True,
)


# Consume the table configuration.
#   From 'Files/hubspot/contacts/data' to 'Bronze.hubspot.contacts'
#       Checkpoint saved to 'Files/_checkpoints/file_to_bronze/hubspot/contacts'

run_id = loader.load(contacts_config)

print(f"Bronze load completed. run_id={run_id}")
```

## Loading multiple configured tables

A single `BronzeLoader` instance can consume multiple table configurations:

```python
from file_to_bronze import BronzeLoadConfig, BronzeLoader


configs = [
    BronzeLoadConfig(
        source_system="hubspot",
        table_name="contacts",
        load_mode="upsert",
        merge_keys=("id",),
        sequence_column="updatedat",
        delete_column="archived",
        soft_delete=True,
    ),
    BronzeLoadConfig(
        source_system="hubspot",
        table_name="companies",
        load_mode="upsert",
        merge_keys=("id",),
        sequence_column="updatedat",
        delete_column="archived",
        soft_delete=True,
    ),
    BronzeLoadConfig(
        source_system="awardco",
        table_name="recognition_details",
        load_mode="append",
    ),
]


loader = BronzeLoader(
    spark,
    bronze_lakehouse="Bronze",
    file_format="json",
    reader_options={"multiLine": "false"},
)


run_results = {}

for config in configs:
    run_results[f"{config.source_system}.{config.table_name}"] = (
        loader.load(config)
    )

display(run_results)
```

Each configuration receives its own source path, target table, and streaming checkpoint.

## Load modes

### `append`

Use `append` when every source row represents a new event and existing Bronze rows should never be updated. Append mode does not require merge keys.

### `upsert`

Use `upsert` when landed files may contain new records, changed records, or deletion indicators. Upsert mode requires at least one merge key.

When duplicate merge keys exist in the same processed batch, `sequence_column` determines which row is retained. The greatest non-null sequence value is selected.

### `snapshot`

Use `snapshot` when each delivery contains the complete current state of a source object. Snapshot mode requires an explicit `source_path`. This prevents the loader from accidentally treating the entire historical landing directory as the current snapshot.

Target rows not in the new snapshot are:
- Marked as deleted when `soft_delete=True`
- Physically deleted when `soft_delete=False`

```python
# snapshot config
config = BronzeLoadConfig(
    source_system="awardco",
    table_name="users",
    load_mode="snapshot",
    merge_keys=("user_id",),
    soft_delete=True,
)

# source_path explicitely passed for snapshot load
run_id = loader.load(
    config,
    source_path="Files/awardco/users/snapshots/2026-08-06",
)
```

## Configuration fields

| Field | Type | Purpose |
|---|---|---|
| `source_system` | `str` | Schema name under the Bronze Lakehouse and source folder name. |
| `table_name` | `str` | Bronze table name and source folder name. |
| `load_mode` | `"append"`, `"upsert"`, or `"snapshot"` | Determines how incoming records affect the target table. |
| `merge_keys` | `tuple[str, ...]` | Columns used to match source and target rows. Required for upsert and snapshot modes. |
| `sequence_column` | `str or None` | Selects the newest row when duplicate merge keys occur in one batch. |
| `delete_column` | `str or None` | Boolean-like source column indicating that a row was deleted. |
| `soft_delete` | `bool` | Marks deleted rows instead of physically deleting them. |


## Column name normalization

All source column names are automatically normalized for safety before loading:

- Converted to lowercase
- Non-alphanumeric character groups replaced with `_`
- Leading and trailing underscores removed
- Names beginning with a number get prefixed with `column_`
- If the normalizer resolves to a duplicate column name, a hash of the original column name is appended to each name to allow for deterministic continuity

Examples:

```text
Contact ID       -> contact_id
audit.modifiedAt -> audit_modifiedat
2026 Value       -> column_2026_value
```

## Change Data Feed

When `enable_cdf=True`, the loader:

1. Sets the session default so newly created Delta tables enable Change Data Feed.
2. Explicitly sets `delta.enableChangeDataFeed = true` on each processed target table.

CDF can then be read from the Bronze table:

```python
changes_df = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("Bronze.hubspot.contacts")
)

display(changes_df)
```

## Notes

- Append and upsert modes use an available-now Structured Streaming trigger.
- Streaming checkpoints prevent files already processed by that checkpoint from being loaded again.
- Snapshot paths should contain only one complete current snapshot.


```
FEATURES TO BE REMOVED
```
## Bronze metadata columns

The loader adds the following columns:

| Column | Purpose |
|---|---|
| `_bronze_source_file` | Source file from which the row was read. |
| `_bronze_ingested_at` | Timestamp at which the row entered Bronze. |
| `_bronze_run_id` | UUID identifying the loader execution. |
| `_bronze_is_deleted` | Soft-delete status when soft deletes are enabled. |
| `_bronze_deleted_at` | Timestamp at which a row was soft deleted. |

The loader raises an error when two original columns normalize to the same final name.