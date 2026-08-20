# file-to-bronze

`file-to-bronze` is a small PySpark package for loading landed files into Bronze Delta tables in Microsoft Fabric.

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


# Schema Tools

This module provides utilities for developing and saving explicit Spark schemas for landed source files.

The intended workflow is:

1. Load the landed files using Spark schema inference.
2. Normalize the source column names.
3. Review and adjust the inferred data types.
4. Save the finalized schema to the Lakehouse.
5. Use the saved schema with `BronzeLoader` for future loads.

## Basic Usage

```python
from file_to_bronze.schema_tools import (
    load_inferred_dataframe,
    save_schema,
)
```

### 1. Load the source data with an inferred schema

```python
df = load_inferred_dataframe(
    spark,
    source_system="hubspot",
    table_name="contacts",
)

df.printSchema()
display(df)
```

By default, the function reads from:

```text
Files/{source_system}/{table_name}/data
```

For the example above:

```text
Files/hubspot/contacts/data
```

Spark infers the source data types, and the returned DataFrame has its top-level column names normalized using the package's shared `normalize_columns()` logic.

---

## Adjust the Inferred Schema

The inferred DataFrame is intended as a starting point. Cast columns to the types you want Bronze to use.

For example:

```python
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    LongType,
    TimestampType,
)

df = (
    df
    .withColumn(
        "id",
        col("id").cast(LongType()),
    )
    .withColumn(
        "created_at",
        col("created_at").cast(TimestampType()),
    )
    .withColumn(
        "is_deleted",
        col("is_deleted").cast(BooleanType()),
    )
)

df.printSchema()
```

The current schema of `df` becomes the desired Bronze schema when `save_schema()` is called.

---

## Save the Finalized Schema

```python
schema_path = save_schema(
    df,
    source_system="hubspot",
    table_name="contacts",
)

print(schema_path)
```

The schema is written to:

```text
Files/_schemas/{source_system}/{table_name}.json
```

For example:

```text
Files/_schemas/hubspot/contacts.json
```

The file contains Spark's serialized `StructType` definition.

---

## Why `save_schema()` Reads the Source Again

The working DataFrame contains normalized column names.

For example, a raw source might contain:

```text
Customer ID
Created (Date)
Is Deleted?
```

After normalization:

```text
customer_id
created_date
is_deleted
```

However, Spark must apply an explicit schema **while reading the raw file**, before column normalization occurs.

For that reason, `save_schema()`:

1. Reads the raw source again.
2. Normalizes those source columns to determine their normalized equivalents.
3. Matches them to the columns in the supplied DataFrame.
4. Takes the desired data types from the supplied DataFrame.
5. Restores the original raw source column names.
6. Saves that schema for use during future raw-file reads.

Conceptually:

```text
Raw source
    |
    | Spark schema inference
    v
Raw names + inferred types
    |
    | normalize_columns()
    v
Normalized DataFrame
    |
    | manually adjust data types
    v
Normalized names + desired types
    |
    | save_schema()
    v
Raw names + desired types
    |
    v
Files/_schemas/{source_system}/{table_name}.json
```

This allows `BronzeLoader` to apply the correct types to the original source fields and then normalize the resulting DataFrame consistently.

---

## Custom Source Path

If the files are not stored in the default location, provide `source_path`.

```python
df = load_inferred_dataframe(
    spark,
    source_system="hubspot",
    table_name="contacts",
    source_path="Files/schema_development/hubspot_contacts",
)
```

Use the same source path when saving the schema:

```python
schema_path = save_schema(
    df,
    source_system="hubspot",
    table_name="contacts",
    source_path="Files/schema_development/hubspot_contacts",
)
```

---

## Reader Options

Additional Spark reader options can be supplied with `reader_options`.

For JSON Lines data:

```python
df = load_inferred_dataframe(
    spark,
    source_system="hubspot",
    table_name="contacts",
    reader_options={
        "multiLine": "false",
    },
)
```

For CSV data:

```python
df = load_inferred_dataframe(
    spark,
    source_system="example",
    table_name="customers",
    file_format="csv",
    reader_options={
        "header": "true",
        "delimiter": ",",
    },
)
```

The same reader settings should also be supplied to `save_schema()`:

```python
schema_path = save_schema(
    df,
    source_system="example",
    table_name="customers",
    file_format="csv",
    reader_options={
        "header": "true",
        "delimiter": ",",
    },
)
```

---

## Complete Example

```python
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    LongType,
    TimestampType,
)

from file_to_bronze.schema_tools import (
    load_inferred_dataframe,
    save_schema,
)


source_system = "hubspot"
table_name = "contacts"


# 1. Read a representative sample of the landed data.
df = load_inferred_dataframe(
    spark,
    source_system=source_system,
    table_name=table_name,
)


# 2. Inspect Spark's inferred schema.
df.printSchema()
display(df)


# 3. Adjust the columns that require explicit types.
df = (
    df
    .withColumn(
        "id",
        col("id").cast(LongType()),
    )
    .withColumn(
        "created_at",
        col("created_at").cast(TimestampType()),
    )
    .withColumn(
        "updated_at",
        col("updated_at").cast(TimestampType()),
    )
    .withColumn(
        "is_deleted",
        col("is_deleted").cast(BooleanType()),
    )
)


# 4. Review the desired Bronze schema.
df.printSchema()


# 5. Save it.
schema_path = save_schema(
    df,
    source_system=source_system,
    table_name=table_name,
)

print(f"Schema saved to: {schema_path}")
```

## Important Behavior

`source_system` and `table_name` must be valid identifiers containing only letters, numbers, and underscores, and they cannot begin with a number.

Examples of valid values:

```text
hubspot
halo_itsm
contacts
journal_entry_lines
```

Examples of invalid values:

```text
halo-itsm
journal entry lines
123_contacts
```

`save_schema()` also validates that the supplied DataFrame still contains exactly the same normalized columns as the source. Adding, removing, or renaming columns before saving the schema will raise a `ValueError`.

Changing **data types** is expected. Changing the **column set** is not.

The source files used for schema development should also be representative of the actual data. Spark inference can only infer types from values present in the files being inspected.
