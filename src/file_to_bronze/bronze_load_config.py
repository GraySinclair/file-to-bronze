from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True, kw_only=True)
class BronzeLoadConfig:
    """Configuration for loading one source table from Files into Bronze.

    The configuration describes the source-specific behavior required by a
    reusable Files-to-Bronze loader. It should contain only ingestion concerns,
    such as record identity, load behavior, sequencing, and deletion handling.

    Attributes:
        source_system:
            Name of the source system. Used to locate source files and construct
            the target Bronze table name.

            Example:
                'hubspot'

        table_name:
            Logical source object and target Bronze table name.

            Example:
                'contacts'

        load_mode:
            Determines how incoming rows are applied to the Bronze table.

            load_mode options:

            - append:
                Every incoming row gets appended. Use for immutable 
                events and data that is never updated.

                
            - upsert:
                Incoming rows are merged into Bronze using merge_keys. 
                Existing records are updated and new records are inserted.

                
            - snapshot: 
                The incoming batch represents the complete current state of the 
                source-object. Records are upserted, and existing Bronze records 
                missing from the completed snapshot may be deleted or marked as deleted.

                
        merge_keys:
            One or more normalized column names that uniquely identify a source
            record.

            Required for upsert and snapshot loads. Optional for
            append-only loads unless duplicate detection is required.

            Example:
                ('id')

            Composite key example:
                ('journal_id', 'line_number')

        sequence_column:
            Optional column used to determine the newest row when an
            incoming batch contains multiple records for the same merge key.

            This is commonly a source modification timestamp, version number, or
            other monotonically increasing value.

            Example:
                'updated_at'

            When omitted, the loader must either reject duplicate merge keys or
            use another explicitly defined deterministic rule.

        delete_column:
            Optional source column indicating that a record has been
            deleted, archived, or deactivated.

            Examples:
                'archived'
                'is_deleted'

            The loader is responsible for interpreting the column according to
            the source contract. A truthy value normally causes the matching
            Bronze row to be deleted or soft-deleted.

        soft_delete:
            Controls how detected deletions are represented in Bronze. (Default = False)

            When True, the Bronze row remains present and is marked using
            deletion metadata such as _is_deleted and _deleted_at.

            When False, the matching row is physically deleted from the
            Bronze Delta table.

    Notes:
        - Column names stored in this configuration should use the normalized
          names produced by the Files-to-Bronze loader. (e.g., original_column: 'amount (YTD)' -> loader_produced_column: 'amount_ytd')

        - Snapshot deletion logic should run only after confirming that the
          source extraction completed successfully and contains the full
          expected dataset.

        - kw_only=True requires all values to be passed by keyword, making
          configuration declarations safer to maintain.

    Examples:
        Incremental upsert with soft deletion:
        >>> BronzeLoadConfig(
        ...     source_system='hubspot',
        ...     table_name='contacts',
        ...     load_mode='upsert',
        ...     merge_keys=('id'),
        ...     sequence_column='updatedat',
        ...     delete_column='archived',
        ...     soft_delete=True
        ... )

        Append-only transaction table:
        >>> BronzeLoadConfig(
        ...     source_system='intacct',
        ...     table_name='journal_entry_lines',
        ...     load_mode='append',
        ...     merge_keys=('id')
        ... )

        Complete source snapshot:
        >>> BronzeLoadConfig(
        ...     source_system='awardco',
        ...     table_name='users',
        ...     load_mode='snapshot',
        ...     merge_keys=('user_id'),
        ...     sequence_column='modified_at',
        ...     soft_delete=True
        ... )
    """

    source_system: str
    table_name: str
    load_mode: Literal["append", "upsert", "snapshot"]
    merge_keys: tuple[str, ...] = ()
    sequence_column: str | None = None
    delete_column: str | None = None
    soft_delete: bool = False
