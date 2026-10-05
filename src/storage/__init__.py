"""
Storage subsystem package for Data Harvester.

Exposes configuration, schema definition, and atomic publication for the
Partitioned Parquet Tick Lake (Milestone v4.0).
"""

from src.storage.config import (
    StorageConfigError,
    LakeNotFoundError,
    LakeMaintenanceInProgressError,
    IncompatibleSchemaError,
    PathTraversalError,
    LakeMetadata,
    TICK_LAKE_ROOT_ENV,
    DATA_DIR_ENV,
    MICRON_DATA_DIR,
    DEFAULT_LAKE_SUBDIR,
    LAKE_METADATA_FILENAME,
    MAINTENANCE_GUARD_FILENAME,
    SUBDIRECTORIES,
    resolve_tick_lake_root,
    init_tick_lake,
    load_lake_metadata,
    encode_symbol,
    decode_symbol,
    get_partition_path,
)

from src.storage.schema import (
    QuoteTick,
    SchemaValidationError,
    SCHEMA_V1_VERSION,
    SCHEMA_V1_VERSION_STR,
    SCHEMA_V1_METADATA_KEY,
    SCHEMA_V1_FORMAT_KEY,
    SCHEMA_V1_FORMAT_VAL,
    SCHEMA_V1_COLUMNS,
    LAKE_SCHEMA_V1,
    validate_schema_v1,
    validate_table_v1,
    ticks_to_table,
    table_to_ticks,
)

from src.storage.publication import (
    PublishError,
    LakeOwnershipError,
    BatchCollisionError,
    FilePublicationReceipt,
    PublishReceipt,
    PublishIntent,
    LakePublisherLock,
    LakePublisher,
    recover_pending_publications,
    cleanup_orphaned_staging_files,
    cleanup_orphaned_staging_files as cleanup_orphaned_parquet_staging_files,
)

from src.storage.parquet_writer import (
    TickLakeWriter,
    WriterMetrics,
)

from src.storage.registry import (
    DEFAULT_REGISTRY_FILENAME,
    DEFAULT_SIGNAL_FILENAME,
    DEFAULT_CONTROL_LOCK_FILENAME,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    RegistryError,
    SymbolPendingPurgeError,
    SymbolNotFoundError,
    SymbolAlreadyExistsError,
    InvalidSymbolError,
    RegistryLockError,
    RegistryCorruptedError,
    SymbolEntry,
    RegistrySnapshot,
    RegistryControlLock,
    SymbolRegistry,
    init_registry,
    load_registry,
    get_registry_path,
    touch_stream_reload_signal,
    get_symbol_registry,
    cleanup_orphaned_registry_staging_files,
)

from src.storage.reader import (
    TickLakeReader,
    get_tick_lake_reader,
)

from src.storage.capacity import (
    CapacityMonitor,
    CapacityAlert,
    PartitionMetrics,
)

from src.storage.compaction import (
    CompactionError,
    MaintenanceJournalError,
    ConsumerDrainError,
    ConsumerDrainRefusedError,
    EquivalenceVerificationError,
    PurgeError,
    MaintenanceJournal,
    LineageManager,
    LakeCompactor,
    recover_maintenance,
    purge_symbol_physical,
)

__all__ = [
    # Config & Hierarchy
    "StorageConfigError",
    "LakeNotFoundError",
    "LakeMaintenanceInProgressError",
    "IncompatibleSchemaError",
    "PathTraversalError",
    "LakeMetadata",
    "TICK_LAKE_ROOT_ENV",
    "DATA_DIR_ENV",
    "MICRON_DATA_DIR",
    "DEFAULT_LAKE_SUBDIR",
    "LAKE_METADATA_FILENAME",
    "MAINTENANCE_GUARD_FILENAME",
    "SUBDIRECTORIES",
    "resolve_tick_lake_root",
    "init_tick_lake",
    "load_lake_metadata",
    "encode_symbol",
    "decode_symbol",
    "get_partition_path",
    # Schema v1
    "QuoteTick",
    "SchemaValidationError",
    "SCHEMA_V1_VERSION",
    "SCHEMA_V1_VERSION_STR",
    "SCHEMA_V1_METADATA_KEY",
    "SCHEMA_V1_FORMAT_KEY",
    "SCHEMA_V1_FORMAT_VAL",
    "SCHEMA_V1_COLUMNS",
    "LAKE_SCHEMA_V1",
    "validate_schema_v1",
    "validate_table_v1",
    "ticks_to_table",
    "table_to_ticks",
    # Publication & Recovery
    "PublishError",
    "LakeOwnershipError",
    "BatchCollisionError",
    "FilePublicationReceipt",
    "PublishReceipt",
    "PublishIntent",
    "LakePublisherLock",
    "LakePublisher",
    "recover_pending_publications",
    "cleanup_orphaned_parquet_staging_files",
    "cleanup_orphaned_staging_files",
    # Streaming Parquet Writer
    "TickLakeWriter",
    "WriterMetrics",
    # Versioned Symbol Registry (Phase 18)
    "DEFAULT_REGISTRY_FILENAME",
    "DEFAULT_SIGNAL_FILENAME",
    "DEFAULT_CONTROL_LOCK_FILENAME",
    "STATUS_ACTIVE",
    "STATUS_INACTIVE",
    "STATUS_PENDING_PURGE",
    "RegistryError",
    "SymbolPendingPurgeError",
    "SymbolNotFoundError",
    "SymbolAlreadyExistsError",
    "InvalidSymbolError",
    "RegistryLockError",
    "RegistryCorruptedError",
    "SymbolEntry",
    "RegistrySnapshot",
    "RegistryControlLock",
    "SymbolRegistry",
    "init_registry",
    "load_registry",
    "get_registry_path",
    "touch_stream_reload_signal",
    "get_symbol_registry",
    "cleanup_orphaned_registry_staging_files",
    # Lake Reader (Phase 19)
    "TickLakeReader",
    "get_tick_lake_reader",
    # Capacity & Compaction (Phase 41)
    "CapacityMonitor",
    "CapacityAlert",
    "PartitionMetrics",
    "CompactionError",
    "MaintenanceJournalError",
    "ConsumerDrainError",
    "ConsumerDrainRefusedError",
    "EquivalenceVerificationError",
    "PurgeError",
    "MaintenanceJournal",
    "LineageManager",
    "LakeCompactor",
    "recover_maintenance",
    "purge_symbol_physical",
]

