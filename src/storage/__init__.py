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
    "cleanup_orphaned_staging_files",
]
