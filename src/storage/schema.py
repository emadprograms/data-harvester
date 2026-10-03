"""
Lake Schema v1 definition, PyArrow builders, and vectorized validations.
"""
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
import pyarrow as pa

SCHEMA_V1_VERSION = 1
SCHEMA_V1_VERSION_STR = "1"
SCHEMA_V1_METADATA_KEY = b"schema_version"
SCHEMA_V1_FORMAT_KEY = b"format"
SCHEMA_V1_FORMAT_VAL = b"tick_lake_v1"

SCHEMA_V1_COLUMNS = [
    "timestamp",
    "symbol",
    "price",
    "volume",
    "bid",
    "ask",
    "source",
    "session",
    "ingest_id",
]

# Unimplemented placeholder schema for TDD RED state
LAKE_SCHEMA_V1 = None


class SchemaValidationError(ValueError):
    """Raised when data or schema violates Lake Schema v1 constraints."""
    pass


def validate_schema_v1(schema_or_table: Union[pa.Schema, pa.Table, pa.RecordBatch]) -> bool:
    """Validate that schema matches Lake Schema v1 exact column names, order, types, and nullability."""
    raise NotImplementedError("validate_schema_v1 not implemented yet")


def validate_table_v1(table: pa.Table) -> bool:
    """Perform vectorized validation of Lake Schema v1 constraints on a PyArrow Table."""
    raise NotImplementedError("validate_table_v1 not implemented yet")


def ticks_to_table(
    records: Iterable[Union[Any, Dict[str, Any], Tuple]],
    validate: bool = True,
) -> pa.Table:
    """Convert QuoteTick objects, dicts, or tuples into a validated PyArrow Table."""
    raise NotImplementedError("ticks_to_table not implemented yet")


def table_to_ticks(table: pa.Table) -> List[Any]:
    """Convert a PyArrow Table into a list of QuoteTick objects."""
    raise NotImplementedError("table_to_ticks not implemented yet")
