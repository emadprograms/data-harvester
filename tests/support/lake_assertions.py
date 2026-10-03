"""Independent Parquet oracles; deliberately do not call the application reader."""
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import pyarrow.parquet as pq


LAKE_COLUMNS = (
    "timestamp",
    "symbol",
    "price",
    "volume",
    "bid",
    "ask",
    "source",
    "session",
    "ingest_id",
)


def finalized_parquet_files(root: Path) -> list[Path]:
    """Return only published files; migration staging/control files are excluded."""
    root = Path(root)
    return sorted((root / "ticks").glob("symbol=*/date=*/*.parquet"))


def read_final_rows(root: Path, columns: Sequence[str] = LAKE_COLUMNS) -> list[dict]:
    """Read finalized Parquet rows directly with PyArrow, preserving duplicates."""
    rows = []
    for path in finalized_parquet_files(root):
        table = pq.read_table(path, columns=list(columns))
        rows.extend(table.to_pylist())
    return rows


def _stable_value(value):
    if isinstance(value, datetime):
        # Arrow TIMESTAMP(us) is naive UTC by the lake contract.
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


def row_multiset(
    rows: Iterable[Mapping],
    columns: Sequence[str] = LAKE_COLUMNS,
) -> Counter:
    """Canonical full-row multiset, retaining exact duplicate multiplicity."""
    return Counter(
        tuple(_stable_value(row.get(column)) for column in columns)
        for row in rows
    )


def assert_row_multiset(actual: Iterable[Mapping], expected: Iterable[Mapping], columns=LAKE_COLUMNS) -> None:
    actual_counts = row_multiset(actual, columns)
    expected_counts = row_multiset(expected, columns)
    assert actual_counts == expected_counts, {
        "missing": expected_counts - actual_counts,
        "unexpected": actual_counts - expected_counts,
    }


def assert_ingest_ids_unique(rows: Iterable[Mapping]) -> None:
    ids = [row["ingest_id"] for row in rows]
    assert len(ids) == len(set(ids)), f"duplicate ingest_id values found: {ids}"


def assert_receipt_files_intact(root: Path, receipt: Mapping) -> None:
    """Independently validate every receipt path, checksum, byte size and row count."""
    import hashlib

    root = Path(root).resolve()
    for detail in receipt.get("file_details", []):
        target = (root / detail["relative_path"]).resolve()
        assert target.is_relative_to(root), f"receipt path escapes lake: {target}"
        assert target.is_file(), f"receipt file missing: {target}"
        payload = target.read_bytes()
        assert len(payload) == detail["file_size_bytes"]
        assert hashlib.sha256(payload).hexdigest() == detail["sha256"]
        assert pq.ParquetFile(target).metadata.num_rows == detail["row_count"]
