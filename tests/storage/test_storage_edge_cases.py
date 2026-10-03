"""
Phase 22: Storage Foundation & Publication Edge Case Test Suite.

Comprehensive edge-case, boundary-condition, and failure-injection test suite
covering Tick Lake storage config, path traversal security, Lake Schema v1 validation,
and atomic publication / crash recovery.

Requirements Covered:
- TEST-P22-01: Storage layout path traversal, unicode/special symbol encoding, and corrupted metadata handling.
- TEST-P22-02: PyArrow schema type coercion, extreme numeric limits (float min/max, subnormal), null bitmasks.
- TEST-P22-03: Atomic publication concurrency collisions, crashed intent recovery, and file lock serialization.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any, List

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from src.storage.config import (
    DATA_DIR_ENV,
    DEFAULT_LAKE_SUBDIR,
    IncompatibleSchemaError,
    LAKE_METADATA_FILENAME,
    LakeMaintenanceInProgressError,
    LakeMetadata,
    LakeNotFoundError,
    MAINTENANCE_GUARD_FILENAME,
    MICRON_DATA_DIR,
    PathTraversalError,
    SAFE_SYMBOL_CHARS,
    StorageConfigError,
    TICK_LAKE_ROOT_ENV,
    decode_symbol,
    encode_symbol,
    get_partition_path,
    init_tick_lake,
    load_lake_metadata,
    resolve_tick_lake_root,
)
from src.storage.publication import (
    LakeOwnershipError,
    LakePublisher,
    LakePublisherLock,
    PublishReceipt,
    recover_pending_publications,
)
from src.storage.schema import (
    LAKE_SCHEMA_V1,
    QuoteTick,
    SCHEMA_V1_COLUMNS,
    SchemaValidationError,
    table_to_ticks,
    ticks_to_table,
    validate_schema_v1,
    validate_table_v1,
)


# =====================================================================
# Group 1: Config, Encoding, Traversal & Lake Metadata (TEST-P22-01)
# =====================================================================


def test_symbol_length_boundary():
    """Exact 64-character boundary (valid) vs 65-character (ValueError)."""
    # 1. Exact 64-char boundary: valid
    sym_64 = "A" * 64
    enc_64 = encode_symbol(sym_64)
    assert enc_64 == sym_64
    assert decode_symbol(enc_64) == sym_64

    # 64-char boundary with special chars that expand when encoded
    sym_64_special = "A" * 62 + ".B"
    assert len(sym_64_special) == 64
    enc_64_special = encode_symbol(sym_64_special)
    assert decode_symbol(enc_64_special) == sym_64_special

    # 2. Exact 65-char boundary: must raise ValueError
    sym_65 = "A" * 65
    with pytest.raises(ValueError, match="exceeds 64"):
        encode_symbol(sym_65)


def test_symbol_invalid_types_and_empty():
    """Empty strings, None, numbers, lists, dicts, booleans raise ValueError."""
    invalid_inputs = ["", None, 123, 45.67, ["AAPL"], {"symbol": "AAPL"}, True, False]
    for bad_input in invalid_inputs:
        with pytest.raises(ValueError):
            encode_symbol(bad_input)  # type: ignore

    # Same check for decode_symbol
    for bad_input in invalid_inputs:
        with pytest.raises(ValueError):
            decode_symbol(bad_input)  # type: ignore


def test_symbol_path_traversal_advanced_patterns():
    """Directory traversal patterns (.., ., ../foo, foo/../bar, AAPL/., foo//bar, \\) raise PathTraversalError."""
    traversal_patterns = [
        "..",
        ".",
        "../foo",
        "foo/../bar",
        "AAPL/.",
        "foo//bar",
        "\\",
        "\\..\\",
        "foo\\bar",
        "foo/./bar",
        "./foo",
        "AAPL/..",
        "../../etc/passwd",
        "//",
        "///",
        "foo///bar",
        "AAPL\x00",
        "\x00NVDA",
    ]
    for pattern in traversal_patterns:
        with pytest.raises(PathTraversalError):
            encode_symbol(pattern)


def test_symbol_encoded_traversal_rejection():
    """%2e%2e, %2E%2E, %00, %5C in decode_symbol raise PathTraversalError."""
    malicious_encodings = [
        "%2e%2e",  # ..
        "%2E%2E",  # ..
        "%00",      # null byte
        "%5C",      # backslash
        "%5c",      # backslash lowercase
        "%2e",      # .
        "%2E",      # .
        "%2F%2E",   # /.
        "%2E%2F",   # ./
        "%2F%2F",   # //
    ]
    for enc in malicious_encodings:
        with pytest.raises(PathTraversalError):
            decode_symbol(enc)


def test_symbol_tampering_and_non_canonical_decoding():
    """Non-canonical casing (%2ea, %2f) and percent-encoding safe chars (%41%42%43) fail roundtrip check."""
    tampered_encodings = [
        "%2ea",       # .a with lowercase hex
        "%2f",        # / with lowercase hex
        "%3d",        # = with lowercase hex
        "%41%42%43",  # ABC encoded when it should be raw
        "%31%32%33",  # 123 encoded
        "%5F",        # _ encoded
        "%2D",        # - encoded
    ]
    for enc in tampered_encodings:
        with pytest.raises(ValueError, match="roundtrip mismatch"):
            decode_symbol(enc)


def test_symbol_malformed_percent_encoding():
    """Incomplete or invalid hex escapes (%, %2, %ZZ, %1G) raise ValueError."""
    malformed_patterns = ["%", "%2", "%ZZ", "%1G", "AAPL%G1", "AAPL%2", "%!1", "foo%ZZbar"]
    for malformed in malformed_patterns:
        with pytest.raises(ValueError, match="Malformed percent encoding"):
            decode_symbol(malformed)


def test_symbol_special_asset_classes_roundtrip():
    """Exhaustive multi-asset conventions: BRK.A, BRK.B, BTC/USDT, ETH/BTC, CL=F, ES_F, ^GSPC, ^VIX, BINANCE:BTCUSDT, @ES, $SPX."""
    asset_symbols = [
        "BRK.A",
        "BRK.B",
        "BTC/USDT",
        "ETH/BTC",
        "CL=F",
        "ES_F",
        "^GSPC",
        "^VIX",
        "BINANCE:BTCUSDT",
        "@ES",
        "$SPX",
    ]
    for sym in asset_symbols:
        encoded = encode_symbol(sym)
        # Ensure filesystem illegal characters are not present in encoded output
        assert "/" not in encoded
        assert "\\" not in encoded
        assert ":" not in encoded
        # Decoding must recover original symbol
        decoded = decode_symbol(encoded)
        assert decoded == sym, f"Failed roundtrip for symbol {sym}: {encoded} -> {decoded}"


def test_symbol_unicode_and_currency_roundtrip():
    """Multi-byte UTF-8 tickers (¥, €, ₽, £, ₹, РУБ, BÉR, ＡＡＰＬ, 🚀) roundtrip cleanly."""
    unicode_symbols = ["¥", "€", "₽", "£", "₹", "РУБ", "BÉR", "ＡＡＰＬ", "🚀"]
    for sym in unicode_symbols:
        encoded = encode_symbol(sym)
        # Verify encoded string is ASCII-safe
        assert all(c in SAFE_SYMBOL_CHARS or c == "%" for c in encoded)
        decoded = decode_symbol(encoded)
        assert decoded == sym, f"Unicode roundtrip mismatch for {sym}: {encoded} -> {decoded}"


def test_partition_path_containment_and_separators(tmp_path):
    """Verifies get_partition_path stays inside ticks/ with exactly 2 subcomponents."""
    lake_root = tmp_path / "lake"
    ticks_dir = lake_root / "ticks"

    p = get_partition_path(lake_root, "BTC/USDT", "2026-10-02")
    assert p.is_relative_to(ticks_dir)

    rel = p.relative_to(ticks_dir)
    assert len(rel.parts) == 2
    assert rel.parts[0] == "symbol=BTC%2FUSDT"
    assert rel.parts[1] == "date=2026-10-02"

    # Traversal in symbol or date must be rejected
    with pytest.raises((ValueError, PathTraversalError)):
        get_partition_path(lake_root, "../escape", "2026-10-02")

    with pytest.raises((ValueError, PathTraversalError)):
        get_partition_path(lake_root, "AAPL", "../../escape")

    with pytest.raises(PathTraversalError):
        get_partition_path(lake_root, "AAPL", "2026-10-02/nested")


def test_partition_path_invalid_dates(tmp_path):
    """Invalid dates (2026-02-30, 2026-13-01, non-dates) raise error."""
    lake_root = tmp_path / "lake"
    invalid_dates = ["2026-02-30", "2026-13-01", "not-a-date", "", None, 12345]
    for bad_date in invalid_dates:
        with pytest.raises((PathTraversalError, ValueError)):
            get_partition_path(lake_root, "AAPL", bad_date)  # type: ignore


def test_root_resolution_empty_env_vars(tmp_path, monkeypatch):
    """TICK_LAKE_ROOT="" falls back cleanly to DATA_DIR or raises StorageConfigError."""
    data_dir = tmp_path / "custom_data"
    data_dir.mkdir()

    # 1. Empty string TICK_LAKE_ROOT falls back to DATA_DIR
    monkeypatch.setenv(TICK_LAKE_ROOT_ENV, "")
    monkeypatch.setenv(DATA_DIR_ENV, str(data_dir))
    resolved = resolve_tick_lake_root(custom_root=None)
    assert resolved == (data_dir / DEFAULT_LAKE_SUBDIR).resolve()

    # 2. Both empty string env vars fall back to hardware or raise StorageConfigError
    monkeypatch.setenv(TICK_LAKE_ROOT_ENV, "")
    monkeypatch.setenv(DATA_DIR_ENV, "")
    monkeypatch.setattr("src.storage.config.MICRON_DATA_DIR", str(tmp_path / "nonexistent_micron"))
    monkeypatch.setattr("pathlib.Path.exists", lambda self: False)

    with pytest.raises(StorageConfigError):
        resolve_tick_lake_root(custom_root=None)


def test_root_resolution_data_is_file_raises(tmp_path, monkeypatch):
    """Repo data path existing as a regular file (not directory) raises StorageConfigError."""
    monkeypatch.delenv(TICK_LAKE_ROOT_ENV, raising=False)
    monkeypatch.delenv(DATA_DIR_ENV, raising=False)
    monkeypatch.setattr("src.storage.config.MICRON_DATA_DIR", str(tmp_path / "nonexistent_micron"))

    # Simulate repo root data existing as a regular file
    orig_is_dir = Path.is_dir
    orig_exists = Path.exists

    monkeypatch.setattr("pathlib.Path.is_symlink", lambda self: False)
    monkeypatch.setattr(
        "pathlib.Path.is_dir",
        lambda self: False if "data" in self.parts else orig_is_dir(self),
    )
    monkeypatch.setattr(
        "pathlib.Path.exists",
        lambda self: True if "data" in self.parts else orig_exists(self),
    )

    with pytest.raises(StorageConfigError, match="Unable to resolve tick lake root"):
        resolve_tick_lake_root(custom_root=None)


def test_load_lake_metadata_corrupt_json(tmp_path):
    """Truncated/corrupt JSON in lake.json raises json.JSONDecodeError."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    lake_json = lake_root / LAKE_METADATA_FILENAME
    lake_json.write_text('{"lake_id": "lake_1", "schema_version": ', encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_lake_metadata(lake_root)


def test_load_lake_metadata_empty_file(tmp_path):
    """0-byte lake.json file raises json.JSONDecodeError."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    lake_json = lake_root / LAKE_METADATA_FILENAME
    lake_json.write_text("", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_lake_metadata(lake_root)


def test_load_lake_metadata_missing_keys(tmp_path):
    """Missing required metadata keys (lake_id or created_at) raises KeyError."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    lake_json = lake_root / LAKE_METADATA_FILENAME
    # Missing created_at
    lake_json.write_text(
        json.dumps({"format": "tick_lake", "schema_version": 1, "lake_id": "lake_001"}),
        encoding="utf-8",
    )
    with pytest.raises(KeyError, match="created_at"):
        load_lake_metadata(lake_root)

    # Missing lake_id
    lake_json.write_text(
        json.dumps({"format": "tick_lake", "schema_version": 1, "created_at": "2026-10-02T00:00:00Z"}),
        encoding="utf-8",
    )
    with pytest.raises(KeyError, match="lake_id"):
        load_lake_metadata(lake_root)


def test_load_lake_metadata_incompatible_format_or_version(tmp_path):
    """Incompatible format string or version numbers raise IncompatibleSchemaError."""
    lake_root = tmp_path / "lake"
    lake_root.mkdir()
    lake_json = lake_root / LAKE_METADATA_FILENAME

    # 1. Incompatible format
    lake_json.write_text(
        json.dumps({
            "lake_id": "lake_1",
            "created_at": "2026-10-02T00:00:00Z",
            "format": "unsupported_lake_format",
            "schema_version": 1,
            "compatible_versions": [1],
        }),
        encoding="utf-8",
    )
    with pytest.raises(IncompatibleSchemaError, match="Unsupported lake format"):
        load_lake_metadata(lake_root)

    # 2. Incompatible schema version
    lake_json.write_text(
        json.dumps({
            "lake_id": "lake_1",
            "created_at": "2026-10-02T00:00:00Z",
            "format": "tick_lake",
            "schema_version": 99,
            "compatible_versions": [99],
        }),
        encoding="utf-8",
    )
    with pytest.raises(IncompatibleSchemaError, match="Incompatible schema version"):
        load_lake_metadata(lake_root)


def test_init_tick_lake_force_overwrite(tmp_path):
    """force=True overwrites metadata with new lake_id; force=False preserves existing metadata."""
    lake_root = tmp_path / "lake"
    meta1 = init_tick_lake(lake_root, force=False)
    original_id = meta1.lake_id

    # force=False retains existing metadata
    meta2 = init_tick_lake(lake_root, force=False)
    assert meta2.lake_id == original_id

    # force=True regenerates metadata
    meta3 = init_tick_lake(lake_root, force=True)
    assert meta3.lake_id != original_id

    # Verify on-disk file has new metadata
    disk_meta = load_lake_metadata(lake_root)
    assert disk_meta.lake_id == meta3.lake_id


def test_init_tick_lake_concurrent_execution(tmp_path):
    """Concurrent threads running init_tick_lake succeed without corruption or race errors."""
    lake_root = tmp_path / "lake"

    def init_worker():
        return init_tick_lake(lake_root)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(init_worker) for _ in range(20)]
        results = [f.result() for f in futures]

    assert len(results) == 20
    # All threads must produce a valid LakeMetadata instance
    for res in results:
        assert isinstance(res, LakeMetadata)
        assert res.format == "tick_lake"

    # Lake file on disk is valid and loadable
    loaded = load_lake_metadata(lake_root)
    assert loaded.format == "tick_lake"


def test_maintenance_guard_lifecycle(tmp_path):
    """_maintenance/in_progress.json raises LakeMaintenanceInProgressError unless check_maintenance=False."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 1. Normal state: metadata loads cleanly
    assert load_lake_metadata(lake_root).format == "tick_lake"

    # 2. Enter maintenance mode
    guard_file = lake_root / "_maintenance" / MAINTENANCE_GUARD_FILENAME
    guard_file.write_text(
        json.dumps({"operation": "schema_upgrade", "started_at": datetime.now(timezone.utc).isoformat()}),
        encoding="utf-8",
    )

    with pytest.raises(LakeMaintenanceInProgressError, match="Lake maintenance in progress"):
        load_lake_metadata(lake_root, check_maintenance=True)

    # Bypass check works
    bypassed_meta = load_lake_metadata(lake_root, check_maintenance=False)
    assert bypassed_meta.format == "tick_lake"

    # 3. Exit maintenance mode
    guard_file.unlink()
    assert load_lake_metadata(lake_root).format == "tick_lake"


# =====================================================================
# Group 2: Lake Schema v1 & Extreme Numeric Limits (TEST-P22-02)
# =====================================================================


def test_schema_v1_extra_and_missing_columns():
    """8-column and 10-column schemas raise SchemaValidationError."""
    # 8-column schema (missing ingest_id)
    schema_8 = pa.schema([LAKE_SCHEMA_V1.field(col) for col in SCHEMA_V1_COLUMNS[:-1]])
    with pytest.raises(SchemaValidationError, match="column count mismatch"):
        validate_schema_v1(schema_8)

    # 10-column schema (extra column appended)
    schema_10 = pa.schema(list(LAKE_SCHEMA_V1) + [pa.field("extra_column", pa.string())])
    with pytest.raises(SchemaValidationError, match="column count mismatch"):
        validate_schema_v1(schema_10)


def test_schema_v1_permuted_column_order():
    """Swapped column order raises SchemaValidationError."""
    # Swap symbol and timestamp
    permuted_fields = [
        LAKE_SCHEMA_V1.field("symbol"),
        LAKE_SCHEMA_V1.field("timestamp"),
    ] + [LAKE_SCHEMA_V1.field(c) for c in SCHEMA_V1_COLUMNS[2:]]
    schema_permuted = pa.schema(permuted_fields)

    with pytest.raises(SchemaValidationError, match="column names mismatch"):
        validate_schema_v1(schema_permuted)


def test_schema_v1_data_type_mismatches():
    """Invalid column data types raise SchemaValidationError."""
    # price as int64
    s1 = LAKE_SCHEMA_V1.set(2, pa.field("price", pa.int64(), nullable=False))
    with pytest.raises(SchemaValidationError, match="Field 'price' type mismatch"):
        validate_schema_v1(s1)

    # timestamp as millisecond instead of microsecond
    s2 = LAKE_SCHEMA_V1.set(0, pa.field("timestamp", pa.timestamp("ms"), nullable=False))
    with pytest.raises(SchemaValidationError, match="Field 'timestamp' type mismatch"):
        validate_schema_v1(s2)

    # volume as int32
    s3 = LAKE_SCHEMA_V1.set(3, pa.field("volume", pa.int32(), nullable=True))
    with pytest.raises(SchemaValidationError, match="Field 'volume' type mismatch"):
        validate_schema_v1(s3)


def test_schema_v1_nullability_mismatches():
    """Inverted nullability constraints raise SchemaValidationError."""
    # symbol nullable=True (required to be non-nullable)
    s1 = LAKE_SCHEMA_V1.set(1, pa.field("symbol", pa.string(), nullable=True))
    with pytest.raises(SchemaValidationError, match="Field 'symbol' nullable mismatch"):
        validate_schema_v1(s1)

    # volume nullable=False (allowed to be nullable)
    s2 = LAKE_SCHEMA_V1.set(3, pa.field("volume", pa.float64(), nullable=False))
    with pytest.raises(SchemaValidationError, match="Field 'volume' nullable mismatch"):
        validate_schema_v1(s2)


def test_schema_v1_dictionary_encoded_symbol():
    """symbol with dictionary(int32, string) passes; dictionary(int32, int64) raises error."""
    # Valid dictionary encoded symbol (string values)
    valid_dict_sym = pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False)
    s_valid = LAKE_SCHEMA_V1.set(1, valid_dict_sym)
    assert validate_schema_v1(s_valid) is True

    # Invalid dictionary encoded symbol (int64 values)
    invalid_dict_sym = pa.field("symbol", pa.dictionary(pa.int32(), pa.int64()), nullable=False)
    s_invalid = LAKE_SCHEMA_V1.set(1, invalid_dict_sym)
    with pytest.raises(SchemaValidationError, match="Field 'symbol' type mismatch"):
        validate_schema_v1(s_invalid)


def test_validation_extreme_float_limits():
    """Subnormal float64 (5e-324, 1e-300), trillion-scale (1e12, 1.79e308), and Satoshi precision pass."""
    extreme_prices = [5e-324, 1e-300, 1e-8, 1e12, 1.79e308, sys.float_info.max]
    ticks = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0, i * 1000),
            symbol="BTC/USD",
            price=p,
            ingest_id=f"extreme_{i}",
        )
        for i, p in enumerate(extreme_prices)
    ]
    table = ticks_to_table(ticks, validate=True)
    assert validate_table_v1(table) is True
    # Values match accurately
    for i, p in enumerate(extreme_prices):
        assert table["price"][i].as_py() == p


def test_validation_subnormal_negative_and_zero_rejection():
    """0.0, -0.0, -5e-324, -0.00001, -1.79e308 raise SchemaValidationError for price."""
    invalid_prices = [0.0, -0.0, -5e-324, -0.00001, -1.79e308]
    for p in invalid_prices:
        with pytest.raises(SchemaValidationError, match="Price contains non-positive values"):
            ticks_to_table(
                [{"timestamp": datetime(2026, 10, 2, 14, 30, 0), "symbol": "AAPL", "price": p, "ingest_id": "inv"}],
                validate=True,
            )


def test_validation_nullable_columns_zero_and_negative():
    """volume=0.0, bid=0.0, ask=0.0 pass; negative values raise SchemaValidationError."""
    t_zero = QuoteTick(
        timestamp=datetime(2026, 10, 2, 14, 30, 0),
        symbol="AAPL",
        price=150.0,
        volume=0.0,
        bid=0.0,
        ask=0.0,
        ingest_id="zero_val",
    )
    table = ticks_to_table([t_zero], validate=True)
    assert validate_table_v1(table) is True
    assert table["volume"][0].as_py() == 0.0
    assert table["bid"][0].as_py() == 0.0
    assert table["ask"][0].as_py() == 0.0

    # Negative values must be rejected
    for field, val in [("volume", -0.001), ("bid", -0.001), ("ask", -0.001)]:
        kwargs = {
            "timestamp": datetime(2026, 10, 2, 14, 30, 0),
            "symbol": "AAPL",
            "price": 150.0,
            "ingest_id": "neg_test",
        }
        kwargs[field] = val
        with pytest.raises(SchemaValidationError, match=f"Column '{field}' contains negative values"):
            ticks_to_table([QuoteTick(**kwargs)], validate=True)


def test_exhaustive_32_null_bitmask_permutations():
    """32 rows representing all binary combinations of nullness across (volume, bid, ask, source, session) validate and roundtrip."""
    ticks = []
    for mask in range(32):
        vol = None if (mask & 1) else float(mask * 10)
        bid = None if (mask & 2) else 150.0 + mask
        ask = None if (mask & 4) else 151.0 + mask
        src = None if (mask & 8) else "CAPITAL"
        sess = None if (mask & 16) else "REG"

        t = QuoteTick(
            timestamp=datetime(2026, 10, 2, 14, 30, 0, mask * 1000),
            symbol="AAPL",
            price=150.0 + mask,
            volume=vol,
            bid=bid,
            ask=ask,
            source=src,
            session=sess,
            ingest_id=f"mask_{mask:02d}",
        )
        ticks.append(t)

    table = ticks_to_table(ticks, validate=True)
    assert validate_table_v1(table) is True

    roundtripped = table_to_ticks(table)
    assert len(roundtripped) == 32

    for i in range(32):
        orig = ticks[i]
        rt = roundtripped[i]
        assert rt.volume == orig.volume, f"Row {i} volume mismatch"
        assert rt.bid == orig.bid, f"Row {i} bid mismatch"
        assert rt.ask == orig.ask, f"Row {i} ask mismatch"
        assert rt.source == orig.source, f"Row {i} source mismatch"
        assert rt.session == orig.session, f"Row {i} session mismatch"


def test_validation_empty_string_in_symbol_or_ingest_id():
    """symbol='' or ingest_id='' raise SchemaValidationError."""
    # Empty symbol
    with pytest.raises(SchemaValidationError, match="Column 'symbol' contains empty strings"):
        ticks_to_table(
            [{"timestamp": datetime(2026, 10, 2, 14, 30, 0), "symbol": "", "price": 100.0, "ingest_id": "id_1"}],
            validate=True,
        )

    # Empty ingest_id
    with pytest.raises(SchemaValidationError, match="Column 'ingest_id' contains empty strings"):
        ticks_to_table(
            [{"timestamp": datetime(2026, 10, 2, 14, 30, 0), "symbol": "AAPL", "price": 100.0, "ingest_id": ""}],
            validate=True,
        )


def test_timestamp_boundaries_and_timezone_normalization():
    """Microsecond timestamps, leap days, UTC-aware and non-UTC timezones normalized to UTC naive timestamp('us')."""
    ts_cases = [
        datetime(1970, 1, 1, 0, 0, 0, 1),                        # 1 microsecond post-epoch
        datetime(2099, 12, 31, 23, 59, 59, 999999),             # Microsecond precision
        datetime(2024, 2, 29, 12, 0, 0),                         # Leap day
        datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc),   # UTC aware
        datetime(2026, 10, 2, 14, 30, 0, 500000, tzinfo=timezone(timedelta(hours=-4))),  # EDT (UTC-4)
    ]
    ticks = [
        QuoteTick(
            timestamp=ts,
            symbol="AAPL",
            price=150.0,
            ingest_id=f"ts_{i}",
        )
        for i, ts in enumerate(ts_cases)
    ]
    table = ticks_to_table(ticks, validate=True)
    assert validate_table_v1(table) is True
    assert table["timestamp"].type == pa.timestamp("us")

    pydict_ts = table["timestamp"].to_pylist()
    assert pydict_ts[0] == datetime(1970, 1, 1, 0, 0, 0, 1)
    assert pydict_ts[1] == datetime(2099, 12, 31, 23, 59, 59, 999999)
    assert pydict_ts[2] == datetime(2024, 2, 29, 12, 0, 0)
    assert pydict_ts[3] == datetime(2026, 10, 2, 14, 30, 0)
    # EDT 14:30:00 -> UTC 18:30:00
    assert pydict_ts[4] == datetime(2026, 10, 2, 18, 30, 0, 500000)


def test_table_to_ticks_dictionary_encoded_symbol():
    """Table with dictionary-encoded symbol column returns QuoteTick with Python string symbol."""
    arr_sym = pa.array(["AAPL", "NVDA", "AAPL"])
    dict_sym = pc.dictionary_encode(arr_sym)

    schema = LAKE_SCHEMA_V1.set(1, pa.field("symbol", pa.dictionary(pa.int32(), pa.string()), nullable=False))
    ts = [datetime(2026, 10, 2, 14, 30, 0, i * 1000) for i in range(3)]
    prices = [150.0, 120.0, 150.5]
    volumes = [10.0, 20.0, 30.0]
    bids = [149.9, 119.9, 150.4]
    asks = [150.1, 120.1, 150.6]
    sources = ["CAPITAL", "CAPITAL", "CAPITAL"]
    sessions = ["REG", "REG", "REG"]
    ingest_ids = ["dict_01", "dict_02", "dict_03"]

    table = pa.Table.from_arrays([
        pa.array(ts, type=pa.timestamp("us")),
        dict_sym,
        pa.array(prices, type=pa.float64()),
        pa.array(volumes, type=pa.float64()),
        pa.array(bids, type=pa.float64()),
        pa.array(asks, type=pa.float64()),
        pa.array(sources, type=pa.string()),
        pa.array(sessions, type=pa.string()),
        pa.array(ingest_ids, type=pa.string()),
    ], schema=schema)

    ticks = table_to_ticks(table)
    assert len(ticks) == 3
    assert ticks[0].symbol == "AAPL" and isinstance(ticks[0].symbol, str)
    assert ticks[1].symbol == "NVDA" and isinstance(ticks[1].symbol, str)
    assert ticks[2].symbol == "AAPL" and isinstance(ticks[2].symbol, str)


# =====================================================================
# Group 3: Atomic Publication, Lock Contention & Crash Recovery (TEST-P22-03)
# =====================================================================


def test_publisher_lock_multithreaded_contention(tmp_path):
    """Thread A holds lock; Thread B receives LakeOwnershipError."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    lock1 = LakePublisherLock(root=lake_root, writer_id="thread_1")
    lock1.acquire()

    contention_caught = []

    def second_thread_worker():
        lock2 = LakePublisherLock(root=lake_root, writer_id="thread_2")
        try:
            lock2.acquire()
        except LakeOwnershipError as e:
            contention_caught.append(e)

    t = threading.Thread(target=second_thread_worker)
    t.start()
    t.join()

    assert len(contention_caught) == 1
    assert "Publisher lock already held" in str(contention_caught[0])

    # Releasing lock allows subsequent acquisition
    lock1.release()
    lock2 = LakePublisherLock(root=lake_root, writer_id="thread_2")
    assert lock2.acquire() is True
    lock2.release()


def test_publisher_lock_multiprocess_contention(tmp_path):
    """Subprocess holds lock; parent receives LakeOwnershipError."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    ready_flag = lake_root / "child_locked.flag"

    child_script = (
        "import pathlib, sys, time\n"
        "from src.storage.publication import LakePublisherLock\n"
        f"lock = LakePublisherLock(pathlib.Path(r'{lake_root}'), 'child_writer')\n"
        "lock.acquire()\n"
        f"pathlib.Path(r'{ready_flag}').write_text('ready')\n"
        "time.sleep(10)\n"
    )

    proc = subprocess.Popen([sys.executable, "-c", child_script])
    try:
        # Wait for child process to acquire lock
        for _ in range(50):
            if ready_flag.is_file():
                break
            time.sleep(0.1)
        assert ready_flag.is_file(), "Child process did not acquire lock in time"

        # Parent process must receive LakeOwnershipError
        with pytest.raises(LakeOwnershipError, match="Publisher lock already held"):
            with LakePublisher(root=lake_root, writer_id="parent_writer"):
                pass
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


def test_atomic_publish_staging_truncated_parquet_failure(tmp_path, monkeypatch):
    """Injected truncated Parquet write raises pa.ArrowInvalid, leaves target partition untouched, cleans up staging file."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    orig_write_table = pq.write_table

    def corrupt_write_table(table, where, **kwargs):
        # Write corrupted/truncated 14 bytes into staging file
        with open(where, "wb") as f:
            f.write(b"PAR1_TRUNCATED")

    monkeypatch.setattr(pq, "write_table", corrupt_write_table)

    ticks = [QuoteTick(datetime(2026, 10, 2, 14, 30, 0), "AAPL", 150.0, ingest_id="t1")]

    publisher = LakePublisher(root=lake_root, writer_id="w1")
    try:
        with pytest.raises(pa.ArrowInvalid):
            publisher.publish_batch(ticks, batch_id="batch_trunc_01", sequence=1)
    finally:
        publisher.close()

    # Target partition must not exist
    target_files = list((lake_root / "ticks").rglob("*.parquet"))
    assert len(target_files) == 0, f"Target directory should have no files: {target_files}"

    # Staging directory must have cleaned up the failed staging file
    staging_files = list((lake_root / "_staging").glob("*.tmp"))
    assert len(staging_files) == 0, f"Dangling staging files found: {staging_files}"

    # Intent file must not exist
    intent_files = list((lake_root / "_control" / "intent").glob("*.json"))
    assert len(intent_files) == 0, f"Intent file should not have been created: {intent_files}"


def test_atomic_publish_os_replace_failure_and_recovery(tmp_path):
    """Simulated os.replace failure preserves intent file; recover_pending_publications recovers and commits staged batch."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    publisher = LakePublisher(root=lake_root, writer_id="w1")

    orig_replace = os.replace

    def fail_on_ticks_rename(src, dst):
        if "ticks" in str(dst):
            raise OSError("Simulated filesystem I/O error moving file to partition")
        return orig_replace(src, dst)

    ticks = [
        QuoteTick(datetime(2026, 10, 2, 14, 30, 0, i * 1000), "AAPL", 150.0 + i, ingest_id=f"tick_{i}")
        for i in range(5)
    ]

    os.replace = fail_on_ticks_rename
    try:
        with pytest.raises(OSError, match="Simulated filesystem I/O error"):
            publisher.publish_batch(ticks, batch_id="crash_replace_001", sequence=1)
    finally:
        os.replace = orig_replace
        publisher.close()

    # Verify intent file was preserved
    intent_file = lake_root / "_control" / "intent" / "crash_replace_001.json"
    assert intent_file.is_file(), "Intent file must be preserved after rename failure"

    # Verify target partition file does not exist yet
    target_file = lake_root / "ticks" / "symbol=AAPL" / "date=2026-10-02" / "batch_w1_000001.parquet"
    assert not target_file.is_file(), "Target partition file should not exist before recovery"

    # Staged file is still in _staging
    staging_files = list((lake_root / "_staging").glob("*.tmp"))
    assert len(staging_files) == 1, "Staged file must remain in _staging"

    # Run recovery
    recovered = recover_pending_publications(lake_root)
    assert len(recovered) == 1
    rec = recovered[0]
    assert rec.batch_id == "crash_replace_001"
    assert rec.status == "PUBLISHED"
    assert rec.row_count == 5

    # Target partition file now exists and is valid
    assert target_file.is_file()
    pf = pq.ParquetFile(target_file)
    assert pf.metadata.num_rows == 5

    # Intent file cleaned up, receipt created
    assert not intent_file.exists(), "Intent file should be deleted upon recovery"
    receipt_file = lake_root / "_control" / "receipts" / "crash_replace_001.json"
    assert receipt_file.is_file(), "Receipt file must be created upon recovery"


def test_recovery_corrupted_staging_and_corrupted_target(tmp_path):
    """Recovery safely refuses corrupted staging or target files (SHA mismatch) without publishing corrupted data."""
    lake_root = tmp_path / "lake"
    init_tick_lake(lake_root)

    # 1. Corrupted staged file: SHA256 does not match intent
    intent_file_stage = lake_root / "_control" / "intent" / "corrupt_stage_batch.json"
    stg_file = lake_root / "_staging" / "corrupt_staged.parquet.tmp"
    stg_file.write_bytes(b"tampered staging bytes")

    intent_payload_stage = {
        "batch_id": "corrupt_stage_batch",
        "writer_id": "w1",
        "sequence": 1,
        "expected_row_count": 1,
        "targets": [{
            "relative_path": "ticks/symbol=AAPL/date=2026-10-02/batch_w1_000001.parquet",
            "staging_path": "_staging/corrupt_staged.parquet.tmp",
            "symbol": "AAPL",
            "date": "2026-10-02",
            "row_count": 1,
            "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        }],
    }
    intent_file_stage.write_text(json.dumps(intent_payload_stage), encoding="utf-8")

    rec1 = recover_pending_publications(lake_root)
    assert rec1 == [], "Recovery must not commit batch with corrupted staging file"
    target_dest = lake_root / "ticks" / "symbol=AAPL" / "date=2026-10-02" / "batch_w1_000001.parquet"
    assert not target_dest.exists(), "Target file must not be created from corrupted staging file"
    assert intent_file_stage.is_file(), "Intent file must be preserved for operator inspection"
    assert not (lake_root / "_control" / "receipts" / "corrupt_stage_batch.json").exists()

    # Clean up stage test intent
    intent_file_stage.unlink()
    stg_file.unlink()

    # 2. Corrupted target file: destination exists but SHA256 does not match intent
    target_dest.parent.mkdir(parents=True, exist_ok=True)
    target_dest.write_bytes(b"tampered target bytes")

    intent_file_target = lake_root / "_control" / "intent" / "corrupt_target_batch.json"
    intent_payload_target = {
        "batch_id": "corrupt_target_batch",
        "writer_id": "w1",
        "sequence": 2,
        "expected_row_count": 1,
        "targets": [{
            "relative_path": "ticks/symbol=AAPL/date=2026-10-02/batch_w1_000001.parquet",
            "symbol": "AAPL",
            "date": "2026-10-02",
            "row_count": 1,
            "sha256": "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        }],
    }
    intent_file_target.write_text(json.dumps(intent_payload_target), encoding="utf-8")

    rec2 = recover_pending_publications(lake_root)
    assert rec2 == [], "Recovery must not acknowledge batch with corrupted target file"
    assert intent_file_target.is_file(), "Intent file must be preserved for operator inspection"
    assert not (lake_root / "_control" / "receipts" / "corrupt_target_batch.json").exists()
