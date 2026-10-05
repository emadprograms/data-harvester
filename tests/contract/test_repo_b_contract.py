"""
Repo B contract and integration (Phase 33 / REPB-01..REPB-05).

These tests read the lake the way a third party would: through the examples
published in `docs/contracts/repo_b_tick_lake_contract.md`. The examples are
extracted from the markdown and executed, and for the isolation requirements
they run in a **separate process with `src` imports blocked by an import hook**.
A copy of the examples kept in the test tree would prove nothing about what
Repo B actually pastes into its own codebase.

Two defects in the published contract were found by these tests and are asserted
here against the implementation's real behaviour:

- The document claimed periods stay unescaped (`symbol=BRK.B/`). The encoder's
  safe set is `[A-Za-z0-9_-]`, so the real directory is `symbol=BRK%2EB/`.
  A consumer following the old text would look in the wrong place.
- The document listed `symbol` as Arrow `string`. It is physically written
  dictionary-encoded (`dictionary<values=string, indices=int32>`).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from zoneinfo import ZoneInfo

from src.storage.config import LakeMaintenanceInProgressError
from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import recover_pending_publications
from src.storage.config import LakeNotFoundError
from src.storage.reader import (
    LakeCorruptedMetadataError,
    LakeIncompatibleSchemaError,
    LakeReaderError,
    LakeUnavailableError,
    TickLakeReader,
)
from tests.contract.lake_fixture import (
    ALL_TICKS,
    EDGE_CASE_TICKS,
    build_contract_lake,
)
from tests.fixtures.deterministic_quotes import QuoteTick, calculate_expected_candles
from tests.support.contract_examples import (
    assert_no_product_imports,
    extract_python_blocks,
    load_contract_namespace,
    run_isolated,
)
from tests.support.lake_assertions import finalized_parquet_files, read_final_rows

SYMBOL = "AAPL"
START_DATE = date(2026, 10, 1)
END_DATE = date(2026, 11, 30)
TIMEFRAMES = ("1m", "5m", "1d")

# --------------------------------------------------------------------------
# Snippets executed in the isolated subprocess, after the documented examples.
# --------------------------------------------------------------------------

SNIPPET_CANDLES = """
import json, sys
payload = json.loads(sys.stdin.read())
reader = RepoBTickReader(payload["lake_root"])
out = {}
for tf in payload["timeframes"]:
    candles = reader.query_candles(
        payload["symbol"],
        date.fromisoformat(payload["start"]),
        date.fromisoformat(payload["end"]),
        tf,
    )
    out[tf] = [dict(c, time=c["time"].isoformat()) for c in candles]
print(json.dumps(out))
"""

SNIPPET_TAPE = """
import json, sys
payload = json.loads(sys.stdin.read())
rows = query_tape(Path(payload["lake_root"]), payload["symbol"], payload["limit"])
print(json.dumps([dict(r, timestamp=r["timestamp"].isoformat()) for r in rows]))
"""

SNIPPET_ARROW = """
import json, sys
payload = json.loads(sys.stdin.read())
table = scan_ticks_with_arrow(
    Path(payload["lake_root"]), payload["symbol"], payload["start"], payload["end"]
)
print(json.dumps({"rows": 0 if table is None else table.num_rows}))
"""

SNIPPET_GUARD = """
import json, sys
payload = json.loads(sys.stdin.read())
print(json.dumps(is_lake_maintenance_in_progress(Path(payload["lake_root"]))))
"""

SNIPPET_LOCKED_LEGACY_DB = """
import json, sys
import duckdb

payload = json.loads(sys.stdin.read())
opened = []
_real_connect = duckdb.connect


def _spy(database=":memory:", *args, **kwargs):
    opened.append(str(database))
    return _real_connect(database, *args, **kwargs)


duckdb.connect = _spy

reader = RepoBTickReader(payload["lake_root"])
# "Opens charts and switches symbols": query each symbol in turn, as a UI
# session would. Every connection the documented reader opens must be private
# and in-memory: v5.0 has no disk database to attach.
per_symbol = {}
for symbol in payload["symbols"]:
    candles = reader.query_candles(
        symbol,
        date.fromisoformat(payload["start"]),
        date.fromisoformat(payload["end"]),
        "1d",
    )
    per_symbol[symbol] = {
        "candles": len(candles),
        "ticks": sum(c["tick_count"] for c in candles),
    }
print(json.dumps({
    "connections": opened,
    "per_symbol": per_symbol,
}))
"""

SNIPPET_CANCELLATION = """
import json, sys, threading, time
import duckdb

result = {"interrupted": False, "error": None, "connection_usable": False}
con = duckdb.connect(":memory:")
con.execute("SET max_memory = '1GB'")

# A query long enough to still be in flight when we interrupt it. Sizes escalate
# only if a previous attempt finished first, so the assertion is never vacuous
# but also never depends on a single hard-coded duration.
for size in (50_000_000, 200_000_000, 800_000_000):
    errors = []
    finished = threading.Event()

    def _run():
        try:
            con.execute("SELECT sum(hash(range)) FROM range(?)", [size]).fetchall()
        except Exception as exc:  # noqa: BLE001 - reporting the type is the point
            errors.append(type(exc).__name__)
        finally:
            finished.set()

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    time.sleep(0.05)
    if finished.is_set():
        worker.join()
        continue  # completed before the interrupt could land; try a longer query

    con.interrupt()
    worker.join(timeout=60)
    result["interrupted"] = bool(errors) and "Interrupt" in errors[0]
    result["error"] = errors[0] if errors else None
    break

try:
    con.execute("SELECT 1").fetchall()
    result["connection_usable"] = True
except Exception as exc:  # noqa: BLE001
    result["connection_usable"] = False
    result["error"] = str(exc)
finally:
    con.close()

print(json.dumps(result))
"""

SNIPPET_TRY_IMPORT_SRC = """
import json
try:
    import src  # noqa: F401
except ImportError as exc:
    print(json.dumps({"blocked": True, "message": str(exc)}))
else:
    print(json.dumps({"blocked": False, "message": "src imported successfully"}))
"""


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def lake(tmp_path_factory) -> Path:
    """A lake holding every documented edge case, built by the product writer."""
    root = tmp_path_factory.mktemp("contract_lake") / "lake"
    # build_contract_lake verifies the lake against the independent oracle, so
    # the comparisons below are against exactly the population we think.
    build_contract_lake(root)
    return root


def _oracle_candles(timeframe: str, symbol: str = SYMBOL):
    ticks = [t for t in ALL_TICKS if t.symbol == symbol]
    return [
        {
            "time": c.time,
            "symbol": c.symbol,
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume,
            "tick_count": c.tick_count,
        }
        for c in calculate_expected_candles(ticks, timeframe)
    ]


def _compare(actual, expected, label: str) -> None:
    assert len(actual) == len(expected), (
        f"{label}: expected {len(expected)} candles, got {len(actual)}"
    )
    for got, want in zip(actual, expected):
        assert got["symbol"] == want["symbol"], f"{label}: symbol mismatch"
        assert got["time"] == want["time"], f"{label}: bucket time mismatch {got['time']} != {want['time']}"
        assert got["tick_count"] == want["tick_count"], f"{label}: tick_count mismatch at {want['time']}"
        for field in ("open", "high", "low", "close", "volume"):
            assert abs(got[field] - want[field]) < 1e-9, (
                f"{label}: {field} mismatch at {want['time']}: {got[field]} != {want[field]}"
            )


# --------------------------------------------------------------------------
# REPB-01 — independent consumer, no product imports
# --------------------------------------------------------------------------


def test_documented_examples_contain_no_product_imports():
    """Static check on every python block in the contract document."""
    assert_no_product_imports()
    assert extract_python_blocks(), "the contract document publishes no examples"


def test_isolation_harness_really_blocks_the_product_package():
    """Guard against a vacuous isolation claim: prove the import hook fires."""
    result = run_isolated(SNIPPET_TRY_IMPORT_SRC)
    assert result["blocked"] is True, (
        f"the isolation harness did not block `src`: {result['message']}"
    )


def test_documented_examples_run_in_a_process_without_the_product(lake):
    """The published reader works with `src` unimportable, end to end."""
    result = run_isolated(
        SNIPPET_CANDLES,
        payload={
            "lake_root": str(lake),
            "symbol": SYMBOL,
            "start": START_DATE.isoformat(),
            "end": END_DATE.isoformat(),
            "timeframes": list(TIMEFRAMES),
        },
    )
    assert set(result) == set(TIMEFRAMES)
    assert result["1m"], "the isolated reader returned no 1m candles"


def test_physical_schema_matches_the_documented_contract(lake):
    """Column names, order, physical types and nullability."""
    files = finalized_parquet_files(lake)
    assert files
    schema = pq.ParquetFile(files[0]).schema_arrow

    assert [f.name for f in schema] == [
        "timestamp", "symbol", "price", "volume", "bid", "ask",
        "source", "session", "ingest_id",
    ]

    types = {f.name: str(f.type) for f in schema}
    assert types["timestamp"] == "timestamp[us]", types
    assert types["price"] == "double"
    assert types["volume"] == "double"
    assert types["ingest_id"] == "string"
    # Documented as `string`; physically dictionary-encoded (contract defect).
    assert types["symbol"].startswith("dictionary<"), types

    nullable = {f.name: f.nullable for f in schema}
    for column in ("timestamp", "symbol", "price", "ingest_id"):
        assert nullable[column] is False, f"{column} must be non-nullable"
    for column in ("volume", "bid", "ask", "source", "session"):
        assert nullable[column] is True, f"{column} must be nullable"


def test_symbol_directory_encoding_matches_the_implementation(lake):
    """What the encoder actually produces is what consumers must look for."""
    ticks_root = lake / "ticks"
    names = {p.name for p in ticks_root.iterdir() if p.is_dir()}
    assert "symbol=AAPL" in names
    # The contract claimed `symbol=BRK.B/`; the safe set excludes the period.
    assert "symbol=BRK%2EB" in names, names
    assert "symbol=BRK.B" not in names, names
    assert "symbol=EUR%2FUSD" in names, names


def test_partition_date_is_the_utc_event_date(lake):
    """UTC event date, not exchange-local date and not ingest time."""
    partitions = {
        p.parent.name for p in finalized_parquet_files(lake)
        if p.parent.parent.name == "symbol=AAPL"
    }
    assert "date=2026-10-02" in partitions
    assert "date=2026-10-03" in partitions
    assert "date=2026-11-01" in partitions

    rows = read_final_rows(lake)
    by_id = {row["ingest_id"]: row for row in rows}

    # Written last but carrying an earlier timestamp: it must land in its own
    # event-date partition, not with the rows written around it.
    late = by_id["late_arrival"]
    assert late["timestamp"].date().isoformat() == "2026-10-02"

    # 02:00 UTC on 2026-10-02 is 22:00 ET on 2026-10-01: the UTC date wins.
    boundary = by_id["exchange_boundary"]
    assert boundary["timestamp"].isoformat().startswith("2026-10-02T02:00:00")
    assert any(
        "date=2026-10-02" in str(p) and "symbol=AAPL" in str(p)
        for p in finalized_parquet_files(lake)
    )


def test_every_documented_edge_case_is_present_in_the_fixture(lake):
    """The fixture must actually contain each edge case it claims to cover."""
    ids = {row["ingest_id"] for row in read_final_rows(lake)}
    missing = [f"{name} ({why})" for name, why in EDGE_CASE_TICKS.items() if name not in ids]
    assert not missing, f"edge cases missing from the fixture lake: {missing}"


def test_empty_lake_and_missing_symbol_return_empty_results(tmp_path):
    """Documented behaviour: empty results, not an error."""
    empty_root = tmp_path / "empty_lake"
    empty_root.mkdir()

    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(empty_root))
    assert reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m") == []
    assert reader.query_candles("NOSUCH", START_DATE, END_DATE, "1m") == []
    assert namespace["query_tape"](empty_root, SYMBOL, limit=10) == []


def test_duplicate_multiplicity_and_tie_breaking_are_deterministic(lake):
    """Duplicates keep their multiplicity; ties break on (timestamp, ingest_id)."""
    rows = [r for r in read_final_rows(lake) if r["ingest_id"] == "duplicate_row"]
    assert len(rows) == 2, f"duplicate multiplicity not preserved: {len(rows)} row(s)"

    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    candles = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")

    tie_bucket = [c for c in candles if c["time"] == datetime(2026, 10, 2, 12, 0)]
    assert tie_bucket, "no candle produced for the tie bucket"
    candle = tie_bucket[0]
    # "tie_a" sorts before "tie_b" on (timestamp, ingest_id), even though the
    # rows were written in the opposite order.
    assert candle["open"] == 150.0, candle
    assert candle["close"] == 160.0, candle
    assert candle["tick_count"] == 2, candle

    dup_bucket = [c for c in candles if c["time"] == datetime(2026, 10, 2, 12, 1)]
    assert dup_bucket and dup_bucket[0]["tick_count"] == 2, dup_bucket
    assert abs(dup_bucket[0]["volume"] - 20.0) < 1e-9, dup_bucket


# --------------------------------------------------------------------------
# REPB-02 — examples execute and match an independent oracle
# --------------------------------------------------------------------------


@pytest.mark.parametrize("timeframe", TIMEFRAMES)
def test_documented_candle_example_matches_the_oracle(lake, timeframe):
    """The published resampling SQL must agree with the Python reference oracle."""
    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    actual = reader.query_candles(SYMBOL, START_DATE, END_DATE, timeframe)
    _compare(actual, _oracle_candles(timeframe), f"documented example {timeframe}")


def test_candles_match_the_oracle_for_encoded_symbols(lake):
    """The same oracle holds for symbols whose directory name is escaped."""
    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    for symbol in ("BRK.B", "EUR/USD"):
        actual = reader.query_candles(symbol, START_DATE, END_DATE, "1d")
        _compare(actual, _oracle_candles("1d", symbol=symbol), f"encoded symbol {symbol}")


def test_null_and_zero_volume_follow_the_documented_coalesce_rule(lake):
    """Null volume coalesces to 1.0; an explicit zero stays 0.0."""
    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    candles = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")

    null_bucket = [c for c in candles if c["time"] == datetime(2026, 10, 2, 12, 2)][0]
    assert abs(null_bucket["volume"] - 1.0) < 1e-9, null_bucket

    zero_bucket = [c for c in candles if c["time"] == datetime(2026, 10, 2, 12, 3)][0]
    assert abs(zero_bucket["volume"] - 0.0) < 1e-9, zero_bucket


def test_candles_span_the_dst_transition_deterministically(lake):
    """Bars around the November clock change are produced consistently."""
    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    candles = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
    dst = [c for c in candles if c["time"].date() == date(2026, 11, 1)]
    assert len(dst) == 2, f"expected two bars on the DST date, got {len(dst)}"
    _compare(candles, _oracle_candles("1m"), "dst transition")


def test_documented_tape_example_matches_the_oracle(lake):
    """Reverse-chronological tape ordering and spread calculation."""
    result = run_isolated(
        SNIPPET_TAPE,
        payload={"lake_root": str(lake), "symbol": SYMBOL, "limit": 10},
    )
    assert result, "the tape example returned no rows"
    timestamps = [datetime.fromisoformat(row["timestamp"]) for row in result]
    assert timestamps == sorted(timestamps, reverse=True), "tape is not reverse-chronological"

    keys = [(datetime.fromisoformat(r["timestamp"]), r["ingest_id"]) for r in result]
    assert keys == sorted(keys, reverse=True), "tie-break (timestamp, ingest_id) not respected"

    for row in result:
        if row["bid"] is None or row["ask"] is None:
            assert row["spread"] is None
        else:
            assert abs(row["spread"] - round(row["ask"] - row["bid"], 4)) < 1e-9


def test_documented_arrow_example_returns_the_expected_rows(lake):
    """The PyArrow example filters correctly despite dictionary-encoded symbols."""
    result = run_isolated(
        SNIPPET_ARROW,
        payload={
            "lake_root": str(lake),
            "symbol": SYMBOL,
            "start": "2026-10-01T00:00:00",
            "end": "2026-11-30T00:00:00",
        },
    )
    expected = len([t for t in ALL_TICKS if t.symbol == SYMBOL])
    assert result["rows"] == expected, f"expected {expected} rows, got {result['rows']}"


def test_documented_maintenance_guard_example_detects_the_guard_file(lake, tmp_path):
    """The published three-line guard helper works both ways."""
    guard = lake / "_maintenance" / "in_progress.json"
    guard.parent.mkdir(parents=True, exist_ok=True)

    def _check():
        return run_isolated(SNIPPET_GUARD, payload={"lake_root": str(lake)})

    try:
        guard.write_text("{}", encoding="utf-8")
        assert _check() is True, "the guard example did not detect maintenance"
    finally:
        guard.unlink(missing_ok=True)
    assert _check() is False, "the guard example reported maintenance after it ended"


# --------------------------------------------------------------------------
# REPB-03 — no disk database, and readers open in-memory connections only
# --------------------------------------------------------------------------


def test_reader_opens_only_in_memory_connections_and_creates_no_database(lake, tmp_path):
    """The documented reader is a pure Parquet consumer.

    v5.0 removed the disk-database layer, so the contract is stronger than the old
    "survives a locked legacy database" test: the reader must open private
    ``:memory:`` sessions only, and a full read cycle must not create a ``.duckdb``
    file anywhere on disk (the milestone's freeze semantics).
    """
    result = run_isolated(
        SNIPPET_LOCKED_LEGACY_DB,
        payload={
            "lake_root": str(lake),
            # Includes symbols whose directory names are percent-encoded.
            "symbols": [SYMBOL, "BRK.B", "EUR/USD"],
            "start": START_DATE.isoformat(),
            "end": END_DATE.isoformat(),
        },
    )

    assert result["connections"], "the reader opened no DuckDB connection"
    assert set(result["connections"]) == {":memory:"}, (
        f"the reader attached a database other than :memory: {result['connections']}"
    )

    # Exact results for every symbol.
    for symbol in (SYMBOL, "BRK.B", "EUR/USD"):
        expected = _oracle_candles("1d", symbol=symbol)
        got = result["per_symbol"][symbol]
        assert got["candles"] == len(expected), f"{symbol}: candle count disagrees"
        assert got["ticks"] == sum(c["tick_count"] for c in expected), (
            f"{symbol}: tick count disagrees with the oracle"
        )

    # Freeze semantics: nothing on disk gained a database file, in the lake or beside it.
    strays = sorted(str(path.relative_to(tmp_path)) for path in tmp_path.rglob("*.duckdb*"))
    assert strays == [], f"a read cycle created database files: {strays}"


# --------------------------------------------------------------------------
# REPB-04 — visibility: staging, migration, retired, and fresh reads
# --------------------------------------------------------------------------


def test_reader_excludes_staging_and_migration_artifacts(lake, tmp_path):
    """Only finalized files under ticks/ are visible to a consumer."""
    staging = lake / "_staging"
    staging.mkdir(exist_ok=True)
    (staging / "batch_inflight.parquet.tmp").write_bytes(b"not a parquet file")

    migration_dir = lake / "_migration" / "staging" / "ticks" / "symbol=AAPL" / "date=2026-10-02"
    migration_dir.mkdir(parents=True, exist_ok=True)

    import pyarrow as pa

    sentinel = pa.table(
        {
            "timestamp": pa.array([datetime(2026, 10, 2, 9, 0)], pa.timestamp("us")),
            "symbol": pa.array(["AAPL"], pa.string()),
            "price": pa.array([1.0]),
            "volume": pa.array([1.0]),
            "bid": pa.array([1.0]),
            "ask": pa.array([1.0]),
            "source": pa.array(["MIGRATION"]),
            "session": pa.array(["REG"]),
            "ingest_id": pa.array(["SENTINEL_MIGRATION"]),
        }
    )
    pq.write_table(sentinel, migration_dir / "chunk_000001.parquet")

    retired_dir = lake / "_maintenance" / "retired"
    retired_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(sentinel, retired_dir / "retired_000001.parquet")

    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    candles = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")

    rows = read_final_rows(lake)
    assert not [r for r in rows if r["ingest_id"] == "SENTINEL_MIGRATION"], (
        "a migration or retired artifact was visible to the reader"
    )
    # The published reader prunes at the filesystem level, so candles are unchanged.
    _compare(candles, _oracle_candles("1m"), "visibility")

    resolved = reader._resolve_files(SYMBOL, START_DATE, END_DATE)
    assert all(f"{os.sep}ticks{os.sep}" in str(p) for p in resolved), resolved
    assert all("_staging" not in str(p) for p in resolved), resolved
    assert all("_migration" not in str(p) for p in resolved), resolved


def test_a_fresh_request_observes_newly_finalized_files(lake):
    """A new reader instance sees a batch published after an earlier read."""
    namespace = load_contract_namespace()
    before = namespace["RepoBTickReader"](str(lake))
    before_count = sum(
        c["tick_count"] for c in before.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
    )

    extra = [
        QuoteTick(
            timestamp=datetime(2026, 10, 2, 13, 0, 0),
            symbol=SYMBOL,
            price=175.0,
            volume=2.0,
            bid=174.9,
            ask=175.1,
            source="CAPITAL",
            session="REG",
            ingest_id="fresh_batch_0001",
        )
    ]
    writer = TickLakeWriter(root=lake, writer_id="contract_fresh", max_batch_rows=10**9)
    writer.write_ticks(extra)
    writer.flush(block=True)
    writer.close()

    after = namespace["RepoBTickReader"](str(lake))
    candles = after.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
    after_count = sum(c["tick_count"] for c in candles)

    assert after_count == before_count + 1, (
        f"a fresh request did not observe the new batch ({before_count} -> {after_count})"
    )
    assert [c for c in candles if c["time"] == datetime(2026, 10, 2, 13, 0)], (
        "the new bar is missing"
    )


# --------------------------------------------------------------------------
# REPB-05 — cancellation, cleanup, concurrency, missing roots, maintenance
# --------------------------------------------------------------------------


def test_a_query_in_flight_can_be_cancelled_and_the_connection_survives():
    """Recommended pattern: in-memory connection, interrupt, connection reusable."""
    result = run_isolated(SNIPPET_CANCELLATION)
    assert result["interrupted"] is True, (
        f"the long query was not interrupted (error={result['error']!r})"
    )
    assert result["connection_usable"] is True, "the connection was unusable after a cancel"


def test_reader_connections_are_released(lake):
    """The published examples close their connections; nothing leaks across reads."""
    gc = __import__("gc")
    duckdb = pytest.importorskip("duckdb")

    def _live_connections() -> int:
        gc.collect()
        return sum(
            1
            for obj in gc.get_objects()
            if isinstance(obj, duckdb.DuckDBPyConnection)
        )

    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
    baseline = _live_connections()

    for _ in range(20):
        namespace["RepoBTickReader"](str(lake)).query_candles(SYMBOL, START_DATE, END_DATE, "1m")
        namespace["query_tape"](lake, SYMBOL, limit=5)

    assert _live_connections() <= baseline + 2, (
        "DuckDB connections accumulated across reads"
    )


def test_concurrent_readers_agree(lake):
    """Parallel in-memory readers return identical results."""
    namespace = load_contract_namespace()
    expected = namespace["RepoBTickReader"](str(lake)).query_candles(
        SYMBOL, START_DATE, END_DATE, "1m"
    )

    results: dict[int, list] = {}
    errors: dict[int, BaseException] = {}

    def _read(index: int) -> None:
        try:
            reader = namespace["RepoBTickReader"](str(lake))
            results[index] = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors[index] = exc

    threads = [threading.Thread(target=_read, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)

    assert not errors, f"concurrent readers raised: {errors}"
    assert len(results) == 8
    for index, candles in results.items():
        assert len(candles) == len(expected), f"reader {index} disagreed"
        for got, want in zip(candles, expected):
            assert got["time"] == want["time"] and got["tick_count"] == want["tick_count"]


def test_missing_lake_root_behaves_as_documented(tmp_path):
    """Published examples degrade to empty results; the shipped reader fails fast."""
    missing = tmp_path / "does_not_exist"

    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(missing))
    assert reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m") == []
    assert namespace["query_tape"](missing, SYMBOL, limit=5) == []

    # The shipped reader fails fast with LakeUnavailableError on a missing root
    shipped = TickLakeReader(root=missing)
    with pytest.raises(LakeUnavailableError):
        shipped.query_candles(SYMBOL, "1m")

    # Also fails fast when initialized with validate_root=True
    with pytest.raises(LakeUnavailableError):
        TickLakeReader(root=missing, validate_root=True)


def test_maintenance_guard_pauses_the_reader_and_resumes_afterwards(lake):
    """Guard present: refused. Guard removed: normal results."""
    guard = lake / "_maintenance" / "in_progress.json"
    guard.parent.mkdir(parents=True, exist_ok=True)
    guard.write_text("{}", encoding="utf-8")

    try:
        # Readiness is checked when a connection is opened, not at construction.
        with pytest.raises(LakeMaintenanceInProgressError):
            TickLakeReader(root=lake).query_candles(SYMBOL, "1m")
    finally:
        guard.unlink(missing_ok=True)

    reader = TickLakeReader(root=lake)
    candles = reader.query_candles(SYMBOL, "1m")
    assert candles, "the reader did not resume after maintenance finished"


def test_file_removed_before_resolution_returns_discovered_files_cleanly(lake):
    """Experiment 1 (Before resolution): Vanished file before discovery.

    When a file is removed before resolution occurs, partition discovery simply
    identifies the remaining files. DuckDB executes cleanly over the discovered
    subset without error, returning fewer rows corresponding only to discovered data.
    """
    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    complete = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
    assert len(complete) > 1

    resolved = reader._resolve_files(SYMBOL, START_DATE, END_DATE)
    assert len(resolved) > 1

    victim = Path(resolved[0])
    backup = victim.with_suffix(".parquet.bak")
    victim.rename(backup)
    try:
        # File was removed before query_candles called _resolve_files
        partial = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
        assert len(partial) < len(complete), (
            f"expected fewer candles after pre-resolution file removal: {len(partial)} vs {len(complete)}"
        )
    finally:
        backup.rename(victim)

    restored = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
    assert len(restored) == len(complete), "restoring the file did not restore the full result"


def test_barrier_snapshot_race_file_removed_after_resolution_raises_io_error(lake):
    """Experiment 2 (After resolution with barrier): True snapshot race.

    Capture the exact resolved file list passed to read_parquet(...).
    Block at a barrier immediately before DuckDB executes read_parquet(files).
    In another thread, remove one of the captured files from disk.
    Release the barrier to execute the query.
    Assert that DuckDB raises an explicit duckdb.IOException (file not found),
    and NEVER returns silent partial results.
    """
    import duckdb

    namespace = load_contract_namespace()
    reader = namespace["RepoBTickReader"](str(lake))
    resolved = reader._resolve_files(SYMBOL, START_DATE, END_DATE)
    assert len(resolved) > 1

    victim = Path(resolved[0])
    backup = victim.with_suffix(".parquet.bak")

    barrier_before_query = threading.Event()
    barrier_file_removed = threading.Event()
    thread_result = {"candles": None, "error": None}

    orig_connect = duckdb.connect

    class HookedConnection:
        def __init__(self, con):
            self._con = con

        def execute(self, sql, *args, **kwargs):
            if "read_parquet" in sql:
                barrier_before_query.set()
                if not barrier_file_removed.wait(timeout=10.0):
                    raise TimeoutError("Barrier timeout waiting for victim file removal")
            return self._con.execute(sql, *args, **kwargs)

        def close(self):
            return self._con.close()

        def __getattr__(self, name):
            return getattr(self._con, name)

    def hooked_connect(*args, **kwargs):
        return HookedConnection(orig_connect(*args, **kwargs))

    def reader_worker():
        try:
            candles = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
            thread_result["candles"] = candles
        except Exception as exc:
            thread_result["error"] = exc

    duckdb.connect = hooked_connect
    t = threading.Thread(target=reader_worker)
    try:
        t.start()
        assert barrier_before_query.wait(timeout=5.0), "Reader worker did not reach execution barrier"

        victim.rename(backup)
        barrier_file_removed.set()

        t.join(timeout=10.0)
        assert not t.is_alive(), "Reader worker thread hung"
    finally:
        duckdb.connect = orig_connect
        if backup.exists() and not victim.exists():
            backup.rename(victim)

    # Assert DuckDB raises an explicit IO error and NEVER returns silent partial results
    assert thread_result["candles"] is None, (
        f"DuckDB silently returned partial results ({len(thread_result['candles'])} candles) instead of raising error!"
    )
    err = thread_result["error"]
    assert err is not None, "Expected an error but reader completed without exception"
    assert isinstance(err, duckdb.IOException) or "IO Error" in str(err) or "No files found" in str(err), (
        f"Expected duckdb.IOException or file not found error, got {type(err)}: {err}"
    )


def test_shipped_reader_barrier_snapshot_race_raises_io_error(lake):
    """Verify shipped TickLakeReader also fails fast with IOException when a file vanishes after resolution."""
    import duckdb

    reader = TickLakeReader(root=lake)
    files = reader.resolve_partition_files(SYMBOL)
    assert len(files) > 1

    victim = Path(files[0])
    backup = victim.with_suffix(".parquet.bak")

    barrier_before_query = threading.Event()
    barrier_file_removed = threading.Event()
    thread_result = {"candles": None, "error": None}

    orig_connect = duckdb.connect

    class HookedConnection:
        def __init__(self, con):
            self._con = con

        def execute(self, sql, *args, **kwargs):
            if "read_parquet" in sql:
                barrier_before_query.set()
                if not barrier_file_removed.wait(timeout=10.0):
                    raise TimeoutError("Barrier timeout waiting for victim file removal")
            return self._con.execute(sql, *args, **kwargs)

        def close(self):
            return self._con.close()

        def __getattr__(self, name):
            return getattr(self._con, name)

    def hooked_connect(*args, **kwargs):
        return HookedConnection(orig_connect(*args, **kwargs))

    def reader_worker():
        try:
            candles = reader.query_candles(SYMBOL, "1m")
            thread_result["candles"] = candles
        except Exception as exc:
            thread_result["error"] = exc

    duckdb.connect = hooked_connect
    t = threading.Thread(target=reader_worker)
    try:
        t.start()
        assert barrier_before_query.wait(timeout=5.0), "Reader worker did not reach execution barrier"

        victim.rename(backup)
        barrier_file_removed.set()

        t.join(timeout=10.0)
        assert not t.is_alive(), "Reader worker thread hung"
    finally:
        duckdb.connect = orig_connect
        if backup.exists() and not victim.exists():
            backup.rename(victim)

    assert thread_result["candles"] is None
    err = thread_result["error"]
    assert err is not None
    assert isinstance(err, duckdb.IOException) or "IO Error" in str(err) or "No files found" in str(err)


def test_aware_datetimes_versus_utc_strings_produce_identical_results(lake):
    """READ-04: Aware datetimes (in any timezone) vs UTC strings produce identical candle queries."""
    reader = TickLakeReader(root=lake)

    # 1. Using tz-aware datetime in US/Eastern (EDT is UTC-4 on 2026-10-02)
    start_et = datetime(2026, 10, 2, 8, 0, tzinfo=ZoneInfo("America/New_York"))
    end_et = datetime(2026, 10, 2, 8, 5, tzinfo=ZoneInfo("America/New_York"))
    candles_et = reader.query_candles(SYMBOL, "1m", start=start_et, end=end_et)

    # 2. Using tz-aware datetime in UTC (equivalent instant: 12:00 to 12:05 UTC)
    start_utc = datetime(2026, 10, 2, 12, 0, tzinfo=ZoneInfo("UTC"))
    end_utc = datetime(2026, 10, 2, 12, 5, tzinfo=ZoneInfo("UTC"))
    candles_utc = reader.query_candles(SYMBOL, "1m", start=start_utc, end=end_utc)

    # 3. Using ISO UTC string with Z
    start_str_z = "2026-10-02T12:00:00Z"
    end_str_z = "2026-10-02T12:05:00Z"
    candles_str_z = reader.query_candles(SYMBOL, "1m", start=start_str_z, end=end_str_z)

    # 4. Using naive string formatted as UTC
    start_str_naive = "2026-10-02 12:00:00"
    end_str_naive = "2026-10-02 12:05:00"
    candles_str_naive = reader.query_candles(SYMBOL, "1m", start=start_str_naive, end=end_str_naive)

    assert len(candles_et) > 0, "No candles returned for valid session range"
    assert len(candles_et) == len(candles_utc) == len(candles_str_z) == len(candles_str_naive), (
        f"Mismatch in candle counts: ET={len(candles_et)}, UTC={len(candles_utc)}, "
        f"str_z={len(candles_str_z)}, str_naive={len(candles_str_naive)}"
    )

    for i in range(len(candles_et)):
        assert candles_et[i] == candles_utc[i] == candles_str_z[i] == candles_str_naive[i], (
            f"Candle mismatch at index {i}: {candles_et[i]} vs {candles_utc[i]}"
        )


def test_half_open_intervals_support(lake):
    """READ-04: Verify inclusive_end=True vs inclusive_end=False (half-open [start, end))."""
    reader = TickLakeReader(root=lake)

    # Retrieve ticks for AAPL on 2026-10-02 between 12:00:00 and 12:05:00
    all_ticks = reader.query_ticks(SYMBOL, start="2026-10-02 12:00:00", end="2026-10-02 12:05:00")
    assert len(all_ticks) >= 5, f"Expected at least 5 ticks, got {len(all_ticks)}"

    # Pick the timestamp of the 5th tick (12:02:00) as the cutoff
    cutoff_ts_str = all_ticks[4]["timestamp"]

    # Inclusive end: includes the tick exactly at cutoff_ts_str
    ticks_inclusive = reader.query_ticks(
        SYMBOL,
        start="2026-10-02 12:00:00",
        end=cutoff_ts_str,
        inclusive_end=True,
    )
    # Exclusive end: excludes the tick exactly at cutoff_ts_str
    ticks_exclusive = reader.query_ticks(
        SYMBOL,
        start="2026-10-02 12:00:00",
        end=cutoff_ts_str,
        inclusive_end=False,
    )

    assert len(ticks_inclusive) == 5
    assert len(ticks_exclusive) == 4
    assert cutoff_ts_str not in [t["timestamp"] for t in ticks_exclusive]
    assert any(t["timestamp"] == cutoff_ts_str for t in ticks_inclusive)


def test_reader_root_validation_matrix(tmp_path):
    """READ-01: Verify structured exceptions for unavailable, corrupt, and uninitialized roots."""
    # 1. Nonexistent directory -> LakeUnavailableError
    missing_dir = tmp_path / "does_not_exist"
    reader_missing = TickLakeReader(root=missing_dir)
    with pytest.raises(LakeUnavailableError):
        reader_missing.query_candles(SYMBOL, "1m")
    with pytest.raises(LakeUnavailableError):
        reader_missing.connect()
    with pytest.raises(LakeUnavailableError):
        reader_missing.get_lake_health_report()
    with pytest.raises(LakeUnavailableError):
        TickLakeReader(root=missing_dir, validate_root=True)

    # 2. Path is a file, not a directory -> LakeUnavailableError
    file_not_dir = tmp_path / "a_file.txt"
    file_not_dir.write_text("hello", encoding="utf-8")
    reader_file = TickLakeReader(root=file_not_dir)
    with pytest.raises(LakeUnavailableError):
        reader_file.validate_lake()

    # 3. Missing lake.json -> LakeCorruptedMetadataError
    empty_uninit = tmp_path / "empty_dir"
    empty_uninit.mkdir()
    reader_uninit = TickLakeReader(root=empty_uninit)
    with pytest.raises(LakeCorruptedMetadataError):
        reader_uninit.query_candles(SYMBOL, "1m")
    with pytest.raises(LakeCorruptedMetadataError):
        reader_uninit.validate_lake()

    # 4. Invalid JSON in lake.json -> LakeCorruptedMetadataError
    bad_json_dir = tmp_path / "bad_json"
    bad_json_dir.mkdir()
    (bad_json_dir / "lake.json").write_text("{corrupt: json syntax}", encoding="utf-8")
    reader_bad_json = TickLakeReader(root=bad_json_dir)
    with pytest.raises(LakeCorruptedMetadataError):
        reader_bad_json.query_candles(SYMBOL, "1m")

    # 5. Unsupported schema version -> LakeIncompatibleSchemaError
    bad_ver_dir = tmp_path / "bad_version"
    bad_ver_dir.mkdir()
    bad_meta = {
        "lake_id": "lake_test",
        "created_at": "2026-10-04T00:00:00Z",
        "format": "tick_lake",
        "schema_version": 999,
        "compatible_versions": [999],
    }
    (bad_ver_dir / "lake.json").write_text(json.dumps(bad_meta), encoding="utf-8")
    reader_bad_ver = TickLakeReader(root=bad_ver_dir)
    with pytest.raises(LakeIncompatibleSchemaError):
        reader_bad_ver.query_candles(SYMBOL, "1m")

    # 6. Validly initialized lake with no ticks -> returns legitimate empty results without error
    valid_empty = tmp_path / "valid_empty"
    valid_meta = {
        "lake_id": "lake_empty",
        "created_at": "2026-10-04T00:00:00Z",
        "format": "tick_lake",
        "schema_version": 1,
        "compatible_versions": [1],
    }
    valid_empty.mkdir()
    (valid_empty / "lake.json").write_text(json.dumps(valid_meta), encoding="utf-8")
    reader_valid_empty = TickLakeReader(root=valid_empty)
    assert reader_valid_empty.query_candles(SYMBOL, "1m") == []
    assert reader_valid_empty.query_ticks(SYMBOL) == []
    assert reader_valid_empty.get_tape(SYMBOL)["ticks"] == []
    assert reader_valid_empty.get_candles(SYMBOL)["candles"] == []
    assert reader_valid_empty.get_lake_health_report()["status"] == "empty"
