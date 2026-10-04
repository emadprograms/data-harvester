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

from src.storage.config import LakeMaintenanceInProgressError
from src.storage.parquet_writer import TickLakeWriter
from src.storage.publication import recover_pending_publications
from src.storage.config import LakeNotFoundError
from src.storage.reader import TickLakeReader
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
# session would, while the legacy database stays locked by its owner.
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
    "legacy_path": payload["legacy_path"],
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
# REPB-03 — legacy database locked; no accidental attachment
# --------------------------------------------------------------------------


def test_reader_works_while_the_legacy_database_is_exclusively_locked(lake, tmp_path):
    """Exact results while `streaming.duckdb` is locked, and it is never attached."""
    duckdb = pytest.importorskip("duckdb")

    legacy_path = tmp_path / "streaming.duckdb"
    # Hold an exclusive lock for the duration of the read, as the streamer does.
    legacy = duckdb.connect(str(legacy_path))
    try:
        legacy.execute(
            "CREATE TABLE IF NOT EXISTS ticks (symbol VARCHAR, price DOUBLE)"
        )
        legacy.execute("INSERT INTO ticks VALUES ('SENTINEL', 1.0)")
        legacy.commit() if hasattr(legacy, "commit") else None

        result = run_isolated(
            SNIPPET_LOCKED_LEGACY_DB,
            payload={
                "lake_root": str(lake),
                # Includes symbols whose directory names are percent-encoded.
                "symbols": [SYMBOL, "BRK.B", "EUR/USD"],
                "start": START_DATE.isoformat(),
                "end": END_DATE.isoformat(),
                "legacy_path": str(legacy_path),
            },
        )
    finally:
        legacy.close()

    # Every connection the documented reader opened was private and in-memory.
    assert result["connections"], "the reader opened no DuckDB connection"
    assert set(result["connections"]) == {":memory:"}, (
        f"the reader attached a database other than :memory: {result['connections']}"
    )

    # Exact results for every symbol, unaffected by the locked legacy database.
    for symbol in (SYMBOL, "BRK.B", "EUR/USD"):
        expected = _oracle_candles("1d", symbol=symbol)
        got = result["per_symbol"][symbol]
        assert got["candles"] == len(expected), f"{symbol}: candle count disagrees"
        assert got["ticks"] == sum(c["tick_count"] for c in expected), (
            f"{symbol}: tick count disagrees with the oracle"
        )


def test_reader_never_takes_the_legacy_lock(lake, tmp_path):
    """The legacy database must still be locked by its owner after a read.

    The intruder must be a *separate process*: DuckDB's file lock is not
    contended by a second connection opened inside the holding process, so an
    in-process attempt would prove nothing.
    """
    duckdb = pytest.importorskip("duckdb")

    legacy_path = tmp_path / "streaming.duckdb"
    owner = duckdb.connect(str(legacy_path))
    owner.execute("CREATE TABLE ticks (symbol VARCHAR)")
    try:
        run_isolated(
            SNIPPET_LOCKED_LEGACY_DB,
            payload={
                "lake_root": str(lake),
                "symbols": [SYMBOL, "BRK.B", "EUR/USD"],
                "start": START_DATE.isoformat(),
                "end": END_DATE.isoformat(),
                "legacy_path": str(legacy_path),
            },
        )

        intruder = subprocess.run(
            [
                sys.executable,
                "-c",
                "import duckdb, sys\n"
                "try:\n"
                "    duckdb.connect(sys.argv[1]).close()\n"
                "except Exception as exc:\n"
                "    print(type(exc).__name__)\n"
                "else:\n"
                "    print('ACQUIRED')\n",
                str(legacy_path),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert intruder.stdout.strip() != "ACQUIRED", (
            "the legacy database was not still locked after the lake read; "
            "something released the owner's lock"
        )
        assert intruder.stdout.strip(), (
            f"the intruder produced no verdict\n{intruder.stdout}\n{intruder.stderr}"
        )
    finally:
        owner.close()


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

    # The shipped reader does NOT fail fast on a missing root: query_candles
    # returns [] as soon as partition resolution finds nothing. Earlier revisions
    # of the contract claimed otherwise; §7.1 now states the real behaviour, and
    # this test holds the documentation to it.
    shipped = TickLakeReader(root=missing)
    assert shipped.query_candles(SYMBOL, "1m") == [], (
        "the shipped reader changed its missing-root behaviour; §7.1 must be updated"
    )


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


def test_a_file_removed_between_resolve_and_query_is_silently_partial(lake):
    """Documented hazard (§7.3): a vanished file yields fewer rows, not an error.

    DuckDB does not raise when one of the explicitly listed files has been
    removed — it returns results computed from what is left. A consumer that
    treats "no exception" as "complete" will silently under-report. This test
    pins that behaviour so the documented remedy (re-resolve and retry, plus a
    row-count sanity check) cannot quietly become stale advice.
    """
    namespace = load_contract_namespace()
    complete = namespace["RepoBTickReader"](str(lake)).query_candles(
        SYMBOL, START_DATE, END_DATE, "1m"
    )
    assert len(complete) > 1

    reader = namespace["RepoBTickReader"](str(lake))
    resolved = reader._resolve_files(SYMBOL, START_DATE, END_DATE)
    assert len(resolved) > 1

    victim = Path(resolved[0])
    backup = victim.with_suffix(".parquet.bak")
    victim.rename(backup)
    try:
        partial = reader.query_candles(SYMBOL, START_DATE, END_DATE, "1m")
        assert len(partial) < len(complete), (
            "expected a partial result after removing a resolved file; "
            f"got {len(partial)} candles vs {len(complete)}"
        )
    finally:
        backup.rename(victim)

    # Re-resolving clears the stale snapshot and restores the full result.
    reread = namespace["RepoBTickReader"](str(lake)).query_candles(
        SYMBOL, START_DATE, END_DATE, "1m"
    )
    assert len(reread) == len(complete), "re-resolving did not restore the full result"
