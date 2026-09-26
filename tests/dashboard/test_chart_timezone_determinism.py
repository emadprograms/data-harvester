"""
Regression tests for exchange-time rendering on the Historical Database page.

Reported bug: the chart X-axis and the hover legend showed the NYSE opening bell — and its
opening volume spike — at "13:30 ET" instead of 09:30 ET, even though 13:30 UTC is the raw
storage value of the 09:30 EDT open.

Root cause: the conversion
``((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York')::TIMESTAMP`` is not
deterministic. Its result depends on

1. the *physical* type of the ``timestamp`` column — over ``TIMESTAMPTZ`` the first hop yields a
   naive UTC wall clock that the second hop then re-interprets as New York time, and
2. the DuckDB session ``TimeZone`` — inherited from the host OS — which resolves the trailing
   ``::TIMESTAMP`` cast.

On a US-Eastern host with a tz-aware column the two hops cancel out, so raw UTC (13:30) reached
the browser while the UI labelled it "ET".

Contract pinned by these tests:
* ``candle["time"]``     -> true UTC epoch seconds (unique + ascending, safe for Lightweight Charts)
* ``candle["time_str"]`` -> the same instant rendered on the NYSE clock (09:30 open, 16:00 close)
* identical results for every storage shape (TIMESTAMP / TIMESTAMPTZ) and every host timezone.
"""
import json
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from src.dashboard.analytics import (
    EXCHANGE_TZ,
    TIME_EPOCH_BASIS,
    build_exchange_local_sql,
    build_instant_from_exchange_local_sql,
    build_timestamp_range_clause,
    build_utc_epoch_sql,
    build_utc_instant_sql,
    detect_timestamp_column_type,
    get_historical_candles,
    get_streaming_candles,
    is_tz_aware_column,
)
from src.database.connection import DuckDBClient

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATIC_DIR = os.path.join(PROJECT_ROOT, "src", "dashboard", "static")

# 2026-07-15 is EDT (UTC-4) and 2026-01-14 is EST (UTC-5): together they prove DST correctness.
EDT_SESSION_DATE = "2026-07-15"
EST_SESSION_DATE = "2026-01-14"
REG_OPEN_ET = "09:30:00"
REG_LAST_BAR_ET = "15:59:00"
OPENING_SPIKE_VOLUME = 5_000_000.0
REGULAR_BAR_VOLUME = 120_000.0


def _utc_epoch(day: str, hour: int, minute: int = 0) -> int:
    return int(datetime.strptime(day, "%Y-%m-%d").replace(hour=hour, minute=minute, tzinfo=timezone.utc).timestamp())


def _reg_open_utc(day: str) -> int:
    """UTC epoch of the 09:30 America/New_York opening bell for a given exchange date."""
    from zoneinfo import ZoneInfo

    local = datetime.strptime(f"{day} {REG_OPEN_ET}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo(EXCHANGE_TZ))
    return int(local.timestamp())


def _session_rows():
    """Synthetic UTC 1-minute bars: pre-market, regular session (with opening spike), post-market."""
    rows = []
    for day, utc_offset in ((EDT_SESSION_DATE, 4), (EST_SESSION_DATE, 5)):
        base = datetime.strptime(day, "%Y-%m-%d")
        reg_open = base + timedelta(hours=9 + utc_offset, minutes=30)
        price = 175.0
        for minute in range(-30, 420):  # 09:00 ET .. 16:29 ET (pre-market tail + post-market head)
            cursor = reg_open + timedelta(minutes=minute)
            session = "REG" if 0 <= minute < 390 else ("PRE" if minute < 0 else "POST")
            volume = REGULAR_BAR_VOLUME
            if minute == 0:
                volume = OPENING_SPIKE_VOLUME  # opening bell volume spike
            if minute == 389:
                volume = 1_500_000.0  # closing auction
            rows.append((
                cursor.strftime("%Y-%m-%d %H:%M:%S"),
                "NVDA",
                round(price, 4), round(price + 0.4, 4), round(price - 0.4, 4), round(price + 0.1, 4),
                volume,
                session,
                "MASSIVE",
            ))
            price += 0.05
    return rows


def _build_historical_db(path, ts_type: str) -> str:
    """Creates a market_data table whose timestamp column is TIMESTAMP or TIMESTAMPTZ."""
    path = str(path)
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    try:
        con.execute(f"""
            CREATE TABLE market_data (
                timestamp {ts_type} NOT NULL,
                symbol VARCHAR NOT NULL,
                open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
                session VARCHAR, source VARCHAR,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        rows = _session_rows()
        if ts_type.upper() == "TIMESTAMPTZ":
            # tz-aware inserts: DuckDB keeps the instant, exactly like a legacy pandas backfill
            rows = [(datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc),) + r[1:] for r in rows]
        con.executemany("INSERT INTO market_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        con.close()
    return path


def _build_streaming_db(path, ts_type: str = "TIMESTAMP") -> str:
    path = str(path)
    if os.path.exists(path):
        os.remove(path)
    con = duckdb.connect(path)
    try:
        con.execute(f"""
            CREATE TABLE ticks (
                timestamp {ts_type} NOT NULL,
                symbol VARCHAR NOT NULL,
                price DOUBLE NOT NULL,
                volume DOUBLE, bid DOUBLE, ask DOUBLE, source VARCHAR, session VARCHAR DEFAULT 'REG'
            )
        """)
        rows = []
        open_utc = datetime.strptime(f"{EDT_SESSION_DATE} 13:30:00", "%Y-%m-%d %H:%M:%S")
        for i in range(180):  # three minutes of ticks across the opening bell
            ts = open_utc + timedelta(seconds=i)
            rows.append((
                ts.replace(tzinfo=timezone.utc) if ts_type.upper() == "TIMESTAMPTZ" else ts,
                "NVDA", 175.0 + i * 0.01, 900.0, 174.99, 175.01, "CAPITAL_STREAM", "REG",
            ))
        con.executemany("INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        con.close()
    return path


@pytest.fixture(scope="module")
def storage_shapes(tmp_path_factory):
    """Both physical storage shapes of the same UTC data."""
    base = tmp_path_factory.mktemp("tz-fixtures")
    return {
        "TIMESTAMP": _build_historical_db(base / "historical_naive.duckdb", "TIMESTAMP"),
        "TIMESTAMPTZ": _build_historical_db(base / "historical_tz.duckdb", "TIMESTAMPTZ"),
    }


@pytest.fixture
def historical_db(monkeypatch, request, storage_shapes):
    """Points the analytics engine at a fixture database of the requested storage shape."""
    ts_type = request.param
    db_path = storage_shapes[ts_type]
    import src.database.connection as conn_mod

    monkeypatch.setattr(conn_mod, "DEFAULT_HISTORICAL_DB_PATH", db_path)
    monkeypatch.setattr(conn_mod, "LEGACY_MARKET_DATA_PATH", db_path)
    return ts_type, db_path


# --- Opening bell must render at 09:30 ET for every storage shape ---------------------------

@pytest.mark.parametrize("historical_db", ["TIMESTAMP", "TIMESTAMPTZ"], indirect=True)
def test_opening_bell_renders_at_0930_et(historical_db):
    """The 09:30 NYSE open (13:30 UTC in EDT) must be labelled 09:30, never 13:30."""
    ts_type, _ = historical_db
    res = get_historical_candles("NVDA", timeframe="1m", start=EDT_SESSION_DATE, end="2026-07-16", limit=2000)
    assert not res.get("error"), res.get("error")
    assert res["count"] > 0

    reg = [c for c in res["candles"] if c["session"] == "REG"]
    assert reg, "fixture must contain regular session bars"

    first = reg[0]
    assert first["time_str"] == f"{EDT_SESSION_DATE} {REG_OPEN_ET}"
    assert first["time_str"].split(" ")[1] != "13:30:00", "opening bell rendered in raw UTC storage time"
    # `time` is the true UTC epoch of the bar, not a shifted exchange-local epoch
    assert first["time"] == _reg_open_utc(EDT_SESSION_DATE)
    assert first["time"] == _utc_epoch(EDT_SESSION_DATE, 13, 30)

    last = reg[-1]
    assert last["time_str"] == f"{EDT_SESSION_DATE} {REG_LAST_BAR_ET}"


@pytest.mark.parametrize("historical_db", ["TIMESTAMP", "TIMESTAMPTZ"], indirect=True)
def test_opening_volume_spike_sits_on_the_0930_bar(historical_db):
    """The opening-bell volume spike must land on the 09:30 ET bar, not on a 13:30 labelled bar."""
    res = get_historical_candles("NVDA", timeframe="1m", start=EDT_SESSION_DATE, end="2026-07-16", limit=2000)
    assert res["count"] > 0

    spike = max(res["candles"], key=lambda c: c["volume"])
    assert spike["volume"] == OPENING_SPIKE_VOLUME
    assert spike["time_str"] == f"{EDT_SESSION_DATE} {REG_OPEN_ET}"
    assert spike["time"] == _reg_open_utc(EDT_SESSION_DATE)


@pytest.mark.parametrize("historical_db", ["TIMESTAMP", "TIMESTAMPTZ"], indirect=True)
def test_est_winter_session_open_is_also_0930_et(historical_db):
    """EST (UTC-5) days must render 09:30 too, proving DST-aware conversion (14:30 UTC storage)."""
    res = get_historical_candles("NVDA", timeframe="1m", start=EST_SESSION_DATE, end="2026-01-15", limit=2000)
    assert res["count"] > 0

    reg = [c for c in res["candles"] if c["session"] == "REG"]
    assert reg[0]["time_str"] == f"{EST_SESSION_DATE} {REG_OPEN_ET}"
    assert reg[0]["time"] == _utc_epoch(EST_SESSION_DATE, 14, 30)


@pytest.mark.parametrize("historical_db", ["TIMESTAMP", "TIMESTAMPTZ"], indirect=True)
def test_candle_payload_declares_contract_and_is_chart_safe(historical_db):
    """Payload advertises the exchange timezone / epoch basis and stays strictly ascending+unique."""
    res = get_historical_candles("NVDA", timeframe="1m", limit=2000)
    assert res["timezone"] == EXCHANGE_TZ == "America/New_York"
    assert res["time_epoch_basis"] == TIME_EPOCH_BASIS == "utc"
    assert res["database"] == "historical"

    times = [c["time"] for c in res["candles"]]
    assert times == sorted(times), "Lightweight Charts requires ascending timestamps"
    assert len(set(times)) == len(times), "duplicate timestamps break Lightweight Charts setData()"


@pytest.mark.parametrize("historical_db", ["TIMESTAMP", "TIMESTAMPTZ"], indirect=True)
def test_daily_buckets_align_to_exchange_midnight(historical_db):
    """1D bars must start at exchange-local midnight (04:00 UTC in EDT), not at UTC midnight."""
    res = get_historical_candles("NVDA", timeframe="1d", limit=50)
    assert res["count"] > 0

    by_day = {c["time_str"]: c for c in res["candles"]}
    assert f"{EDT_SESSION_DATE} 00:00:00" in by_day

    edt_day = by_day[f"{EDT_SESSION_DATE} 00:00:00"]
    assert edt_day["time"] == _utc_epoch(EDT_SESSION_DATE, 4, 0), "daily bucket anchored to UTC midnight"
    # The bucket must hold only that exchange day's bars: the 09:30 opening spike is inside it.
    assert edt_day["volume"] >= OPENING_SPIKE_VOLUME

    est_day = by_day[f"{EST_SESSION_DATE} 00:00:00"]
    assert est_day["time"] == _utc_epoch(EST_SESSION_DATE, 5, 0), "EST daily bucket must anchor at 05:00 UTC"


@pytest.mark.parametrize("historical_db", ["TIMESTAMP", "TIMESTAMPTZ"], indirect=True)
def test_intraday_buckets_snap_to_exchange_clock(historical_db):
    """Aggregated intraday buckets keep exchange-local labels and true UTC epochs."""
    res = get_historical_candles("NVDA", timeframe="5m", start=EDT_SESSION_DATE, end="2026-07-16", limit=500)
    assert res["count"] > 0

    open_bucket = next((c for c in res["candles"] if c["time_str"] == f"{EDT_SESSION_DATE} 09:30:00"), None)
    assert open_bucket is not None, "expected a 5m bucket opening exactly at 09:30 ET"
    assert open_bucket["time"] == _reg_open_utc(EDT_SESSION_DATE)
    # The bucket must hold exactly the 09:30..09:34 ET bars: opening spike + four regular bars.
    assert open_bucket["volume"] == OPENING_SPIKE_VOLUME + (4 * REGULAR_BAR_VOLUME)

    labels = [c["time_str"][11:16] for c in res["candles"]]
    assert labels == sorted(labels)
    assert "09:30" in labels and "09:35" in labels


# --- The SQL conversion itself must ignore the DuckDB session timezone ----------------------

@pytest.mark.parametrize("session_tz", ["Etc/UTC", "America/New_York", "Asia/Bahrain", "Europe/London"])
@pytest.mark.parametrize("ts_type", ["TIMESTAMP", "TIMESTAMPTZ"])
def test_exchange_local_sql_is_session_timezone_independent(storage_shapes, session_tz, ts_type):
    """No session TimeZone may change the rendered exchange wall clock or epoch."""
    con = duckdb.connect(storage_shapes[ts_type], read_only=True)
    try:
        con.execute(f"SET TimeZone='{session_tz}'")
        local_sql = build_exchange_local_sql(ts_type)
        instant_sql = build_utc_instant_sql(ts_type)
        epoch_sql = build_utc_epoch_sql(ts_type)
        row = con.execute(f"""
            SELECT strftime({local_sql}, '%Y-%m-%d %H:%M:%S') AS et_str,
                   {epoch_sql} AS epoch_sec
            FROM market_data
            WHERE symbol = 'NVDA' AND session = 'REG'
              AND strftime({local_sql}, '%Y-%m-%d') = '{EDT_SESSION_DATE}'
            ORDER BY {instant_sql} ASC
            LIMIT 1
        """).fetchone()
    finally:
        con.close()

    assert row[0] == f"{EDT_SESSION_DATE} {REG_OPEN_ET}"
    assert int(row[1]) == _reg_open_utc(EDT_SESSION_DATE)


def test_bucket_expression_round_trips_to_a_true_utc_epoch():
    """An exchange-local bucket must convert back to the real instant (used for aggregated bars)."""
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='America/New_York'")
        expr = build_instant_from_exchange_local_sql("TIMESTAMP '2026-07-15 09:30:00'")
        assert int(con.execute(f"SELECT epoch({expr})").fetchone()[0]) == _reg_open_utc(EDT_SESSION_DATE)
    finally:
        con.close()


@pytest.mark.parametrize("ts_type", ["TIMESTAMP", "TIMESTAMPTZ"])
def test_column_type_detection_drives_the_conversion_branch(storage_shapes, ts_type):
    """detect_timestamp_column_type() decides which conversion branch is used."""
    expected_aware = ts_type == "TIMESTAMPTZ"
    client = DuckDBClient(db_path=storage_shapes[ts_type], read_only=True)
    try:
        detected = detect_timestamp_column_type(client, "market_data")
        assert is_tz_aware_column(detected) is expected_aware
        if expected_aware:
            assert detected == "TIMESTAMP WITH TIME ZONE"
            assert build_utc_instant_sql(detected) == "timestamp::TIMESTAMPTZ"
        else:
            assert detected == "TIMESTAMP"
            assert build_utc_instant_sql(detected) == "timezone('UTC', timestamp::TIMESTAMP)"
    finally:
        client.close()


def test_range_filter_matches_utc_wall_clock_for_both_shapes(storage_shapes):
    """start/end parameters keep their UTC semantics for naive and tz-aware storage alike."""
    for ts_type, db_path in storage_shapes.items():
        clause_gte = build_timestamp_range_clause(ts_type, "timestamp", ">=")
        clause_lte = build_timestamp_range_clause(ts_type, "timestamp", "<=")
        con = duckdb.connect(db_path, read_only=True)
        try:
            con.execute("SET TimeZone='America/New_York'")
            count = con.execute(f"""
                SELECT COUNT(*) FROM market_data
                WHERE symbol = 'NVDA' AND {clause_gte} AND {clause_lte}
            """, [f"{EDT_SESSION_DATE} 13:30:00", f"{EDT_SESSION_DATE} 19:59:00"]).fetchone()[0]
        finally:
            con.close()
        assert count == 390, f"regular session range filter broken for {ts_type}"


# --- Connections must pin the session timezone ---------------------------------------------

@pytest.mark.parametrize("read_only", [False, True])
def test_duckdb_client_pins_session_timezone_to_utc(tmp_path, read_only):
    """Every connection is pinned to UTC so host timezones cannot change query semantics."""
    db_path = str(tmp_path / "pinned.duckdb")
    writer = DuckDBClient(db_path=db_path)
    try:
        writer.execute("CREATE TABLE t (timestamp TIMESTAMP)")
        writer.execute("INSERT INTO t VALUES ('2026-07-15 13:30:00')")
    finally:
        writer.close()

    client = DuckDBClient(db_path=db_path, read_only=read_only)
    try:
        from src.database.connection import SESSION_TIMEZONE

        assert SESSION_TIMEZONE == "UTC"
        assert client.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
    finally:
        client.close()


# --- Host timezone end-to-end (fresh process, because ICU caches the default zone) ----------

_HOST_TZ_PROBE = textwrap.dedent(
    """
    import json, sys
    sys.path.insert(0, {root!r})
    import src.database.connection as conn
    conn.DEFAULT_HISTORICAL_DB_PATH = sys.argv[1]
    conn.LEGACY_MARKET_DATA_PATH = sys.argv[1]
    from src.dashboard.analytics import get_historical_candles
    payload = get_historical_candles("NVDA", timeframe="1m", start="{day}", end="{day_end}", limit=2000)
    print(json.dumps(payload))
    """
).strip()


@pytest.mark.parametrize("host_tz", ["UTC", "America/New_York", "Asia/Bahrain"])
@pytest.mark.parametrize("ts_type", ["TIMESTAMP", "TIMESTAMPTZ"])
def test_host_timezone_does_not_move_the_opening_bell(storage_shapes, host_tz, ts_type):
    """The exact reported symptom: a US-Eastern host must not shift the open to 13:30."""
    code = _HOST_TZ_PROBE.format(root=PROJECT_ROOT, day=EDT_SESSION_DATE, day_end="2026-07-16")
    proc = subprocess.run(
        [sys.executable, "-c", code, storage_shapes[ts_type]],
        capture_output=True,
        text=True,
        env={**os.environ, "TZ": host_tz, "PYTHONPATH": PROJECT_ROOT},
        cwd=PROJECT_ROOT,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr

    payload = json.loads(proc.stdout.strip().splitlines()[-1])
    assert payload["storage_timestamp_type"] == ("TIMESTAMP WITH TIME ZONE" if ts_type == "TIMESTAMPTZ" else "TIMESTAMP")

    reg = [c for c in payload["candles"] if c["session"] == "REG"]
    assert reg, payload.get("error")
    assert reg[0]["time_str"] == f"{EDT_SESSION_DATE} {REG_OPEN_ET}"
    assert reg[0]["time"] == _reg_open_utc(EDT_SESSION_DATE)

    spike = max(payload["candles"], key=lambda c: c["volume"])
    assert spike["time_str"].endswith(REG_OPEN_ET), f"volume spike rendered at {spike['time_str']} on TZ={host_tz}"


# --- Streaming buffer follows the same contract --------------------------------------------

def test_streaming_candles_render_exchange_time(tmp_path, monkeypatch):
    """Tick-to-bar resampling on the Streaming source must open at 09:30 ET as well."""
    import src.database.connection as conn_mod

    db_path = _build_streaming_db(tmp_path / "streaming.duckdb")
    monkeypatch.setattr(conn_mod, "DEFAULT_STREAMING_DB_PATH", db_path)

    res = get_streaming_candles("NVDA", timeframe="1m", limit=50)
    assert not res.get("error"), res.get("error")
    assert res["timezone"] == EXCHANGE_TZ
    assert res["time_epoch_basis"] == TIME_EPOCH_BASIS
    assert res["count"] > 0

    first = res["candles"][0]
    assert first["time_str"] == f"{EDT_SESSION_DATE} {REG_OPEN_ET}"
    assert first["time"] == _reg_open_utc(EDT_SESSION_DATE)


def test_streaming_candles_tz_aware_storage(tmp_path, monkeypatch):
    """A tz-aware ticks column resamples to the same exchange-local buckets."""
    import src.database.connection as conn_mod

    db_path = _build_streaming_db(tmp_path / "streaming_tz.duckdb", ts_type="TIMESTAMPTZ")
    monkeypatch.setattr(conn_mod, "DEFAULT_STREAMING_DB_PATH", db_path)

    res = get_streaming_candles("NVDA", timeframe="1m", limit=50)
    assert res["count"] > 0
    assert res["storage_timestamp_type"] == "TIMESTAMP WITH TIME ZONE"
    assert res["candles"][0]["time_str"] == f"{EDT_SESSION_DATE} {REG_OPEN_ET}"
    assert res["candles"][0]["time"] == _reg_open_utc(EDT_SESSION_DATE)


# --- Front-end must format the axis in the exchange timezone -------------------------------

def test_chart_js_formats_axis_and_legend_in_exchange_timezone():
    """Axis ticks, crosshair badge and legend are formatted with an explicit IANA timezone."""
    with open(os.path.join(STATIC_DIR, "js", "chart.js"), "r", encoding="utf-8") as f:
        js = f.read()

    assert "America/New_York" in js
    assert "timeZone: chartTimezone" in js
    assert "tickMarkFormatter" in js
    assert "timeFormatter" in js
    assert "time_str" in js
    assert "setChartTimezone(data.timezone)" in js
    # UTC-based labelling (the old `toISOString()` / `getUTCHours()` path) must be gone
    assert "toISOString" not in js
    assert "getUTCHours" not in js


def test_chart_js_tick_labels_follow_the_exchange_day():
    """Date tick marks are only drawn at exchange-local midnight, never at UTC midnight."""
    with open(os.path.join(STATIC_DIR, "js", "chart.js"), "r", encoding="utf-8") as f:
        js = f.read()

    assert "isExchangeDayStart" in js
    assert "formatExchangeClock" in js
    assert "formatExchangeDay" in js


def test_inspector_table_and_badges_still_advertise_exchange_time():
    """The raw candle inspector keeps its US/Eastern labelling."""
    with open(os.path.join(STATIC_DIR, "index.html"), "r", encoding="utf-8") as f:
        html = f.read()
    assert "legend-tz-badge" in html
    assert "NYSE (ET)" in html
    assert "Timestamp (US/Eastern)" in html

    with open(os.path.join(STATIC_DIR, "js", "tables.js"), "r", encoding="utf-8") as f:
        tables = f.read()
    assert "Timestamp (US/Eastern)" in tables
    assert "time_str" in tables
