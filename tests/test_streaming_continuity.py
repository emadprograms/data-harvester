"""
Comprehensive Automated Tests for Streaming Data Continuity & Integrity Visualizer (Bird's Eye View).

Context & User Request:
The user wants a "bird's eye view" data continuity ribbon in the SAME tab as the streaming candlestick chart (#stream-tab-chart in the Streaming Dashboard):
- "ribbon lets me see if the data is there. and the chart lets me see if the data is correct."
- "Just look at the 'data' and you instantly spot that something is wrong."
- Built as the combined bird's eye view:
  1. Master Pulse Strip (5 trading days: Mon–Fri, 09:30–16:00 EST): Shows aggregate database health
     (green = all monitored symbols streaming, amber = partial, red = blackout/outage e.g. PC turned off).
  2. Expandable 19-Symbol Spectrogram: Renders the 19 ultra-slim rows (AAPL to TSM) showing continuous data
     vs vertical red cuts across symbols when outages occur.
  3. Incident callout summary (e.g. "🟢 All 19 symbols streamed without interruption this week" or
     "⚠️ 10m gap on Tue 10:14–10:24 AM").
  4. Click-to-sync: Clicking a gap or segment syncs the chart below to inspect that time range.

Coverage:
1. Backend Analytics Engine (src/dashboard/analytics.py):
   - get_streaming_continuity_analysis(days=5, symbol="all", client=None) -> dict
   - Schema validation (days, summary, monitored_symbols_count, view_mode, database)
   - Global blackout detection (status="outage", 15-minute simultaneous gap)
   - Single-symbol partial degradation detection (status="partial", 10-minute gap)
   - Per-symbol filtering (NVDA vs AAPL)
   - Market hours enforcement (09:30-16:00 ET, excluding overnight and weekends)
   - Spectrogram breakdown for 19 monitored symbols
   - Empty database graceful handling

2. REST API Endpoints (src/dashboard/server.py):
   - GET /api/streaming/continuity with ?days=5 and ?symbol=all / ?symbol=NVDA
   - HTTP 200 and JSON response with "database": "streaming"
   - Strict database isolation: never touches historical.duckdb

3. Dashboard HTML Structure (src/dashboard/static/index.html):
   - In #stream-tab-chart, positioned directly above #streaming-chart-container:
     - #streaming-continuity-card or #streaming-continuity-container
     - #continuity-toggle-master and #continuity-toggle-spectrum
     - #continuity-ribbon-canvas or #continuity-ribbon-view
     - #continuity-incident-summary

4. Frontend JS Logic (src/dashboard/static/js/continuity.js or chart.js):
   - Definitions for loadStreamingContinuity, renderContinuityRibbons, toggleContinuityView, syncChartToGap
   - Symbol change in #streaming-symbol-select triggers continuity refresh
   - Chart update triggers continuity refresh
"""
import glob
import os
import re
import socket
import threading
import time
from datetime import datetime, date, time as dtime, timedelta
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

import pytest
import requests
from bs4 import BeautifulSoup

from src.database.connection import (
    DuckDBClient,
    get_historical_db_connection,
    get_streaming_db_connection,
)
from src.database.schema import init_streaming_db
from src.dashboard.server import create_dashboard_server

# Attempt importing backend analytics function; fallback to None for clean test reporting
try:
    from src.dashboard.analytics import get_streaming_continuity_analysis
except (ImportError, AttributeError):
    get_streaming_continuity_analysis = None


# Timezones
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# 19 Monitored Single-Stock Equities (AAPL to TSM)
MONITORED_19_SYMBOLS = [
    "AAPL", "ADBE", "AMD", "AMZN", "APP",
    "AVGO", "BABA", "GOOGL", "META", "MSFT",
    "MU", "NDAQ", "NVDA", "ORCL", "PANW",
    "QCOM", "SHOP", "TSLA", "TSM"
]


# ============================================================================
# Helpers & Fixtures
# ============================================================================

def create_in_memory_streaming_db() -> DuckDBClient:
    """Creates an in-memory DuckDB client initialized with streaming schema & 19 symbols."""
    client = DuckDBClient(":memory:", read_only=False)
    init_streaming_db(client)
    return client


def populate_trading_session_ticks(
    client: DuckDBClient,
    session_date: date,
    symbols: list[str] = MONITORED_19_SYMBOLS,
    start_hour: int = 9,
    start_min: int = 30,
    end_hour: int = 16,
    end_min: int = 0,
    interval_minutes: int = 1,
    blackout_window: tuple[dtime, dtime] | None = None,
    symbol_blackouts: dict[str, tuple[dtime, dtime]] | None = None,
    base_price: float = 100.0,
):
    """
    Populates regular market session ticks (09:30 to 16:00 ET) in DuckDB.
    Timestamps are converted from America/New_York to UTC for storage.
    """
    start_dt_et = datetime(session_date.year, session_date.month, session_date.day, start_hour, start_min, tzinfo=ET)
    end_dt_et = datetime(session_date.year, session_date.month, session_date.day, end_hour, end_min, tzinfo=ET)

    rows = []
    curr_et = start_dt_et
    while curr_et <= end_dt_et:
        curr_time = curr_et.time()

        # Check global blackout
        is_global_blackout = False
        if blackout_window:
            b_start, b_end = blackout_window
            if b_start <= curr_time < b_end:
                is_global_blackout = True

        if not is_global_blackout:
            curr_utc = curr_et.astimezone(UTC)
            ts_str = curr_utc.strftime("%Y-%m-%d %H:%M:%S")

            for sym in symbols:
                # Check per-symbol blackout
                if symbol_blackouts and sym in symbol_blackouts:
                    s_start, s_end = symbol_blackouts[sym]
                    if s_start <= curr_time < s_end:
                        continue  # Skip tick for this symbol

                rows.append((ts_str, sym, base_price, 10.0, base_price - 0.05, base_price + 0.05, "TEST", "REG"))

        curr_et += timedelta(minutes=interval_minutes)

    if rows:
        client.executemany("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)


@pytest.fixture(scope="module")
def api_test_server():
    """Starts an ephemeral ThreadedHTTPServer on an open port for testing REST endpoints."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.2)

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    server.shutdown()
    server.server_close()


@pytest.fixture(scope="module")
def html_soup():
    """Parses src/dashboard/static/index.html using BeautifulSoup."""
    html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src", "dashboard", "static", "index.html")
    assert os.path.exists(html_path), f"File not found: {html_path}"
    with open(html_path, "r", encoding="utf-8") as f:
        return BeautifulSoup(f.read(), "html.parser")


@pytest.fixture(scope="module")
def js_sources():
    """Reads all JavaScript files in src/dashboard/static/js/ into a dict mapping filename -> content."""
    js_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src", "dashboard", "static", "js")
    assert os.path.isdir(js_dir), f"Directory not found: {js_dir}"
    sources = {}
    for p in glob.glob(os.path.join(js_dir, "*.js")):
        fname = os.path.basename(p)
        with open(p, "r", encoding="utf-8") as f:
            sources[fname] = f.read()
    return sources


# ============================================================================
# 1. Backend Analytics Engine (src/dashboard/analytics.py)
# ============================================================================

class TestBackendContinuityAnalyticsEngine:
    """Validates get_streaming_continuity_analysis backend query and analytics logic."""

    def test_continuity_analysis_schema(self):
        """
        get_streaming_continuity_analysis returns a dictionary matching the required contract:
        - database: 'streaming'
        - days: list of day objects with date, day_name, coverage_pct, status, gaps
        - summary: total_gaps, total_outage_minutes, average_coverage
        - monitored_symbols_count: integer count of monitored symbols (19 for 'all')
        - view_mode: 'all' or 'master'
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            # Seed 1 full healthy day (e.g. Wednesday 2026-09-23)
            populate_trading_session_ticks(mem_client, session_date=date(2026, 9, 23))

            res = get_streaming_continuity_analysis(days=5, symbol="all", client=mem_client)

            assert isinstance(res, dict), "Result must be a dictionary"
            assert res.get("database") == "streaming", f"Database must be 'streaming', got {res.get('database')}"
            assert "days" in res, "Result must contain 'days' key"
            assert isinstance(res["days"], list), "'days' must be a list"
            assert len(res["days"]) > 0, "'days' list must not be empty"

            # Check summary schema
            assert "summary" in res, "Result must contain 'summary' key"
            summary = res["summary"]
            assert isinstance(summary, dict), "'summary' must be a dictionary"
            assert "total_gaps" in summary, "summary must contain 'total_gaps'"
            assert "total_outage_minutes" in summary, "summary must contain 'total_outage_minutes'"
            assert "average_coverage" in summary, "summary must contain 'average_coverage'"
            assert isinstance(summary["total_gaps"], int), "total_gaps must be an int"
            assert isinstance(summary["total_outage_minutes"], (int, float)), "total_outage_minutes must be numeric"
            assert isinstance(summary["average_coverage"], (int, float)), "average_coverage must be numeric"

            # Check monitored symbols count and view mode
            assert res.get("monitored_symbols_count") == 19, (
                f"Expected 19 monitored symbols for 'all', got {res.get('monitored_symbols_count')}"
            )
            assert res.get("view_mode") in ["all", "master", "spectrogram"], (
                f"Expected view_mode in ['all', 'master', 'spectrogram'], got {res.get('view_mode')}"
            )

            # Check schema of day objects
            for d in res["days"]:
                assert "date" in d, "Day object must contain 'date'"
                assert "status" in d, "Day object must contain 'status'"
                assert d["status"] in ["healthy", "partial", "outage", "green", "amber", "red"], (
                    f"Unexpected day status '{d.get('status')}'"
                )
                coverage = d.get("coverage_pct", d.get("coverage_percent", d.get("coverage")))
                assert coverage is not None, "Day object must contain coverage percentage"
                assert 0.0 <= coverage <= 100.0, f"Coverage must be between 0 and 100, got {coverage}"
        finally:
            mem_client.close()

    def test_continuity_detects_global_blackout(self):
        """
        Setup test ticks with a 15-minute gap across ALL symbols during trading hours (10:00 to 10:15 ET).
        Asserts gap is classified as global blackout / outage:
        - status='outage' (or type='outage' / 'blackout')
        - duration=15 (or total_outage_minutes >= 15)
        - summary reflects at least 1 gap and at least 15 outage minutes
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            # Populate trading day with 15-minute simultaneous blackout across all symbols (10:00 - 10:15 ET)
            trading_day = date(2026, 9, 23)
            populate_trading_session_ticks(
                mem_client,
                session_date=trading_day,
                blackout_window=(dtime(10, 0), dtime(10, 15))
            )

            res = get_streaming_continuity_analysis(days=5, symbol="all", client=mem_client)

            summary = res.get("summary", {})
            assert summary.get("total_gaps", 0) >= 1, (
                f"Expected at least 1 gap detected for 15m blackout, got {summary.get('total_gaps')}"
            )
            assert summary.get("total_outage_minutes", 0) >= 15, (
                f"Expected at least 15 outage minutes, got {summary.get('total_outage_minutes')}"
            )

            # Verify that an outage-level gap is reported
            all_gaps = summary.get("gaps", [])
            for d in res.get("days", []):
                all_gaps.extend(d.get("gaps", []))

            # Find the blackout gap
            blackout_found = False
            for g in all_gaps:
                dur = g.get("duration", g.get("duration_minutes", g.get("missing_minutes", 0)))
                status = g.get("status", g.get("type", g.get("severity", "")))
                if dur >= 15 and status in ["outage", "blackout", "critical", "red"]:
                    blackout_found = True
                    break

            # If gaps are recorded per-day status
            day_obj = next((d for d in res.get("days", []) if d.get("date") == trading_day.strftime("%Y-%m-%d")), None)
            if day_obj:
                assert day_obj.get("status") in ["outage", "red", "degraded"], (
                    f"Day status for global blackout should be 'outage', got {day_obj.get('status')}"
                )

            assert blackout_found or (day_obj and day_obj.get("status") in ["outage", "red"]), (
                f"15m blackout was not classified as outage status. Gaps: {all_gaps}"
            )
        finally:
            mem_client.close()

    def test_continuity_detects_single_symbol_partial_gap(self):
        """
        Setup test ticks where 18 symbols stream continuously but 1 symbol (NVDA) has a 10-minute gap.
        Asserts partial degradation is captured:
        - Gap is classified as 'partial' (amber), NOT a global blackout
        - The impacted symbol is identified as NVDA
        - Overall coverage reflects partial degradation
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            trading_day = date(2026, 9, 23)
            # NVDA has a gap from 11:00 to 11:10 ET (10 minutes), while remaining 18 symbols stream normally
            populate_trading_session_ticks(
                mem_client,
                session_date=trading_day,
                symbol_blackouts={"NVDA": (dtime(11, 0), dtime(11, 10))}
            )

            res = get_streaming_continuity_analysis(days=5, symbol="all", client=mem_client)

            summary = res.get("summary", {})
            # Verify that partial gap is captured
            all_gaps = summary.get("gaps", [])
            for d in res.get("days", []):
                all_gaps.extend(d.get("gaps", []))

            # Should NOT be classified as total outage across system
            day_obj = next((d for d in res.get("days", []) if d.get("date") == trading_day.strftime("%Y-%m-%d")), None)
            if day_obj:
                assert day_obj.get("status") in ["partial", "amber", "degraded"], (
                    f"Day status with single symbol gap should be 'partial' or 'amber', got {day_obj.get('status')}"
                )

            # Check that NVDA gap of 10m is captured
            nvda_gap = next((g for g in all_gaps if g.get("symbol") == "NVDA" or "NVDA" in g.get("impacted_symbols", [])), None)
            if nvda_gap:
                dur = nvda_gap.get("duration", nvda_gap.get("duration_minutes", nvda_gap.get("missing_minutes", 0)))
                assert dur == 10, f"Expected 10m duration for NVDA gap, got {dur}"
                assert nvda_gap.get("status", nvda_gap.get("type", "partial")) in ["partial", "amber", "symbol_gap"]
            else:
                # If gaps are recorded inside spectrogram breakdown
                spectrogram = res.get("spectrogram", res.get("symbols", {}))
                assert "NVDA" in spectrogram or any("NVDA" in str(x) for x in res.values()), (
                    "Single symbol NVDA degradation must be captured in continuity analysis."
                )
        finally:
            mem_client.close()

    def test_continuity_per_symbol_filtering(self):
        """
        Calling with symbol='NVDA' calculates continuity specific to NVDA:
        - monitored_symbols_count = 1
        - view_mode reflects NVDA
        - If NVDA is healthy while AAPL has a 20-minute gap, NVDA returns 100% coverage and 0 gaps.
        - Calling with symbol='AAPL' detects AAPL's 20-minute gap.
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            trading_day = date(2026, 9, 23)
            # AAPL has a 20-minute gap (13:00-13:20 ET); NVDA has 0 gaps (streams continuously)
            populate_trading_session_ticks(
                mem_client,
                session_date=trading_day,
                symbol_blackouts={"AAPL": (dtime(13, 0), dtime(13, 20))}
            )

            # 1. Query strictly for NVDA
            res_nvda = get_streaming_continuity_analysis(days=5, symbol="NVDA", client=mem_client)
            assert res_nvda.get("monitored_symbols_count") == 1, (
                f"Expected monitored_symbols_count=1 for symbol='NVDA', got {res_nvda.get('monitored_symbols_count')}"
            )
            assert res_nvda.get("view_mode") in ["NVDA", "symbol", "single"], (
                f"Expected view_mode to reflect single symbol, got {res_nvda.get('view_mode')}"
            )
            summary_nvda = res_nvda.get("summary", {})
            assert summary_nvda.get("total_gaps") == 0, (
                f"NVDA streamed continuously, expected 0 gaps but got {summary_nvda.get('total_gaps')}"
            )
            assert summary_nvda.get("total_outage_minutes") == 0
            assert summary_nvda.get("average_coverage") == 100.0

            # 2. Query strictly for AAPL
            res_aapl = get_streaming_continuity_analysis(days=5, symbol="AAPL", client=mem_client)
            summary_aapl = res_aapl.get("summary", {})
            assert summary_aapl.get("total_gaps", 0) >= 1, (
                f"AAPL had a 20m gap, expected at least 1 gap but got {summary_aapl.get('total_gaps')}"
            )
            assert summary_aapl.get("total_outage_minutes", 0) >= 20, (
                f"Expected >= 20 outage minutes for AAPL, got {summary_aapl.get('total_outage_minutes')}"
            )
            assert summary_aapl.get("average_coverage", 100.0) < 100.0, (
                f"AAPL coverage should be < 100%, got {summary_aapl.get('average_coverage')}"
            )
        finally:
            mem_client.close()

    def test_continuity_excludes_market_closed_hours(self):
        """
        Ticks outside 09:30-16:00 ET (overnight between trading days and weekends)
        must NOT produce false gap alerts.
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            # Populate Tuesday 2026-09-22 and Wednesday 2026-09-23 during regular hours only (09:30-16:00 ET)
            day1 = date(2026, 9, 22)
            day2 = date(2026, 9, 23)
            populate_trading_session_ticks(mem_client, session_date=day1)
            populate_trading_session_ticks(mem_client, session_date=day2)

            # Insert pre-market ticks at 08:00 ET, after-hours at 17:30 ET, and weekend Saturday at 12:00 ET
            extra_ticks = [
                # Pre-market Tuesday 08:00 ET (12:00 UTC)
                ("2026-09-22 12:00:00", "NVDA", 100.0, 1.0, 99.9, 100.1, "TEST", "PRE"),
                # After-hours Tuesday 17:30 ET (21:30 UTC)
                ("2026-09-22 21:30:00", "NVDA", 100.0, 1.0, 99.9, 100.1, "TEST", "POST"),
                # Weekend Saturday 2026-09-26 12:00 ET (16:00 UTC)
                ("2026-09-26 16:00:00", "NVDA", 100.0, 1.0, 99.9, 100.1, "TEST", "CLOSED"),
            ]
            mem_client.executemany("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", extra_ticks)

            res = get_streaming_continuity_analysis(days=5, symbol="all", client=mem_client)
            summary = res.get("summary", {})

            # The 17.5-hour overnight gap (16:00 to 09:30 ET) and weekend must NOT be flagged as gaps
            assert summary.get("total_gaps") == 0, (
                f"Overnight/weekend non-market hours produced false gaps: {summary.get('total_gaps')} gaps found"
            )
            assert summary.get("total_outage_minutes") == 0, (
                f"Overnight/weekend non-market hours produced false outage minutes: {summary.get('total_outage_minutes')}"
            )
        finally:
            mem_client.close()

    def test_continuity_empty_database_graceful_handling(self):
        """Empty tick_data table handles cleanly without exceptions or crashes."""
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            res = get_streaming_continuity_analysis(days=5, symbol="all", client=mem_client)
            assert isinstance(res, dict)
            assert res.get("database") == "streaming"
            assert "days" in res
            assert "summary" in res
        finally:
            mem_client.close()

    def test_continuity_spectrogram_symbols_data(self):
        """
        When symbol='all', the result includes breakdown data for the 19 monitored symbols (AAPL to TSM)
        to power the Expandable 19-Symbol Spectrogram.
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            populate_trading_session_ticks(mem_client, session_date=date(2026, 9, 23))
            res = get_streaming_continuity_analysis(days=5, symbol="all", client=mem_client)

            # Check for per-symbol breakdown in top-level 'spectrogram', 'symbols', or within day objects
            spectrogram = res.get("spectrogram", res.get("symbols"))
            if not spectrogram and res.get("days"):
                # Check within first day object
                spectrogram = res["days"][0].get("symbols", res["days"][0].get("spectrogram"))

            assert spectrogram is not None, (
                "Continuity analysis must provide per-symbol spectrogram data for the 19 monitored symbols"
            )
            # Should contain monitored symbols such as AAPL and TSM
            for test_sym in ["AAPL", "NVDA", "TSM"]:
                assert test_sym in spectrogram or any(s.get("symbol") == test_sym for s in spectrogram if isinstance(s, dict)), (
                    f"Symbol '{test_sym}' missing from spectrogram breakdown"
                )
        finally:
            mem_client.close()


# ============================================================================
# 2. REST API Endpoints (src/dashboard/server.py)
# ============================================================================

class TestStreamingContinuityRESTAPI:
    """Tests the /api/streaming/continuity REST endpoint and database isolation."""

    def test_api_streaming_continuity_endpoint(self, api_test_server):
        """
        GET /api/streaming/continuity:
        - Returns HTTP 200 with JSON payload
        - JSON payload contains "database": "streaming"
        - Contains required keys: days, summary, monitored_symbols_count, view_mode
        """
        resp = requests.get(f"{api_test_server}/api/streaming/continuity?days=5&symbol=all")
        assert resp.status_code == 200, f"Expected HTTP 200, got {resp.status_code}: {resp.text}"
        assert "application/json" in resp.headers.get("Content-Type", "")

        data = resp.json()
        assert data.get("database") == "streaming", f"Expected database='streaming', got {data.get('database')}"
        assert "days" in data, "Response JSON must contain 'days'"
        assert "summary" in data, "Response JSON must contain 'summary'"
        assert "monitored_symbols_count" in data, "Response JSON must contain 'monitored_symbols_count'"
        assert "view_mode" in data, "Response JSON must contain 'view_mode'"

    def test_api_streaming_continuity_symbol_parameter(self, api_test_server):
        """GET /api/streaming/continuity accepts ?symbol=NVDA and returns symbol-specific continuity."""
        resp = requests.get(f"{api_test_server}/api/streaming/continuity?symbol=NVDA&days=3")
        assert resp.status_code == 200, f"Expected HTTP 200, got {resp.status_code}: {resp.text}"

        data = resp.json()
        assert data.get("database") == "streaming"
        assert data.get("monitored_symbols_count") == 1
        assert data.get("view_mode") in ["NVDA", "symbol", "single"]

    def test_api_streaming_continuity_database_isolation(self):
        """
        GET /api/streaming/continuity strictly queries streaming.duckdb (tick_data)
        and NEVER touches historical.duckdb.
        """
        def forbidden_historical_call(*args, **kwargs):
            raise AssertionError("VIOLATION: /api/streaming/continuity touched historical.duckdb connection!")

        # Verify backend function never calls historical db connection
        if callable(get_streaming_continuity_analysis):
            with patch("src.dashboard.analytics.get_historical_db_connection", side_effect=forbidden_historical_call):
                res = get_streaming_continuity_analysis(days=5, symbol="all")
                assert res.get("database") == "streaming"

    def test_api_streaming_continuity_default_and_invalid_params(self, api_test_server):
        """Verifies default parameter handling and graceful fallback for invalid inputs."""
        # 1. Calling with no parameters should default cleanly to 5 days, symbol=all
        resp_def = requests.get(f"{api_test_server}/api/streaming/continuity")
        assert resp_def.status_code == 200, f"Expected HTTP 200 for default params, got {resp_def.status_code}"
        assert resp_def.json().get("database") == "streaming"

        # 2. Calling with non-integer days should default safely rather than raising 500 error
        resp_inv = requests.get(f"{api_test_server}/api/streaming/continuity?days=invalid")
        assert resp_inv.status_code in [200, 400], f"Expected 200 fallback or 400 Bad Request, got {resp_inv.status_code}"


# ============================================================================
# 3. Dashboard HTML Structure (src/dashboard/static/index.html)
# ============================================================================

class TestDashboardHTMLContinuityStructure:
    """
    Tests HTML structure in src/dashboard/static/index.html to ensure:
    - Bird's eye view continuity ribbon is placed inside #stream-tab-chart
    - Positioned directly above #streaming-chart-container
    - Controls for Master Pulse vs 19 Symbols Spectrogram exist
    - Canvas/ribbon view area and incident summary badge exist
    """

    def test_continuity_container_in_stream_tab_chart(self, html_soup):
        """In #stream-tab-chart, container #streaming-continuity-card or #streaming-continuity-container must exist."""
        stream_tab = html_soup.select_one("#stream-tab-chart")
        assert stream_tab is not None, "Element #stream-tab-chart must exist in index.html"

        container = stream_tab.select_one("#streaming-continuity-card, #streaming-continuity-container")
        assert container is not None, (
            "In #stream-tab-chart, container #streaming-continuity-card or #streaming-continuity-container must exist."
        )

    def test_continuity_container_positioned_above_chart(self, html_soup):
        """The continuity ribbon container must be positioned above #streaming-chart-container in the DOM."""
        stream_tab = html_soup.select_one("#stream-tab-chart")
        assert stream_tab is not None

        container = stream_tab.select_one("#streaming-continuity-card, #streaming-continuity-container")
        chart_container = stream_tab.select_one("#streaming-chart-container")

        assert container is not None, "Continuity container must exist in #stream-tab-chart"
        assert chart_container is not None, "#streaming-chart-container must exist in #stream-tab-chart"

        # Check DOM ordering
        all_descendants = list(stream_tab.descendants)
        container_idx = all_descendants.index(container)
        chart_idx = all_descendants.index(chart_container)

        assert container_idx < chart_idx, (
            "Continuity ribbon container must be positioned above #streaming-chart-container"
        )

    def test_continuity_toggle_buttons_exist(self, html_soup):
        """
        Continuity container must contain toggle buttons for switching between:
        - Master Pulse (#continuity-toggle-master)
        - 19 Symbols Spectrum (#continuity-toggle-spectrum)
        """
        stream_tab = html_soup.select_one("#stream-tab-chart")
        assert stream_tab is not None

        btn_master = stream_tab.select_one("#continuity-toggle-master")
        btn_spectrum = stream_tab.select_one("#continuity-toggle-spectrum")

        assert btn_master is not None, (
            "Toggle button #continuity-toggle-master must exist in #stream-tab-chart for Master Pulse"
        )
        assert btn_spectrum is not None, (
            "Toggle button #continuity-toggle-spectrum must exist in #stream-tab-chart for 19 Symbols Spectrum"
        )

    def test_continuity_ribbon_display_area_exists(self, html_soup):
        """Continuity container must contain ribbon display area (#continuity-ribbon-canvas or #continuity-ribbon-view)."""
        stream_tab = html_soup.select_one("#stream-tab-chart")
        assert stream_tab is not None

        ribbon = stream_tab.select_one("#continuity-ribbon-canvas, #continuity-ribbon-view")
        assert ribbon is not None, (
            "Ribbon display element #continuity-ribbon-canvas or #continuity-ribbon-view must exist in #stream-tab-chart"
        )

    def test_continuity_incident_summary_badge_exists(self, html_soup):
        """Continuity container must contain incident summary element (#continuity-incident-summary)."""
        stream_tab = html_soup.select_one("#stream-tab-chart")
        assert stream_tab is not None

        summary_badge = stream_tab.select_one("#continuity-incident-summary")
        assert summary_badge is not None, (
            "Incident summary element #continuity-incident-summary must exist in #stream-tab-chart"
        )


# ============================================================================
# 4. Frontend JS Logic (src/dashboard/static/js/)
# ============================================================================

class TestFrontendJSContinuityLogic:
    """
    Tests frontend JavaScript modules to verify:
    - Definitions for loadStreamingContinuity, renderContinuityRibbons, toggleContinuityView, syncChartToGap
    - Symbol changes in #streaming-symbol-select invoke continuity refresh
    - Chart load/update invokes continuity refresh
    """

    def test_js_defines_load_streaming_continuity(self, js_sources):
        """Frontend JS must define loadStreamingContinuity(symbol, days)."""
        combined_js = "\n".join(js_sources.values())
        assert re.search(r"\b(function\s+loadStreamingContinuity|loadStreamingContinuity\s*=\s*(?:function|\()|async\s+function\s+loadStreamingContinuity)\b", combined_js), (
            "Frontend JS must define function 'loadStreamingContinuity(symbol, days)'."
        )

    def test_js_defines_render_continuity_ribbons(self, js_sources):
        """Frontend JS must define renderContinuityRibbons(data)."""
        combined_js = "\n".join(js_sources.values())
        assert re.search(r"\b(function\s+renderContinuityRibbons|renderContinuityRibbons\s*=\s*(?:function|\()|async\s+function\s+renderContinuityRibbons)\b", combined_js), (
            "Frontend JS must define function 'renderContinuityRibbons(data)'."
        )

    def test_js_defines_toggle_continuity_view(self, js_sources):
        """Frontend JS must define toggleContinuityView(mode)."""
        combined_js = "\n".join(js_sources.values())
        assert re.search(r"\b(function\s+toggleContinuityView|toggleContinuityView\s*=\s*(?:function|\()|async\s+function\s+toggleContinuityView)\b", combined_js), (
            "Frontend JS must define function 'toggleContinuityView(mode)'."
        )

    def test_js_defines_sync_chart_to_gap(self, js_sources):
        """Frontend JS must define syncChartToGap(timestamp)."""
        combined_js = "\n".join(js_sources.values())
        assert re.search(r"\b(function\s+syncChartToGap|syncChartToGap\s*=\s*(?:function|\()|async\s+function\s+syncChartToGap)\b", combined_js), (
            "Frontend JS must define function 'syncChartToGap(timestamp)'."
        )

    def test_js_symbol_change_triggers_continuity_refresh(self, js_sources):
        """
        Switching symbol selector (#streaming-symbol-select / handleStreamingSymbolChange)
        must call continuity refresh (loadStreamingContinuity).
        """
        chart_js = js_sources.get("chart.js", "")
        combined_js = "\n".join(js_sources.values())
        # Check that handleStreamingSymbolChange invokes loadStreamingContinuity
        has_symbol_change_continuity = bool(
            re.search(r"handleStreamingSymbolChange[\s\S]*?loadStreamingContinuity", chart_js) or
            re.search(r"handleStreamingSymbolChange[\s\S]*?loadStreamingContinuity", combined_js)
        )
        assert has_symbol_change_continuity, (
            "handleStreamingSymbolChange must invoke loadStreamingContinuity to keep the ribbon synchronized with symbol selection."
        )

    def test_js_chart_update_triggers_continuity_refresh(self, js_sources):
        """
        Calling loadStreamingChart or chart initialization must invoke continuity refresh.
        """
        chart_js = js_sources.get("chart.js", "")
        combined_js = "\n".join(js_sources.values())
        has_chart_refresh_continuity = bool(
            re.search(r"loadStreamingChart[\s\S]*?loadStreamingContinuity", chart_js) or
            re.search(r"loadStreamingChart[\s\S]*?loadStreamingContinuity", combined_js)
        )
        assert has_chart_refresh_continuity, (
            "loadStreamingChart must invoke loadStreamingContinuity to refresh continuity along with chart data."
        )

    def test_continuity_script_or_chart_integration(self, html_soup, js_sources):
        """
        Verify that either a dedicated continuity.js is created and loaded via script tag in index.html,
        or continuity logic is integrated cleanly into chart.js.
        """
        scripts = [s.get("src", "") for s in html_soup.find_all("script") if s.get("src")]
        has_continuity_script = any("continuity.js" in s for s in scripts)
        has_continuity_file = "continuity.js" in js_sources
        has_continuity_in_chart = "loadStreamingContinuity" in js_sources.get("chart.js", "")

        assert (has_continuity_script and has_continuity_file) or has_continuity_in_chart, (
            "Either /static/js/continuity.js must be created and linked in index.html, "
            "or continuity functions must be defined within chart.js."
        )
