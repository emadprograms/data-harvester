"""
Automated Integration and Unit Tests for Dashboard Segregation.
Enforces clean segregation between:
1. "Historical Dashboard" - dedicated exclusively to historical database contents:
   - minute_data in data/historical.duckdb
   - historical symbol coverage matrix
   - REST harvester runner
   - dedicated historical chart without inline streaming toggle
2. "Streaming Dashboard" - dedicated exclusively to streaming database contents:
   - tick_data in data/streaming.duckdb
   - live tick tape
   - dedicated live streaming chart without inline historical toggle
   - streamer daemon controls
   - streaming symbols selector

Table Naming Convention:
- minute_data in historical.duckdb (canonical 1m bars, alongside backward-compatible market_data)
- tick_data in streaming.duckdb (raw ticks, alongside backward-compatible ticks)

Test Structure:
- TestBackendQueryTableSupport: Backend queries against minute_data and tick_data, connection exclusivity.
- TestDashboardHTMLStructureSegregation: Sidebar navigation element, distinct containers, dedicated charts, no cross-mode toggles.
- TestFrontendJSLogicSegregation: Separated chart controllers, independent symbol selectors.
- TestE2ERESTAPISegregation: Live HTTP endpoint verification on threaded dashboard server.
"""
import glob
import os
import re
import socket
import threading
import time
from unittest.mock import patch, MagicMock

import pytest
import requests
from bs4 import BeautifulSoup

from src.database.connection import (
    DuckDBClient,
    get_historical_db_connection,
    get_streaming_db_connection,
    DEFAULT_HISTORICAL_DB_PATH,
    DEFAULT_STREAMING_DB_PATH,
)
from src.dashboard.analytics import (
    get_historical_candles,
    get_streaming_candles,
    get_candles,
    get_stream_tape,
    get_ticks,
)
from src.dashboard.server import create_dashboard_server


# ============================================================================
# Fixtures
# ============================================================================

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
# 1. Backend Query Table Support
# ============================================================================

class TestBackendQueryTableSupport:
    """Tests backend analytics queries against minute_data and tick_data tables and strict isolation."""

    def test_get_historical_candles_queries_minute_data_table(self):
        """get_historical_candles queries minute_data and succeeds when only minute_data exists."""
        mem_client = DuckDBClient(":memory:", read_only=False)
        try:
            mem_client.execute("""
                CREATE TABLE minute_data (
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR NOT NULL,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume DOUBLE,
                    source VARCHAR,
                    session VARCHAR,
                    PRIMARY KEY (timestamp, symbol)
                )
            """)
            mem_client.execute("""
                INSERT INTO minute_data VALUES
                ('2026-09-25 14:30:00', 'AAPL', 150.0, 151.0, 149.5, 150.5, 1000.0, 'MASSIVE', 'REG'),
                ('2026-09-25 14:31:00', 'AAPL', 150.5, 152.0, 150.0, 151.5, 1500.0, 'MASSIVE', 'REG')
            """)

            mem_client.close = MagicMock()
            with patch("src.dashboard.analytics.get_historical_db_connection", return_value=mem_client):
                res = get_historical_candles("AAPL", timeframe="1m", limit=10)

                assert res.get("database") == "historical", "Database label must be 'historical'"
                assert res.get("symbol") == "AAPL"
                assert res.get("count") == 2, f"Expected 2 candles, got {res.get('count')}"
                candles = res.get("candles", [])
                assert len(candles) == 2
                assert candles[0]["open"] == 150.0
                assert candles[1]["close"] == 151.5

                # Also test multi-minute aggregation from minute_data
                res_agg = get_historical_candles("AAPL", timeframe="5m", limit=10)
                assert res_agg.get("database") == "historical"
                assert res_agg.get("count") == 1
                assert res_agg["candles"][0]["open"] == 150.0
                assert res_agg["candles"][0]["high"] == 152.0
                assert res_agg["candles"][0]["close"] == 151.5
        finally:
            if mem_client.conn:
                try:
                    mem_client.conn.close()
                except Exception:
                    pass

    def test_get_streaming_candles_queries_tick_data_table(self):
        """get_streaming_candles queries tick_data and succeeds when only tick_data exists."""
        mem_client = DuckDBClient(":memory:", read_only=False)
        try:
            mem_client.execute("""
                CREATE TABLE tick_data (
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR NOT NULL,
                    price DOUBLE NOT NULL,
                    volume DOUBLE,
                    bid DOUBLE,
                    ask DOUBLE,
                    source VARCHAR,
                    session VARCHAR
                )
            """)
            mem_client.execute("""
                INSERT INTO tick_data VALUES
                ('2026-09-25 14:30:05', 'AAPL', 150.5, 10.0, 150.4, 150.6, 'CAPITAL', 'REG'),
                ('2026-09-25 14:30:25', 'AAPL', 151.0, 15.0, 150.9, 151.1, 'CAPITAL', 'REG'),
                ('2026-09-25 14:30:50', 'AAPL', 150.8, 5.0, 150.7, 150.9, 'CAPITAL', 'REG')
            """)

            mem_client.close = MagicMock()
            with patch("src.dashboard.analytics.get_streaming_db_connection", return_value=mem_client):
                res = get_streaming_candles("AAPL", timeframe="1m", limit=10)

                assert res.get("database") == "streaming", "Database label must be 'streaming'"
                assert res.get("symbol") == "AAPL"
                assert res.get("count") == 1, f"Expected 1 candle aggregated from ticks, got {res.get('count')}"
                candle = res["candles"][0]
                assert candle["open"] == 150.5
                assert candle["high"] == 151.0
                assert candle["low"] == 150.5
                assert candle["close"] == 150.8
                assert candle["tick_count"] == 3
        finally:
            if mem_client.conn:
                try:
                    mem_client.conn.close()
                except Exception:
                    pass

    def test_get_stream_tape_queries_tick_data_table(self):
        """get_stream_tape queries tick_data and succeeds when only tick_data exists."""
        mem_client = DuckDBClient(":memory:", read_only=False)
        try:
            mem_client.execute("""
                CREATE TABLE tick_data (
                    timestamp TIMESTAMP NOT NULL,
                    symbol VARCHAR NOT NULL,
                    price DOUBLE NOT NULL,
                    volume DOUBLE,
                    bid DOUBLE,
                    ask DOUBLE,
                    source VARCHAR,
                    session VARCHAR
                )
            """)
            mem_client.execute("""
                INSERT INTO tick_data VALUES
                ('2026-09-25 14:30:05', 'AAPL', 150.5, 10.0, 150.4, 150.6, 'CAPITAL', 'REG'),
                ('2026-09-25 14:30:25', 'AAPL', 151.0, 15.0, 150.9, 151.1, 'CAPITAL', 'REG')
            """)

            mem_client.close = MagicMock()
            with patch("src.dashboard.analytics.get_streaming_db_connection", return_value=mem_client):
                res = get_stream_tape("AAPL", limit=10)
                ticks = res.get("ticks", [])
                assert len(ticks) == 2
                assert ticks[0]["symbol"] == "AAPL"
                assert ticks[0]["price"] == 151.0  # newest first
                assert ticks[0]["spread"] == 0.2
        finally:
            if mem_client.conn:
                try:
                    mem_client.conn.close()
                except Exception:
                    pass

    def test_historical_endpoints_strictly_query_historical_duckdb(self):
        """Historical operations must strictly query historical.duckdb and never touch streaming.duckdb."""
        def forbidden_streaming_call(*args, **kwargs):
            raise AssertionError("VIOLATION: Historical operation touched streaming.duckdb connection!")

        with patch("src.dashboard.analytics.get_streaming_db_connection", side_effect=forbidden_streaming_call):
            res = get_historical_candles("SPY", timeframe="1m", limit=5)
            assert res.get("database") == "historical"

    def test_streaming_endpoints_strictly_query_streaming_duckdb(self):
        """Streaming operations must strictly query streaming.duckdb and never touch historical.duckdb."""
        def forbidden_historical_call(*args, **kwargs):
            raise AssertionError("VIOLATION: Streaming operation touched historical.duckdb connection!")

        with patch("src.dashboard.analytics.get_historical_db_connection", side_effect=forbidden_historical_call):
            res_candles = get_streaming_candles("SPY", timeframe="1m", limit=5)
            assert res_candles.get("database") == "streaming"

            res_tape = get_stream_tape("SPY", limit=5)
            assert isinstance(res_tape.get("ticks"), list)


# ============================================================================
# 2. Dashboard HTML & UI Structure Segregation
# ============================================================================

class TestDashboardHTMLStructureSegregation:
    """
    Tests index.html for clean segregation into:
    - Application sidebar navigation element (<aside id="sidebar"> or <nav id="sidebar">)
    - Distinct navigation items for Historical Dashboard and Streaming Dashboard
    - Distinct dedicated container sections for Historical and Streaming dashboards
    - Elimination of cross-mode chart toggling (no inline stream toggle in historical chart,
      no inline historical toggle in streaming chart)
    """

    def test_sidebar_navigation_element_exists(self, html_soup):
        """An application sidebar navigation element (<aside id='sidebar'> or <nav id='sidebar'>) must exist."""
        sidebar = html_soup.select_one("aside#sidebar, nav#sidebar, #sidebar")
        assert sidebar is not None, (
            "Application sidebar navigation element (<aside id='sidebar'> or <nav id='sidebar'>) "
            "must exist in src/dashboard/static/index.html to provide two distinct dashboards."
        )

    def test_sidebar_contains_historical_and_streaming_nav_items(self, html_soup):
        """Sidebar must contain distinct navigation items for Historical and Streaming dashboards."""
        sidebar = html_soup.select_one("aside#sidebar, nav#sidebar, #sidebar")
        assert sidebar is not None, "Sidebar must exist to test navigation items"

        # Check for historical nav item
        hist_nav = (
            sidebar.select_one("#nav-historical, #sidebar-nav-historical, [data-view='historical']")
            or sidebar.find(lambda el: el.name in ("button", "a", "li", "div") and "historical" in el.get_text().lower())
        )
        assert hist_nav is not None, (
            "Sidebar must contain a distinct navigation item for 'Historical Dashboard' "
            "(e.g. #nav-historical or text 'Historical Dashboard')."
        )

        # Check for streaming nav item
        strm_nav = (
            sidebar.select_one("#nav-streaming, #sidebar-nav-streaming, [data-view='streaming']")
            or sidebar.find(lambda el: el.name in ("button", "a", "li", "div") and "streaming" in el.get_text().lower())
        )
        assert strm_nav is not None, (
            "Sidebar must contain a distinct navigation item for 'Streaming Dashboard' "
            "(e.g. #nav-streaming or text 'Streaming Dashboard')."
        )

    def test_distinct_dedicated_container_sections_exist(self, html_soup):
        """Distinct dedicated container sections must exist for Historical and Streaming dashboards."""
        hist_view = html_soup.select_one("#view-historical, #historical-dashboard, #section-historical")
        assert hist_view is not None, (
            "A dedicated container section for Historical Dashboard (#view-historical or #historical-dashboard) "
            "must exist in index.html."
        )

        strm_view = html_soup.select_one("#view-streaming, #streaming-dashboard, #section-streaming")
        assert strm_view is not None, (
            "A dedicated container section for Streaming Dashboard (#view-streaming or #streaming-dashboard) "
            "must exist in index.html."
        )

        assert hist_view != strm_view, "Historical and Streaming containers must be distinct separate elements"

    def test_historical_container_contains_required_historical_components(self, html_soup):
        """Historical Dashboard container must contain historical chart, symbol matrix/coverage, and harvester runner."""
        hist_view = html_soup.select_one("#view-historical, #historical-dashboard, #section-historical")
        assert hist_view is not None, "Historical container must exist"

        # 1. Historical chart container
        chart_el = hist_view.select_one("#tv-chart-container, #historical-chart-container, #tv-historical-chart-container")
        assert chart_el is not None, (
            "Historical Dashboard container must contain a dedicated historical chart container "
            "(e.g. #tv-chart-container or #historical-chart-container)."
        )

        # 2. Historical symbol coverage matrix
        symbol_matrix = hist_view.select_one("#symbols-matrix-body, #symbol-search, #tab-symbols, #historical-symbols-matrix")
        assert symbol_matrix is not None, (
            "Historical Dashboard container must contain historical symbol coverage/matrix "
            "(e.g. #symbols-matrix-body or #historical-symbols-matrix)."
        )

        # 3. REST Harvester runner controls
        harvester = hist_view.select_one("#harvest-run-btn, #terminal-logs, #tab-harvester, #harvester-runner-card")
        assert harvester is not None, (
            "Historical Dashboard container must contain REST harvester runner controls "
            "(e.g. #harvest-run-btn or #terminal-logs)."
        )

    def test_streaming_container_contains_required_streaming_components(self, html_soup):
        """Streaming Dashboard container must contain streaming chart, live tape, streamer daemon status, and streaming symbols."""
        strm_view = html_soup.select_one("#view-streaming, #streaming-dashboard, #section-streaming")
        assert strm_view is not None, "Streaming container must exist"

        # 1. Dedicated live streaming chart container
        strm_chart = strm_view.select_one("#streaming-chart-container, #tv-streaming-chart-container, #stream-chart-container")
        assert strm_chart is not None, (
            "Streaming Dashboard container must contain a dedicated streaming chart container "
            "(e.g. #streaming-chart-container or #tv-streaming-chart-container)."
        )

        # 2. Live tick tape
        tape_el = strm_view.select_one("#stream-ticker-grid, #tape-table-body, #streaming-ticker-tape")
        assert tape_el is not None, (
            "Streaming Dashboard container must contain live tick tape (#stream-ticker-grid or #tape-table-body)."
        )

        # 3. Streamer daemon controls / status
        daemon_el = strm_view.select_one("#stream-daemon-pill, #stream-status-badge, button[onclick*='triggerStreamReload']")
        assert daemon_el is not None, (
            "Streaming Dashboard container must contain streamer daemon controls/status."
        )

        # 4. Streaming symbols section / selector
        strm_sym = strm_view.select_one("#streaming-symbol-select, #streaming-symbols-list, #streaming-symbols-matrix, #streaming-symbol-search")
        assert strm_sym is not None, (
            "Streaming Dashboard container must contain streaming symbols section or selector "
            "(e.g. #streaming-symbol-select or #streaming-symbols-list)."
        )

    def test_historical_chart_strictly_historical_no_inline_stream_toggle(self, html_soup):
        """
        The chart in Historical Dashboard is dedicated strictly to historical data.
        Must NOT contain an inline toggle/button to switch the chart to streaming mode.
        """
        hist_view = html_soup.select_one("#view-historical, #historical-dashboard, #section-historical")
        target_area = hist_view if hist_view else html_soup

        # Check for inline button or toggle switching to streaming
        stream_btn = target_area.select_one("#src-streaming, button[onclick*=\"setDbSource('streaming')\"], button[onclick*='setDbSource(\"streaming\")']")
        assert stream_btn is None, (
            "VIOLATION: Historical Dashboard chart must NOT have an inline toggle button switching to streaming mode "
            "(e.g. #src-streaming or setDbSource('streaming')). "
            "The historical chart must be dedicated exclusively to historical data."
        )

    def test_streaming_chart_strictly_streaming_no_inline_historical_toggle(self, html_soup):
        """
        The chart in Streaming Dashboard is dedicated strictly to streaming data.
        Must NOT contain an inline toggle/button to switch the chart to historical mode.
        """
        strm_view = html_soup.select_one("#view-streaming, #streaming-dashboard, #section-streaming")
        if strm_view is None:
            pytest.fail("Streaming container #view-streaming does not exist yet.")

        # Check for inline button or toggle switching to historical
        hist_btn = strm_view.select_one("#src-historical, button[onclick*=\"setDbSource('historical')\"], button[onclick*='setDbSource(\"historical\")']")
        assert hist_btn is None, (
            "VIOLATION: Streaming Dashboard chart must NOT have an inline toggle button switching to historical mode "
            "(e.g. #src-historical or setDbSource('historical')). "
            "The streaming chart must be dedicated exclusively to streaming data."
        )


# ============================================================================
# 3. Frontend JS Logic Segregation
# ============================================================================

class TestFrontendJSLogicSegregation:
    """
    Tests frontend JavaScript modules in src/dashboard/static/js/ to verify:
    - Clean separation of historical and streaming chart controllers without shared mutable mode switching.
    - Historical symbol selector only targets historical symbols (/api/historical/symbols or /api/symbols?source=historical).
    - Streaming symbol selector only targets streaming symbols (/api/streaming/symbols or /api/symbols?source=streaming).
    """

    def test_chart_controllers_segregated_without_shared_mutable_mode(self, js_sources):
        """
        Historical and streaming chart controllers must be cleanly separated without
        a shared mutable mode switch (e.g. setDbSource mutating a single chart back and forth).
        """
        combined_js = "\n".join(js_sources.values())

        # Verify presence of distinct historical and streaming chart controller logic
        has_historical_chart = bool(re.search(r"\b(initHistoricalChart|loadHistoricalChart|HistoricalChart)\b", combined_js))
        has_streaming_chart = bool(re.search(r"\b(initStreamingChart|loadStreamingChart|StreamingChart)\b", combined_js))

        assert has_historical_chart, (
            "Frontend JS must define a dedicated historical chart controller function or class "
            "(e.g. initHistoricalChart or loadHistoricalChart)."
        )
        assert has_streaming_chart, (
            "Frontend JS must define a dedicated streaming chart controller function or class "
            "(e.g. initStreamingChart or loadStreamingChart)."
        )

        # Verify elimination of shared mutable mode switching on a single chart
        has_shared_set_db_source = bool(re.search(r"function\s+setDbSource\s*\(", combined_js))
        assert not has_shared_set_db_source, (
            "VIOLATION: Found 'function setDbSource()' in frontend JS. "
            "Charts must be cleanly separated into dedicated historical and streaming chart controllers "
            "without shared mutable mode switching."
        )

    def test_historical_symbol_selector_targets_historical_endpoints(self, js_sources):
        """Historical symbol selector must target historical endpoints only."""
        combined_js = "\n".join(js_sources.values())

        # Must fetch from historical symbols endpoint
        has_historical_fetch = bool(
            re.search(r"(/api/historical/symbols|/api/symbols\?source=historical|/api/symbols/coverage)", combined_js)
        )
        assert has_historical_fetch, (
            "Frontend JS must fetch historical symbols using /api/historical/symbols, "
            "/api/symbols?source=historical, or /api/symbols/coverage."
        )

    def test_streaming_symbol_selector_targets_streaming_endpoints(self, js_sources):
        """Streaming symbol selector must target streaming endpoints only."""
        combined_js = "\n".join(js_sources.values())

        # Must fetch from streaming symbols endpoint
        has_streaming_fetch = bool(
            re.search(r"(/api/streaming/symbols|/api/symbols\?source=streaming)", combined_js)
        )
        assert has_streaming_fetch, (
            "Frontend JS must fetch streaming symbols using /api/streaming/symbols "
            "or /api/symbols?source=streaming for the Streaming Dashboard."
        )


# ============================================================================
# 4. End-to-End REST API Verification
# ============================================================================

class TestE2ERESTAPISegregation:
    """Verifies that dedicated REST API endpoints properly serve minute_data and tick_data."""

    def test_e2e_historical_candles_endpoint_with_minute_data(self, api_test_server):
        """GET /api/historical/candles returns candles exclusively from historical.duckdb (minute_data)."""
        resp = requests.get(f"{api_test_server}/api/historical/candles?symbol=SPY&tf=1m&limit=5")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data.get("database") == "historical"
        assert data.get("symbol") == "SPY"
        assert data.get("timeframe") == "1m"
        assert "candles" in data
        assert isinstance(data["candles"], list)

        if len(data["candles"]) > 0:
            c = data["candles"][0]
            assert "open" in c
            assert "high" in c
            assert "low" in c
            assert "close" in c
            assert "time" in c
            assert "time_str" in c

    def test_e2e_streaming_candles_endpoint_with_tick_data(self, api_test_server):
        """GET /api/streaming/candles returns candles resampled from streaming.duckdb (tick_data)."""
        resp = requests.get(f"{api_test_server}/api/streaming/candles?symbol=SPY&tf=1m&limit=5")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data.get("database") == "streaming"
        assert data.get("symbol") == "SPY"
        assert data.get("timeframe") == "1m"
        assert "candles" in data
        assert isinstance(data["candles"], list)

    def test_e2e_candles_endpoint_source_parameter_enforcement(self, api_test_server):
        """GET /api/candles requires explicit source param and rejects requests without source."""
        # 1. Missing source -> 400 Bad Request
        resp_missing = requests.get(f"{api_test_server}/api/candles?symbol=SPY")
        assert resp_missing.status_code == 400, (
            f"Expected HTTP 400 when 'source' query param is omitted, got {resp_missing.status_code}."
        )

        # 2. Explicit source=historical -> 200
        resp_hist = requests.get(f"{api_test_server}/api/candles?symbol=SPY&source=historical&tf=1m&limit=5")
        assert resp_hist.status_code == 200
        assert resp_hist.json().get("database") == "historical"

        # 3. Explicit source=streaming -> 200
        resp_strm = requests.get(f"{api_test_server}/api/candles?symbol=SPY&source=streaming&tf=1m&limit=5")
        assert resp_strm.status_code == 200
        assert resp_strm.json().get("database") == "streaming"

    def test_e2e_symbols_endpoints_segregation(self, api_test_server):
        """GET /api/historical/symbols and GET /api/streaming/symbols strictly isolate symbol inventories."""
        # Historical symbols
        h_resp = requests.get(f"{api_test_server}/api/historical/symbols")
        assert h_resp.status_code == 200
        assert h_resp.json().get("database") == "historical"
        assert isinstance(h_resp.json().get("symbols"), list)

        # Streaming symbols
        s_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
        assert s_resp.status_code == 200
        assert s_resp.json().get("database") == "streaming"
        assert isinstance(s_resp.json().get("symbols"), list)

        # General /api/symbols requires explicit source param
        gen_resp = requests.get(f"{api_test_server}/api/symbols")
        assert gen_resp.status_code == 400, "GET /api/symbols without source must return HTTP 400"

    def test_e2e_historical_candles_never_accesses_streaming_db(self):
        """Direct query for historical candles must never invoke streaming DB connection."""
        def forbidden_streaming_call(*args, **kwargs):
            raise AssertionError("VIOLATION: Historical operation touched streaming.duckdb connection!")

        with patch("src.dashboard.analytics.get_streaming_db_connection", side_effect=forbidden_streaming_call):
            res = get_historical_candles("SPY", timeframe="1m", limit=5)
            assert res.get("database") == "historical"

    def test_e2e_streaming_candles_never_accesses_historical_db(self):
        """Direct query for streaming candles must never invoke historical DB connection."""
        def forbidden_historical_call(*args, **kwargs):
            raise AssertionError("VIOLATION: Streaming operation touched historical.duckdb connection!")

        with patch("src.dashboard.analytics.get_historical_db_connection", side_effect=forbidden_historical_call):
            res = get_streaming_candles("SPY", timeframe="1m", limit=5)
            assert res.get("database") == "streaming"
