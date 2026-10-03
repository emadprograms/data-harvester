"""
Automated Test Suite for Streaming Chart & Continuity Redesign.

Requirements Covered:
1. Backend Analytics (src/dashboard/analytics.py):
   - test_continuity_extended_hours_window:
     When include_extended=True, window spans 04:00 to 20:00 ET (960 minutes per session day).
     Overnights (20:00 to 04:00 ET) and weekends do not trigger false gaps.
     Ticks outside 09:30-16:00 (pre-market 07:00, after-hours 18:00) are recognized in buckets.
   - test_continuity_epoch_contract:
     Gaps and minute buckets include exact start_epoch and end_epoch (UTC epoch seconds).
   - test_get_streaming_candles_full_week_limit:
     Cleanly returns up to 10,000 candles without error or truncation.

2. REST API (src/dashboard/server.py):
   - test_api_continuity_extended_param:
     GET /api/streaming/continuity?symbol=NVDA&extended=true returns HTTP 200 with
     "extended_hours": True (or "hours": "extended").

3. Dashboard HTML Structure (src/dashboard/static/index.html):
   - test_stream_tab_chart_structure:
     #streaming-spectrum-view exists and is visible by default.
     #streaming-detail-view exists and is hidden by default.
     #streaming-chart-card exists and is hidden by default in spectrum view.
     Back button exists inside #streaming-detail-view.
   - test_backward_compatibility_elements_preserved:
     #streaming-symbol-select and #streaming-chart-container remain present in #view-streaming.

4. Frontend JS Logic Simulation (src/dashboard/static/js/):
   - test_sync_chart_to_gap_calls_set_visible_range:
     Node.js mock test asserting syncChartToGap calls setVisibleRange on window.tvStreamingChart.
   - test_open_and_close_symbol_detail_state:
     openSymbolDetail('NVDA') updates symbol, switches views; closeSymbolDetail() restores spectrum view.
   - test_chart_fetches_with_large_limit:
     loadStreamingChart() uses currentStreamingLimit = 10000 (or >= 5000) for candles fetch.
"""
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from datetime import datetime, date, time as dtime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

import pytest
import requests
from bs4 import BeautifulSoup

from src.database.connection import DuckDBClient
from src.database.schema import init_streaming_db
from src.dashboard.server import create_dashboard_server

try:
    from src.dashboard.analytics import get_streaming_continuity_analysis, get_streaming_candles
except (ImportError, AttributeError):
    get_streaming_continuity_analysis = None
    get_streaming_candles = None


ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

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


def populate_extended_session_ticks(
    client: DuckDBClient,
    session_date: date,
    symbol: str = "NVDA",
    include_pre_market: bool = True,
    include_regular: bool = True,
    include_after_hours: bool = True,
    include_overnight: bool = True,
    base_price: float = 120.0,
):
    """
    Populates ticks across pre-market (04:00-09:29 ET), regular (09:30-16:00 ET),
    after-hours (16:01-20:00 ET), and overnight (20:01-03:59 ET) for session_date.
    Timestamps are converted from America/New_York to UTC for storage.
    """
    rows = []

    def add_tick(hour: int, minute: int, session_tag: str = "REG"):
        dt_et = datetime(session_date.year, session_date.month, session_date.day, hour, minute, tzinfo=ET)
        dt_utc = dt_et.astimezone(UTC)
        ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
        rows.append((ts_str, symbol, base_price, 10.0, base_price - 0.05, base_price + 0.05, "TEST", session_tag))

    # Pre-market ticks: 04:00, 07:00, 08:30 ET
    if include_pre_market:
        add_tick(4, 0, "PRE")
        add_tick(7, 0, "PRE")
        add_tick(8, 30, "PRE")

    # Regular market ticks: 09:30 to 16:00 ET
    if include_regular:
        curr = datetime(session_date.year, session_date.month, session_date.day, 9, 30, tzinfo=ET)
        end = datetime(session_date.year, session_date.month, session_date.day, 16, 0, tzinfo=ET)
        while curr <= end:
            dt_utc = curr.astimezone(UTC)
            rows.append((dt_utc.strftime("%Y-%m-%d %H:%M:%S"), symbol, base_price, 10.0, base_price - 0.05, base_price + 0.05, "TEST", "REG"))
            curr += timedelta(minutes=1)

    # After-hours ticks: 16:30, 18:00, 19:59 ET
    if include_after_hours:
        add_tick(16, 30, "POST")
        add_tick(18, 0, "POST")
        add_tick(19, 59, "POST")

    # Overnight ticks (outside 04:00-20:00 ET): 21:30 ET and 02:30 ET
    if include_overnight:
        add_tick(21, 30, "OVERNIGHT")
        add_tick(2, 30, "OVERNIGHT")

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


def run_streaming_redesign_js_simulation(test_body_js: str, extra_setup_js: str = "") -> dict:
    """
    Executes a Node.js simulation of frontend continuity and chart logic,
    evaluating src/dashboard/static/js/continuity.js and src/dashboard/static/js/chart.js.
    """
    node_bin = shutil.which("node")
    if not node_bin:
        pytest.skip("Node.js is required to execute frontend JS simulation tests")

    js_dir = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "js"
    continuity_js_path = js_dir / "continuity.js"
    chart_js_path = js_dir / "chart.js"

    continuity_js = continuity_js_path.read_text(encoding="utf-8") if continuity_js_path.exists() else ""
    chart_js = chart_js_path.read_text(encoding="utf-8") if chart_js_path.exists() else ""

    script = f"""
const elements = {{}};
function mockElement(id, initialClasses = []) {{
  const classes = new Set(initialClasses);
  return {{
    id,
    className: initialClasses.join(' '),
    innerHTML: '',
    style: {{}},
    value: '',
    classList: {{
      add: (c) => {{ classes.add(c); }},
      remove: (c) => {{ classes.delete(c); }},
      contains: (c) => classes.has(c),
      toggle: (c, force) => {{
        if (force === undefined) {{
          classes.has(c) ? classes.delete(c) : classes.add(c);
        }} else if (force) {{
          classes.add(c);
        }} else {{
          classes.delete(c);
        }}
      }}
    }},
    querySelectorAll: (sel) => [],
    querySelector: (sel) => null
  }};
}}

// Initialize expected redesign DOM nodes
elements['streaming-spectrum-view'] = mockElement('streaming-spectrum-view');
elements['streaming-detail-view'] = mockElement('streaming-detail-view', ['hidden']);
elements['streaming-chart-card'] = mockElement('streaming-chart-card', ['hidden']);
elements['streaming-chart-container'] = mockElement('streaming-chart-container');
elements['streaming-symbol-select'] = mockElement('streaming-symbol-select');
elements['streaming-chart-limit-select'] = mockElement('streaming-chart-limit-select');
elements['streaming-chart-limit-select'].value = '500';
elements['continuity-ribbon-view'] = mockElement('continuity-ribbon-view');
elements['continuity-incident-summary'] = mockElement('continuity-incident-summary');

global.document = {{
  getElementById: (id) => elements[id] || (elements[id] = mockElement(id)),
  querySelectorAll: (sel) => [],
  querySelector: (sel) => null
}};

global.window = global;
global.window.addEventListener = () => {{}};
global.fetchCalls = [];
global.visibleRangeCalls = [];
global.fitContentCalls = [];
global.API_BASE = '';
global.currentStreamingSymbol = 'NVDA';
global.currentStreamingTimeframe = '1m';
global.currentStreamingLimit = 500;
global.showToast = () => {{}};

global.LightweightCharts = {{
  createChart: (container, options) => {{
    const chart = {{
      container,
      options,
      timeScale: () => ({{
        setVisibleRange: (range) => {{
          global.visibleRangeCalls.push(range);
        }},
        fitContent: () => {{
          global.fitContentCalls.push(true);
        }}
      }}),
      applyOptions: () => {{}},
      addCandlestickSeries: () => ({{ setData: () => {{}} }}),
      addHistogramSeries: () => ({{ setData: () => {{}} }}),
      subscribeCrosshairMove: () => {{}}
    }};
    return chart;
  }}
}};

global.fetch = async (url) => {{
  global.fetchCalls.push(url);
  if (url.includes('/api/streaming/candles')) {{
    return {{ ok: true, json: async () => ({{ candles: [] }}) }};
  }}
  return {{
    ok: true,
    json: async () => ({{
      database: 'streaming',
      view_mode: 'all',
      symbol: 'all',
      monitored_symbols_count: 19,
      days: [],
      summary: {{ total_gaps: 0, total_outage_minutes: 0, average_coverage: 100 }},
      spectrogram: {{}}
    }})
  }};
}};

{extra_setup_js}

{continuity_js}

{chart_js}

(async () => {{
  {test_body_js}
}})().then(res => {{
  console.log(JSON.stringify(res));
}}).catch(err => {{
  console.error(err);
  process.exit(1);
}});
"""
    res = subprocess.run([node_bin, "-e", script], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"JS simulation failed (code {res.returncode}):\n{res.stderr.strip()}")
    try:
        return json.loads(res.stdout.strip())
    except json.JSONDecodeError:
        raise RuntimeError(f"Failed to parse JS simulation JSON output:\n{res.stdout}")


# ============================================================================
# 1. Backend Analytics (src/dashboard/analytics.py)
# ============================================================================

class TestBackendAnalyticsRedesign:
    """Validates backend analytics extensions for streaming continuity & chart redesign."""

    def test_continuity_extended_hours_window(self):
        """
        When calling get_streaming_continuity_analysis(days=5, symbol="NVDA", include_extended=True):
        - The daily window spans 04:00 to 20:00 ET (960 minutes per session day).
        - Overnights (20:00 to 04:00 ET) and weekends do not trigger false gaps.
        - Ticks outside 09:30-16:00 (e.g. pre-market 07:00, after-hours 18:00) are recognized in the extended continuity buckets.
        - Calling with include_extended=False (default) preserves the 390-minute regular session window (09:30 to 16:00 ET).
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            # Seed Wednesday 2026-09-23 with extended hours ticks
            test_date = date(2026, 9, 23)
            populate_extended_session_ticks(
                mem_client,
                session_date=test_date,
                symbol="NVDA",
                include_pre_market=True,
                include_regular=True,
                include_after_hours=True,
                include_overnight=True
            )

            # Insert a weekend tick (Saturday 2026-09-26 12:00 ET) to verify weekends do not produce gaps
            sat_utc = datetime(2026, 9, 26, 12, 0, tzinfo=ET).astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
            mem_client.execute("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                               [sat_utc, "NVDA", 120.0, 5.0, 119.9, 120.1, "TEST", "WEEKEND"])

            # 1. Query WITH include_extended=True
            res_ext = get_streaming_continuity_analysis(days=1, symbol="NVDA", client=mem_client, include_extended=True)

            assert isinstance(res_ext, dict), "Result must be a dict"
            assert "days" in res_ext and len(res_ext["days"]) > 0, "Result must contain at least 1 day"
            day_obj = res_ext["days"][0]

            buckets = day_obj.get("buckets", [])
            # Extended window is 04:00 to 20:00 ET = 16 hours * 60 minutes = 960 minutes
            assert len(buckets) == 960, (
                f"Expected 960 minute buckets per session day for extended hours (04:00-20:00 ET), got {len(buckets)}"
            )

            # Check boundary times
            assert buckets[0].get("time") == "04:00", f"First bucket in extended hours must be 04:00, got {buckets[0].get('time')}"
            assert buckets[-1].get("time") == "19:59", f"Last bucket in extended hours must be 19:59, got {buckets[-1].get('time')}"

            # Ticks outside 09:30-16:00 (pre-market 07:00, after-hours 18:00) must be recognized in buckets
            bucket_0700 = next((b for b in buckets if b.get("time") == "07:00"), None)
            assert bucket_0700 is not None, "Bucket for 07:00 ET must exist in extended hours"
            assert bucket_0700.get("active_count", 0) > 0, "Pre-market tick at 07:00 ET must be recognized as active in bucket"
            assert bucket_0700.get("status") in ["healthy", "active"], f"07:00 bucket status should be healthy, got {bucket_0700.get('status')}"

            bucket_1800 = next((b for b in buckets if b.get("time") == "18:00"), None)
            assert bucket_1800 is not None, "Bucket for 18:00 ET must exist in extended hours"
            assert bucket_1800.get("active_count", 0) > 0, "After-hours tick at 18:00 ET must be recognized as active in bucket"
            assert bucket_1800.get("status") in ["healthy", "active"], f"18:00 bucket status should be healthy, got {bucket_1800.get('status')}"

            # Overnights (20:00 to 04:00 ET) and weekends must NOT trigger false gaps
            all_gaps = day_obj.get("gaps", []) + res_ext.get("summary", {}).get("gaps", [])
            for g in all_gaps:
                g_start_str = g.get("start_str", g.get("start_time", ""))
                g_end_str = g.get("end_str", g.get("end_time", ""))
                # If timestamp is full datetime, extract time portion
                if " " in g_start_str:
                    g_start_str = g_start_str.split(" ")[1][:5]
                if " " in g_end_str:
                    g_end_str = g_end_str.split(" ")[1][:5]

                # Assert gap does not represent overnight period 20:00 to 04:00
                assert not ("20:00" <= g_start_str or g_end_str <= "04:00"), (
                    f"False overnight gap detected between 20:00 and 04:00 ET: {g}"
                )

            # 2. Query WITHOUT include_extended (default regular hours)
            res_reg = get_streaming_continuity_analysis(days=1, symbol="NVDA", client=mem_client, include_extended=False)
            day_reg = res_reg["days"][0]
            reg_buckets = day_reg.get("buckets", [])
            assert len(reg_buckets) == 390, (
                f"Default regular session window must have 390 minute buckets (09:30-16:00 ET), got {len(reg_buckets)}"
            )
            assert reg_buckets[0].get("time") == "09:30"
            assert reg_buckets[-1].get("time") == "15:59"
        finally:
            mem_client.close()

    def test_continuity_epoch_contract(self):
        """
        Asserts that gaps and buckets include exact start_epoch and end_epoch (true UTC epoch seconds)
        to eliminate timezone parsing mismatches in the frontend:
        - Each bucket has start_epoch and end_epoch (with end_epoch - start_epoch == 60).
        - Each gap has start_epoch and end_epoch (with end_epoch - start_epoch == duration * 60).
        """
        assert callable(get_streaming_continuity_analysis), (
            "get_streaming_continuity_analysis must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            # Seed Wednesday 2026-09-23 with regular session ticks, except a 15m gap from 10:00 to 10:15 ET
            test_date = date(2026, 9, 23)
            # Regular session is 09:30 to 16:00 ET
            curr = datetime(test_date.year, test_date.month, test_date.day, 9, 30, tzinfo=ET)
            end = datetime(test_date.year, test_date.month, test_date.day, 16, 0, tzinfo=ET)
            rows = []
            while curr <= end:
                curr_time = curr.time()
                # 15m blackout: 10:00 to 10:15 ET
                if not (dtime(10, 0) <= curr_time < dtime(10, 15)):
                    ts_utc = curr.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append((ts_utc, "NVDA", 120.0, 10.0, 119.9, 120.1, "TEST", "REG"))
                curr += timedelta(minutes=1)

            mem_client.executemany("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

            res = get_streaming_continuity_analysis(days=1, symbol="NVDA", client=mem_client)
            assert len(res.get("days", [])) > 0, "Expected at least 1 day in analysis result"
            day = res["days"][0]

            buckets = day.get("buckets", [])
            assert len(buckets) > 0, "Day must contain minute buckets"

            # Check epoch contract on buckets
            first_bucket = buckets[0]
            assert "start_epoch" in first_bucket, "Bucket must contain 'start_epoch' (UTC epoch seconds)"
            assert "end_epoch" in first_bucket, "Bucket must contain 'end_epoch' (UTC epoch seconds)"
            assert isinstance(first_bucket["start_epoch"], int), "start_epoch must be an integer"
            assert isinstance(first_bucket["end_epoch"], int), "end_epoch must be an integer"
            assert first_bucket["end_epoch"] - first_bucket["start_epoch"] == 60, (
                f"1-minute bucket end_epoch - start_epoch must equal 60, got {first_bucket['end_epoch'] - first_bucket['start_epoch']}"
            )

            # On 2026-09-23, 09:30 ET is 13:30 UTC -> epoch 1790170200
            expected_0930_epoch = int(datetime(2026, 9, 23, 13, 30, tzinfo=timezone.utc).timestamp())
            assert first_bucket["start_epoch"] == expected_0930_epoch, (
                f"Expected start_epoch {expected_0930_epoch} for 09:30 ET on 2026-09-23, got {first_bucket['start_epoch']}"
            )

            # Check epoch contract on gaps
            gaps = day.get("gaps", [])
            assert len(gaps) > 0, "Expected at least 1 gap detected for the 10:00-10:15 outage"
            gap_1000 = next((g for g in gaps if g.get("duration", 0) == 15 or "10:00" in str(g)), gaps[0])

            assert "start_epoch" in gap_1000, "Gap object must contain 'start_epoch' (UTC epoch seconds)"
            assert "end_epoch" in gap_1000, "Gap object must contain 'end_epoch' (UTC epoch seconds)"
            assert isinstance(gap_1000["start_epoch"], int), "Gap start_epoch must be an integer"
            assert isinstance(gap_1000["end_epoch"], int), "Gap end_epoch must be an integer"
            assert gap_1000["end_epoch"] > gap_1000["start_epoch"], "Gap end_epoch must be greater than start_epoch"

            duration_mins = gap_1000.get("duration", gap_1000.get("duration_minutes", 15))
            expected_diff_sec = duration_mins * 60
            assert (gap_1000["end_epoch"] - gap_1000["start_epoch"]) == expected_diff_sec, (
                f"Gap duration seconds mismatch: end_epoch - start_epoch = {gap_1000['end_epoch'] - gap_1000['start_epoch']}, expected {expected_diff_sec}"
            )

            # 10:00 ET on 2026-09-23 is 14:00 UTC -> epoch 1790172000
            expected_gap_start = int(datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc).timestamp())
            assert gap_1000["start_epoch"] == expected_gap_start, (
                f"Expected gap start_epoch {expected_gap_start} for 10:00 ET on 2026-09-23, got {gap_1000['start_epoch']}"
            )
        finally:
            mem_client.close()

    def test_get_streaming_candles_full_week_limit(self):
        """
        Asserts get_streaming_candles cleanly returns up to 10,000 candles without error or truncation.
        Seeds 6,000 1-minute bars across an entire week into an in-memory streaming DuckDB,
        and requests limit=10000.
        """
        assert callable(get_streaming_candles), (
            "get_streaming_candles must be implemented in src.dashboard.analytics"
        )

        mem_client = create_in_memory_streaming_db()
        try:
            # Seed 6,000 continuous 1-minute ticks for NVDA
            base_start = datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc)
            rows = []
            for i in range(6000):
                tick_time = base_start + timedelta(minutes=i)
                ts_str = tick_time.strftime("%Y-%m-%d %H:%M:%S")
                rows.append((ts_str, "NVDA", 120.0 + (i * 0.01), 10.0, 119.9, 120.1, "TEST", "REG"))

            mem_client.executemany("INSERT INTO tick_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)

            # Patch get_streaming_db_connection to return mem_client without closing it
            mem_client.close = MagicMock()
            with patch("src.dashboard.analytics.get_streaming_db_connection", return_value=mem_client), \
                 patch("src.dashboard.analytics._get_lake_reader", return_value=None):
                res = get_streaming_candles(symbol="NVDA", timeframe="1m", limit=10000)

                assert res.get("error") is None, f"get_streaming_candles returned error: {res.get('error')}"
                assert res.get("symbol") == "NVDA"
                assert res.get("database") == "streaming"
                assert res.get("count") == 6000, (
                    f"Expected 6,000 candles without truncation, got count={res.get('count')}"
                )
                candles = res.get("candles", [])
                assert len(candles) == 6000, f"Expected 6,000 candles in list, got {len(candles)}"

                # Verify ascending chronological order
                assert candles[0]["time"] < candles[-1]["time"], "Candles must be sorted chronologically ascending"
        finally:
            # Restore close method and close client
            mem_client.close = DuckDBClient.close.__get__(mem_client, DuckDBClient)
            mem_client.close()


# ============================================================================
# 2. REST API (src/dashboard/server.py)
# ============================================================================

class TestRestApiRedesign:
    """Validates REST API extended hours parameter handling."""

    def test_api_continuity_extended_param(self, api_test_server):
        """
        GET /api/streaming/continuity?symbol=NVDA&extended=true returns HTTP 200 with
        "extended_hours": True (or "hours": "extended").
        Also checks that default/extended=false returns extended_hours: False (or hours: 'regular').
        """
        # 1. Query with extended=true
        url_extended = f"{api_test_server}/api/streaming/continuity?symbol=NVDA&extended=true"
        resp_ext = requests.get(url_extended, timeout=5)
        assert resp_ext.status_code == 200, f"Expected HTTP 200 for extended continuity query, got {resp_ext.status_code}"

        data_ext = resp_ext.json()
        assert isinstance(data_ext, dict), "Response must be a JSON dictionary"
        assert data_ext.get("database") == "streaming", "Database must be 'streaming'"
        assert data_ext.get("extended_hours") is True or data_ext.get("hours") == "extended", (
            f"Expected extended_hours: True or hours: 'extended' in response, got {data_ext}"
        )

        # 2. Query without extended (default) or extended=false
        url_regular = f"{api_test_server}/api/streaming/continuity?symbol=NVDA&extended=false"
        resp_reg = requests.get(url_regular, timeout=5)
        assert resp_reg.status_code == 200, f"Expected HTTP 200 for regular continuity query, got {resp_reg.status_code}"

        data_reg = resp_reg.json()
        assert data_reg.get("extended_hours") is False or data_reg.get("hours") in ["regular", "rth", None, False], (
            f"Expected regular hours indicator for extended=false, got {data_reg}"
        )


# ============================================================================
# 3. Dashboard HTML Structure (src/dashboard/static/index.html)
# ============================================================================

class TestHtmlStructureRedesign:
    """Validates dashboard DOM structure for spectrum vs detail view and backward compatibility."""

    def test_stream_tab_chart_structure(self, html_soup):
        """
        Validates the redesigned HTML container hierarchy inside #stream-tab-chart:
        - #streaming-spectrum-view exists and is the default view container (visible by default).
        - #streaming-detail-view exists and is the symbol drill-down container (hidden by default).
        - #streaming-chart-card exists and is hidden by default in the spectrum view (either via .hidden or inside detail view).
        - Back button exists inside #streaming-detail-view (e.g. #back-to-spectrum-btn or button calling closeSymbolDetail()).
        """
        stream_tab = html_soup.select_one("#stream-tab-chart")
        assert stream_tab is not None, "Container #stream-tab-chart must exist in index.html"

        # 1. #streaming-spectrum-view exists and is visible by default
        spectrum_view = stream_tab.select_one("#streaming-spectrum-view")
        assert spectrum_view is not None, "Missing #streaming-spectrum-view container inside #stream-tab-chart"
        spectrum_classes = spectrum_view.get("class", [])
        assert "hidden" not in spectrum_classes, "#streaming-spectrum-view must be visible by default (no 'hidden' class)"
        assert "display: none" not in spectrum_view.get("style", ""), "#streaming-spectrum-view must not have display:none inline style"

        # 2. #streaming-detail-view exists and is hidden by default
        detail_view = stream_tab.select_one("#streaming-detail-view")
        assert detail_view is not None, "Missing #streaming-detail-view container inside #stream-tab-chart"
        detail_classes = detail_view.get("class", [])
        detail_style = detail_view.get("style", "")
        assert ("hidden" in detail_classes) or ("display: none" in detail_style) or ("display:none" in detail_style), (
            "#streaming-detail-view must be hidden by default (via 'hidden' class or style 'display: none')"
        )

        # 3. #streaming-chart-card exists and is hidden by default in spectrum view
        chart_card = stream_tab.select_one("#streaming-chart-card")
        if chart_card is None:
            # Fallback: container wrapping #streaming-chart-container inside #streaming-detail-view
            chart_container = stream_tab.select_one("#streaming-chart-container")
            assert chart_container is not None, "Missing #streaming-chart-container"
            # It must be inside #streaming-detail-view (which is hidden) or have #streaming-chart-card
            parent_detail = chart_container.find_parent(id="streaming-detail-view")
            assert parent_detail is not None, (
                "Chart container must either be wrapped in #streaming-chart-card or placed inside #streaming-detail-view"
            )
        else:
            # If #streaming-chart-card exists at top level of #stream-tab-chart, it must be hidden by default
            parent_detail = chart_card.find_parent(id="streaming-detail-view")
            if parent_detail is None:
                card_classes = chart_card.get("class", [])
                assert "hidden" in card_classes or "display: none" in chart_card.get("style", ""), (
                    "#streaming-chart-card must be hidden by default in spectrum view"
                )

        # 4. Back button exists inside #streaming-detail-view
        back_btn = detail_view.select_one("#back-to-spectrum-btn, button[onclick*='closeSymbolDetail']")
        assert back_btn is not None, (
            "Back button (e.g. #back-to-spectrum-btn or button calling closeSymbolDetail()) must exist inside #streaming-detail-view"
        )

    def test_backward_compatibility_elements_preserved(self, html_soup):
        """
        Validates that required legacy/segregation elements remain in #view-streaming:
        - #streaming-symbol-select remains present (can be styled or moved inside detail view).
        - #streaming-chart-container remains present.
        Ensures existing tests in test_dashboard_segregation.py continue to pass without regression.
        """
        strm_view = html_soup.select_one("#view-streaming")
        assert strm_view is not None, "#view-streaming must exist in index.html"

        # Check #streaming-symbol-select
        sym_select = strm_view.select_one("#streaming-symbol-select")
        assert sym_select is not None, (
            "#streaming-symbol-select must remain present in #view-streaming for backward compatibility"
        )

        # Check #streaming-chart-container
        chart_container = strm_view.select_one("#streaming-chart-container")
        assert chart_container is not None, (
            "#streaming-chart-container must remain present in #view-streaming for backward compatibility"
        )


# ============================================================================
# 4. Frontend JS Logic Simulation (src/dashboard/static/js/)
# ============================================================================

class TestFrontendJsSimulationRedesign:
    """Validates frontend JS controller behavior for drill-down transitions and chart syncing."""

    def test_sync_chart_to_gap_calls_set_visible_range(self):
        """
        Node.js mock test asserting that syncChartToGap successfully calls setVisibleRange
        on window.tvStreamingChart (and does not fail due to missing window binding).
        Tests both numeric epoch and gap object signatures.
        """
        result = run_streaming_redesign_js_simulation("""
        let initError = null;
        try {
          if (typeof initStreamingChart === 'function') {
            initStreamingChart();
          } else if (typeof window.initStreamingChart === 'function') {
            window.initStreamingChart();
          }
        } catch (e) {
          initError = e.message;
        }

        const chartBound = Boolean(window.tvStreamingChart);

        // 2. Call syncChartToGap with epoch seconds
        if (typeof syncChartToGap === 'function') {
          syncChartToGap(1790172000);
        }
        const callsNumber = [...visibleRangeCalls];
        visibleRangeCalls.length = 0;

        // 3. Call syncChartToGap with gap object { start_epoch, end_epoch }
        if (typeof syncChartToGap === 'function') {
          syncChartToGap({ start_epoch: 1790172000, end_epoch: 1790172900 });
        }
        const callsObject = [...visibleRangeCalls];

        return {
          initError,
          chartBound,
          callsNumber,
          callsObject
        };
        """)

        assert result.get("initError") is None, (
            f"initStreamingChart threw an error (likely unbound chart variable): {result.get('initError')}"
        )
        assert result.get("chartBound") is True, (
            "window.tvStreamingChart must be bound to the chart instance on window by initStreamingChart"
        )
        assert len(result.get("callsNumber", [])) > 0, (
            "syncChartToGap(epochSec) must call setVisibleRange on window.tvStreamingChart"
        )
        range_num = result["callsNumber"][0]
        assert "from" in range_num and "to" in range_num, "setVisibleRange argument must contain 'from' and 'to'"
        assert range_num["from"] <= 1790172000 <= range_num["to"], (
            f"setVisibleRange range [{range_num['from']}, {range_num['to']}] must encompass target epoch 1790172000"
        )

        assert len(result.get("callsObject", [])) > 0, (
            "syncChartToGap(gapObj) must call setVisibleRange on window.tvStreamingChart"
        )
        range_obj = result["callsObject"][0]
        assert range_obj["from"] <= 1790172000 and range_obj["to"] >= 1790172900, (
            f"setVisibleRange range [{range_obj['from']}, {range_obj['to']}] must encompass gap bounds"
        )

    def test_open_and_close_symbol_detail_state(self):
        """
        Node.js mock test asserting:
        - openSymbolDetail('NVDA') sets currentStreamingSymbol = 'NVDA', unhides detail view and chart,
          hides spectrum view, and triggers loadStreamingChart().
        - closeSymbolDetail() returns to spectrum view (unhides spectrum view) and hides detail view and chart.
        """
        result = run_streaming_redesign_js_simulation("""
        if (typeof openSymbolDetail !== 'function' && typeof window.openSymbolDetail !== 'function') {
          return { error: 'openSymbolDetail is not defined on global or window' };
        }
        if (typeof closeSymbolDetail !== 'function' && typeof window.closeSymbolDetail !== 'function') {
          return { error: 'closeSymbolDetail is not defined on global or window' };
        }

        const fnOpen = typeof openSymbolDetail === 'function' ? openSymbolDetail : window.openSymbolDetail;
        const fnClose = typeof closeSymbolDetail === 'function' ? closeSymbolDetail : window.closeSymbolDetail;

        fetchCalls.length = 0;

        // 1. Open symbol detail for NVDA
        fnOpen('NVDA');

        const specEl = elements['streaming-spectrum-view'];
        const detailEl = elements['streaming-detail-view'];
        const chartCardEl = elements['streaming-chart-card'];

        const afterOpen = {
          symbol: global.currentStreamingSymbol,
          spectrumHidden: specEl ? specEl.classList.contains('hidden') : false,
          detailHidden: detailEl ? detailEl.classList.contains('hidden') : true,
          chartCardHidden: chartCardEl ? chartCardEl.classList.contains('hidden') : true,
          fetchCalls: [...fetchCalls]
        };

        // 2. Close symbol detail (return to spectrum)
        fnClose();

        const afterClose = {
          spectrumHidden: specEl ? specEl.classList.contains('hidden') : true,
          detailHidden: detailEl ? detailEl.classList.contains('hidden') : false,
          chartCardHidden: chartCardEl ? chartCardEl.classList.contains('hidden') : false
        };

        return { afterOpen, afterClose };
        """)

        assert "error" not in result, f"State transition function missing: {result.get('error')}"

        open_state = result["afterOpen"]
        assert open_state["symbol"] == "NVDA", f"Expected currentStreamingSymbol='NVDA', got {open_state['symbol']}"
        assert open_state["spectrumHidden"] is True, "openSymbolDetail must add 'hidden' to #streaming-spectrum-view"
        assert open_state["detailHidden"] is False, "openSymbolDetail must remove 'hidden' from #streaming-detail-view"
        assert open_state["chartCardHidden"] is False, "openSymbolDetail must make #streaming-chart-card visible"
        assert any("symbol=NVDA" in call for call in open_state["fetchCalls"]), (
            f"openSymbolDetail('NVDA') must trigger chart candle fetch for NVDA. Calls: {open_state['fetchCalls']}"
        )

        close_state = result["afterClose"]
        assert close_state["spectrumHidden"] is False, "closeSymbolDetail must unhide #streaming-spectrum-view"
        assert close_state["detailHidden"] is True, "closeSymbolDetail must hide #streaming-detail-view"
        assert close_state["chartCardHidden"] is True, "closeSymbolDetail must hide #streaming-chart-card"

    def test_chart_fetches_with_large_limit(self):
        """
        Asserts loadStreamingChart() uses currentStreamingLimit = 10000 (or >= 5000)
        so a full week of 1m bars is loaded into the chart.
        """
        result = run_streaming_redesign_js_simulation("""
        if (typeof loadStreamingChart !== 'function' && typeof window.loadStreamingChart !== 'function') {
          return { error: 'loadStreamingChart is not defined' };
        }
        const fnLoad = typeof loadStreamingChart === 'function' ? loadStreamingChart : window.loadStreamingChart;

        fetchCalls.length = 0;
        await fnLoad();

        return {
          currentStreamingLimit: global.currentStreamingLimit,
          fetchCalls: [...fetchCalls]
        };
        """)

        assert "error" not in result, f"loadStreamingChart failed: {result.get('error')}"

        calls = [c for c in result.get("fetchCalls", []) if "/api/streaming/candles" in c]
        assert len(calls) > 0, f"Expected fetch call to /api/streaming/candles, got: {result.get('fetchCalls')}"

        last_call = calls[-1]
        match = re.search(r"limit=(\d+)", last_call)
        assert match is not None, f"Expected limit parameter in candle fetch URL: {last_call}"

        limit_val = int(match.group(1))
        assert limit_val >= 5000, (
            f"Expected limit >= 5000 (standard 10000 for full week of 1m bars), got {limit_val} in URL: {last_call}"
        )
        assert result.get("currentStreamingLimit", 0) >= 5000, (
            f"Expected currentStreamingLimit >= 5000, got {result.get('currentStreamingLimit')}"
        )
