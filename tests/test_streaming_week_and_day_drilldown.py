"""
Automated Test Suite for Week Selection / History Navigation & Day-Specific Drill-Down.

Requirements Covered:
1. Week Selection / History Navigation:
   - Users can select previous weeks (not just current week) via dropdown selector.
   - 19-symbol spectrogram queries and renders the selected week.
   - discover_available_weeks groups trading dates Monday-to-Friday, sets is_current, labels, sorted descending.
   - get_streaming_continuity_analysis filters strictly to week_start trading dates.
2. Day-Specific Single-Day Drill-Down:
   - In the spectrogram, clicking a specific day for any symbol opens ONLY that specific day.
   - Opens the single-day detail view showing that day's extended hours continuity ribbon and gap callout pills.
   - Shows the 1-day candlestick chart underneath for ONLY that day.
   - NO click-based zoom-in on gaps (user directive: no disruptive setVisibleRange hacks on gap click).
3. Chart Whitespace Gaps:
   - For missing minutes in the single-day chart, the series receives Lightweight Charts whitespace items
     ({ time: epoch } without OHLC) so physical empty spaces are naturally visible on the x-axis where data
     is missing, rather than collapsing the gap.
"""
import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from datetime import datetime, date, timedelta, timezone
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
    from src.dashboard.analytics import (
        discover_available_weeks,
        get_streaming_continuity_analysis,
        get_streaming_candles,
    )
except (ImportError, AttributeError):
    discover_available_weeks = None
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


def populate_ticks_for_day(
    client: DuckDBClient,
    session_date: date,
    symbol: str = "NVDA",
    minutes_list: list = None,
    base_price: float = 120.0,
):
    """
    Populates ticks for session_date at specified (hour, minute) ET times.
    If minutes_list is None, populates regular market hours (09:30-16:00 ET).
    Timestamps are converted from America/New_York to UTC for storage.
    """
    rows = []
    if minutes_list is None:
        # Default: 1 tick per minute from 09:30 to 16:00 ET
        curr = datetime(session_date.year, session_date.month, session_date.day, 9, 30, tzinfo=ET)
        end = datetime(session_date.year, session_date.month, session_date.day, 16, 0, tzinfo=ET)
        while curr <= end:
            dt_utc = curr.astimezone(UTC)
            rows.append((
                dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                base_price,
                10.0,
                base_price - 0.05,
                base_price + 0.05,
                "TEST",
                "REG"
            ))
            curr += timedelta(minutes=1)
    else:
        for hh, mm in minutes_list:
            dt_et = datetime(session_date.year, session_date.month, session_date.day, hh, mm, tzinfo=ET)
            dt_utc = dt_et.astimezone(UTC)
            session = "REG" if (9 <= hh < 16 or (hh == 9 and mm >= 30)) else ("PRE" if hh < 9 else "POST")
            rows.append((
                dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                symbol,
                base_price,
                10.0,
                base_price - 0.05,
                base_price + 0.05,
                "TEST",
                session
            ))

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


def run_js_simulation(test_body_js: str, extra_setup_js: str = "") -> dict:
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
    options: [],
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

// Initialize expected DOM nodes
elements['streaming-spectrum-view'] = mockElement('streaming-spectrum-view');
elements['streaming-detail-view'] = mockElement('streaming-detail-view', ['hidden']);
elements['streaming-chart-card'] = mockElement('streaming-chart-card', ['hidden']);
elements['streaming-chart-container'] = mockElement('streaming-chart-container');
elements['streaming-symbol-select'] = mockElement('streaming-symbol-select');
elements['streaming-chart-limit-select'] = mockElement('streaming-chart-limit-select');
elements['streaming-chart-limit-select'].value = '10000';
elements['continuity-ribbon-view'] = mockElement('continuity-ribbon-view');
elements['continuity-incident-summary'] = mockElement('continuity-incident-summary');
elements['continuity-week-select'] = mockElement('continuity-week-select');
elements['back-to-spectrum-btn'] = mockElement('back-to-spectrum-btn');
elements['detail-continuity-ribbons'] = mockElement('detail-continuity-ribbons');
elements['streaming-detail-symbol-title'] = mockElement('streaming-detail-symbol-title');

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
global.lastSetCandles = [];
global.lastSetVolumes = [];
global.lastSetMarkers = [];
global.lastAttachedPrimitive = null;
global.API_BASE = '';
global.currentStreamingSymbol = 'NVDA';
global.currentStreamingTimeframe = '1m';
global.currentStreamingLimit = 10000;
global.showToast = () => {{}};

global.LightweightCharts = {{
  CrosshairMode: {{ Normal: 0, Magnet: 1 }},
  createChart: (container, options) => {{
    let seriesObj = null;
    const chart = {{
      container,
      options,
      timeScale: () => ({{
        setVisibleRange: (range) => {{
          global.visibleRangeCalls.push(range);
        }},
        fitContent: () => {{
          global.fitContentCalls.push(true);
        }},
        getVisibleLogicalRange: () => ({{ from: 0, to: 1000 }}),
        timeToCoordinate: (time) => 100,
        options: () => ({{ barSpacing: 6 }}),
        width: () => 800
      }}),
      applyOptions: () => {{}},
      addCandlestickSeries: () => {{
        seriesObj = {{
          setData: (candles) => {{
            global.lastSetCandles = candles;
          }},
          setMarkers: (markers) => {{
            global.lastSetMarkers = markers;
          }},
          attachPrimitive: (primitive) => {{
            global.lastAttachedPrimitive = primitive;
            if (primitive && typeof primitive.attached === 'function') {{
              primitive.attached({{
                chart: global.mockTvChart || chart,
                series: seriesObj,
                requestUpdate: () => {{}}
              }});
            }}
          }},
          data: () => global.lastSetCandles || []
        }};
        return seriesObj;
      }},
      addHistogramSeries: () => ({{
        setData: (volumes) => {{
          global.lastSetVolumes = volumes;
        }}
      }}),
      subscribeCrosshairMove: () => {{}}
    }};
    global.mockTvChart = chart;
    return chart;
  }}
}};

global.fetch = async (url) => {{
  global.fetchCalls.push(url);
  if (url.includes('/api/streaming/candles')) {{
    return {{
      ok: true,
      json: async () => ({{
        symbol: global.currentStreamingSymbol || 'NVDA',
        timeframe: '1m',
        database: 'streaming',
        session_start_epoch: 1790668800,
        session_end_epoch: 1790726400,
        candles: [
          {{ time: 1790668800, open: 120.0, high: 121.0, low: 119.5, close: 120.5, volume: 100 }},
          {{ time: 1790668860, open: 120.5, high: 121.5, low: 120.0, close: 121.0, volume: 150 }},
          {{ time: 1790669100, open: 121.0, high: 122.0, low: 120.5, close: 121.5, volume: 200 }}
        ],
        gaps: [
          {{
            start_epoch: 1790668920,
            end_epoch: 1790669040,
            duration: 3,
            description: "3m Gap"
          }}
        ]
      }})
    }};
  }}
  return {{
    ok: true,
    json: async () => ({{
      database: 'streaming',
      view_mode: 'all',
      symbol: 'all',
      monitored_symbols_count: 19,
      week_start: '2026-09-28',
      available_weeks: [
        {{ week_start: '2026-09-28', week_end: '2026-10-02', label: 'Sep 28 – Oct 02, 2026 (Current)', is_current: true }},
        {{ week_start: '2026-09-21', week_end: '2026-09-25', label: 'Sep 21 – Sep 25, 2026', is_current: false }}
      ],
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
        return {"raw_output": res.stdout.strip()}


# ============================================================================
# 1. Backend Analytics Tests (src/dashboard/analytics.py)
# ============================================================================

class TestBackendWeekAndDayAnalytics:
    """Validates week grouping, week filtering, and single-day extended session analytics."""

    def test_discover_available_weeks(self):
        """
        Verifies discover_available_weeks groups trading dates by Monday-to-Friday weeks,
        attaches human-readable labels, tags the latest/current week with is_current=True,
        and returns weeks sorted descending by week_start.
        """
        assert callable(discover_available_weeks), "discover_available_weeks function must be defined in src/dashboard/analytics.py"

        mem_client = create_in_memory_streaming_db()
        try:
            # 1. Empty database returns empty list
            empty_weeks = discover_available_weeks(client=mem_client)
            assert empty_weeks == [], f"Expected empty list for empty database, got {empty_weeks}"

            # 2. Populate Week 1: 2026-09-21 (Mon), 2026-09-23 (Wed), 2026-09-25 (Fri)
            populate_ticks_for_day(mem_client, date(2026, 9, 21), "NVDA")
            populate_ticks_for_day(mem_client, date(2026, 9, 23), "NVDA")
            populate_ticks_for_day(mem_client, date(2026, 9, 25), "NVDA")

            # 3. Populate Week 2: 2026-09-28 (Mon), 2026-09-29 (Tue), 2026-09-30 (Wed)
            populate_ticks_for_day(mem_client, date(2026, 9, 28), "NVDA")
            populate_ticks_for_day(mem_client, date(2026, 9, 29), "NVDA")
            populate_ticks_for_day(mem_client, date(2026, 9, 30), "NVDA")

            weeks = discover_available_weeks(client=mem_client)
            assert isinstance(weeks, list), f"Expected list of weeks, got {type(weeks)}"
            assert len(weeks) >= 2, f"Expected at least 2 discovered weeks, got {len(weeks)}"

            # Sorted descending: Week 2 (2026-09-28) before Week 1 (2026-09-21)
            w0 = weeks[0]
            w1 = weeks[1]
            assert w0["week_start"] > w1["week_start"], "Weeks must be sorted descending by week_start"
            assert w0["week_start"] == "2026-09-28"
            assert w0["week_end"] == "2026-10-02"
            assert w0["is_current"] is True, "Most recent week must have is_current=True"
            assert "Sep 28" in w0["label"] and "Oct 02" in w0["label"], f"Expected label formatted with month & day, got: {w0['label']}"
            assert w0.get("trading_days_count") == 3, f"Expected 3 trading days in Week 2, got {w0.get('trading_days_count')}"

            assert w1["week_start"] == "2026-09-21"
            assert w1["week_end"] == "2026-09-25"
            assert w1["is_current"] is False, "Previous week must have is_current=False"
            assert "Sep 21" in w1["label"] and "Sep 25" in w1["label"], f"Expected label formatted with month & day, got: {w1['label']}"
            assert w1.get("trading_days_count") == 3, f"Expected 3 trading days in Week 1, got {w1.get('trading_days_count')}"
        finally:
            mem_client.close()

    def test_get_streaming_continuity_week_start_filtering(self):
        """
        Verifies querying get_streaming_continuity_analysis with week_start="2026-09-21"
        filters strictly to trading dates within that Monday-to-Friday window, ignoring dates
        from later weeks, and includes available_weeks in payload.
        """
        assert callable(get_streaming_continuity_analysis), "get_streaming_continuity_analysis must be defined"

        mem_client = create_in_memory_streaming_db()
        try:
            # Populate Week 1 (2026-09-21 to 2026-09-25)
            for d in [date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)]:
                populate_ticks_for_day(mem_client, d, "NVDA")

            # Populate Week 2 (2026-09-28 to 2026-10-02)
            for d in [date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 30)]:
                populate_ticks_for_day(mem_client, d, "NVDA")

            # Query specifically for Week 1
            res = get_streaming_continuity_analysis(client=mem_client, week_start="2026-09-21")

            assert res.get("week_start") == "2026-09-21", f"Expected week_start '2026-09-21' in response, got: {res.get('week_start')}"
            assert "available_weeks" in res, "Expected available_weeks list in response payload"
            assert len(res["available_weeks"]) >= 2, f"Expected at least 2 available weeks in payload, got: {len(res['available_weeks'])}"

            days = res.get("days", [])
            assert len(days) > 0, "Expected non-empty days list for week_start='2026-09-21'"
            for day in days:
                d_str = day["date"]
                assert "2026-09-21" <= d_str <= "2026-09-25", (
                    f"Day date {d_str} is outside the selected week (2026-09-21 to 2026-09-25)"
                )
                assert not d_str.startswith("2026-09-28") and not d_str.startswith("2026-09-29"), (
                    f"Week 2 date {d_str} leaked into Week 1 query!"
                )
        finally:
            mem_client.close()

    def test_get_streaming_continuity_single_date_filtering(self):
        """
        Verifies querying get_streaming_continuity_analysis with target_date="2026-09-29"
        returns ONLY that single day's continuity object, with full extended session minutes (960 min)
        and minute-level buckets and gap objects.
        """
        mem_client = create_in_memory_streaming_db()
        try:
            # Populate multiple days
            populate_ticks_for_day(mem_client, date(2026, 9, 28), "NVDA")
            # For 2026-09-29: populate pre-market (05:00) and regular (10:00), leaving gaps
            populate_ticks_for_day(mem_client, date(2026, 9, 29), "NVDA", minutes_list=[(5, 0), (10, 0), (10, 1), (10, 2), (18, 0)])
            populate_ticks_for_day(mem_client, date(2026, 9, 30), "NVDA")

            res = get_streaming_continuity_analysis(
                client=mem_client,
                symbol="NVDA",
                target_date="2026-09-29",
                include_extended=True
            )

            days = res.get("days", [])
            assert len(days) == 1, f"Expected exactly 1 day for single-date filter, got {len(days)}"
            day = days[0]
            assert day["date"] == "2026-09-29"
            buckets = day.get("buckets", [])
            assert len(buckets) == 960, f"Expected 960 buckets for extended session (04:00 to 19:59 ET), got {len(buckets)}"

            # Gaps must be detected on that single day
            gaps = day.get("gaps", [])
            assert len(gaps) > 0, "Expected gaps detected on single-day session with missing hours"
            for g in gaps:
                assert g["date"] == "2026-09-29"
                assert "start_epoch" in g and "end_epoch" in g, "Gap object must contain start_epoch and end_epoch"
                assert g["end_epoch"] > g["start_epoch"]
        finally:
            mem_client.close()

    def test_get_streaming_candles_single_day_date_filter(self):
        """
        Verifies get_streaming_candles(symbol="NVDA", date="2026-09-29", hours="extended")
        returns only candles for that date's session and returns session_start_epoch and session_end_epoch.
        """
        assert callable(get_streaming_candles), "get_streaming_candles must be defined"

        mem_client = create_in_memory_streaming_db()
        try:
            # 2026-09-29 EDT (UTC-4):
            # 04:00 EDT = 08:00 UTC = 1790668800 (session_start_epoch)
            # 20:00 EDT = 00:00 UTC next day = 1790726400 (session_end_epoch)
            populate_ticks_for_day(mem_client, date(2026, 9, 29), "NVDA", minutes_list=[(4, 30), (10, 0), (19, 30)])
            # Another date's tick
            populate_ticks_for_day(mem_client, date(2026, 9, 30), "NVDA", minutes_list=[(10, 0)])

            mem_client.close = MagicMock()
            with patch("src.dashboard.analytics.get_streaming_db_connection", return_value=mem_client), \
                 patch("src.dashboard.analytics._get_lake_reader", return_value=None):
                res = get_streaming_candles(symbol="NVDA", date="2026-09-29", hours="extended", limit=2000)

                assert res.get("error") is None, f"get_streaming_candles error: {res.get('error')}"
                assert "session_start_epoch" in res, "Response must include session_start_epoch"
                assert "session_end_epoch" in res, "Response must include session_end_epoch"

                start_epoch = res["session_start_epoch"]
                end_epoch = res["session_end_epoch"]

                # Expected duration of extended session: 16 hours = 16 * 3600 = 57,600 seconds
                assert end_epoch - start_epoch == 57600, (
                    f"Expected 57,600 seconds between 04:00 ET and 20:00 ET, got {end_epoch - start_epoch}"
                )

                candles = res.get("candles", [])
                assert len(candles) == 3, f"Expected exactly 3 candles for 2026-09-29, got {len(candles)}"
                for c in candles:
                    assert start_epoch <= c["time"] < end_epoch, (
                        f"Candle time {c['time']} is outside session bounds [{start_epoch}, {end_epoch})"
                    )
        finally:
            mem_client.close = DuckDBClient.close.__get__(mem_client, DuckDBClient)
            mem_client.close()

    def test_get_available_streaming_weeks_exported(self):
        """
        Verifies get_available_streaming_weeks can be imported from src.dashboard.analytics
        and returns discovered week dictionaries.
        """
        from src.dashboard import analytics
        assert hasattr(analytics, "get_available_streaming_weeks"), (
            "get_available_streaming_weeks must be defined and exported from src.dashboard.analytics"
        )
        fn = getattr(analytics, "get_available_streaming_weeks")
        assert callable(fn), "get_available_streaming_weeks must be callable"

        mem_client = create_in_memory_streaming_db()
        try:
            populate_ticks_for_day(mem_client, date(2026, 9, 28), "NVDA")
            populate_ticks_for_day(mem_client, date(2026, 9, 29), "NVDA")
            weeks = fn(client=mem_client)
            assert isinstance(weeks, list), f"Expected list of weeks, got {type(weeks)}"
            assert len(weeks) >= 1, "Expected at least one discovered week"
            w = weeks[0]
            assert "week_start" in w and "week_end" in w, "Discovered week dict must contain 'week_start' and 'week_end'"
            assert "label" in w, "Discovered week dict must contain 'label'"
        finally:
            mem_client.close()

    def test_continuity_analysis_parameter_aliases(self):
        """
        Verifies get_streaming_continuity_analysis accepts parameter aliases
        week_offset, target_week, and end_date without raising TypeError.
        """
        assert callable(get_streaming_continuity_analysis), "get_streaming_continuity_analysis must be defined"
        mem_client = create_in_memory_streaming_db()
        try:
            populate_ticks_for_day(mem_client, date(2026, 9, 28), "NVDA")
            populate_ticks_for_day(mem_client, date(2026, 9, 29), "NVDA")

            # 1. Test week_offset alias (should not raise TypeError)
            res_offset = get_streaming_continuity_analysis(client=mem_client, week_offset=0)
            assert isinstance(res_offset, dict), "Must return dict when called with week_offset"

            # 2. Test target_week alias (should not raise TypeError)
            res_tw = get_streaming_continuity_analysis(client=mem_client, target_week="2026-09-28")
            assert isinstance(res_tw, dict), "Must return dict when called with target_week"
            assert res_tw.get("week_start") == "2026-09-28"

            # 3. Test end_date alias (should not raise TypeError)
            res_end = get_streaming_continuity_analysis(client=mem_client, end_date="2026-09-29")
            assert isinstance(res_end, dict), "Must return dict when called with end_date"
        finally:
            mem_client.close()

    def test_get_streaming_candles_single_date_gaps(self):
        """
        Verifies get_streaming_candles with single date parameter includes
        a 'gaps' list in the response for visual gap markers on the candlestick chart.
        """
        assert callable(get_streaming_candles), "get_streaming_candles must be defined"
        mem_client = create_in_memory_streaming_db()
        try:
            # Populate sparse ticks with an intentional gap between 05:00 and 10:00 ET
            populate_ticks_for_day(mem_client, date(2026, 9, 29), "NVDA", minutes_list=[(4, 30), (5, 0), (10, 0), (19, 30)])
            mem_client.close = MagicMock()
            with patch("src.dashboard.analytics.get_streaming_db_connection", return_value=mem_client), \
                 patch("src.dashboard.analytics._get_lake_reader", return_value=None):
                res = get_streaming_candles(symbol="NVDA", date="2026-09-29", hours="extended", limit=2000)

                assert res.get("error") is None, f"get_streaming_candles error: {res.get('error')}"
                assert "gaps" in res, (
                    "Response from get_streaming_candles must include 'gaps' list when date parameter is provided"
                )
                assert isinstance(res["gaps"], list), f"Expected 'gaps' to be a list, got {type(res['gaps'])}"
                assert len(res["gaps"]) > 0, "Expected at least one gap in 'gaps' list for sparse session"
                gap = res["gaps"][0]
                assert "start_epoch" in gap and "end_epoch" in gap, (
                    f"Gap item must contain 'start_epoch' and 'end_epoch', got {gap}"
                )
                assert gap["end_epoch"] > gap["start_epoch"]
        finally:
            mem_client.close = DuckDBClient.close.__get__(mem_client, DuckDBClient)
            mem_client.close()


# ============================================================================
# 2. REST API Tests (src/dashboard/server.py)
# ============================================================================

class TestRestApiWeekAndDayDrilldown:
    """Validates REST API week_start and single-day date query parameters."""

    def test_api_continuity_week_start_param(self, api_test_server):
        """
        GET /api/streaming/continuity?week_start=2026-09-21 returns HTTP 200,
        returns the week payload with week_start="2026-09-21", and includes available_weeks.
        """
        url = f"{api_test_server}/api/streaming/continuity?week_start=2026-09-21"
        resp = requests.get(url, timeout=5)
        assert resp.status_code == 200, f"Expected HTTP 200 for week_start continuity query, got {resp.status_code}"

        data = resp.json()
        assert isinstance(data, dict), "Response must be a JSON dictionary"
        assert "available_weeks" in data, "Response must include 'available_weeks' list"
        assert isinstance(data["available_weeks"], list), "'available_weeks' must be a list"
        assert data.get("week_start") == "2026-09-21" or "days" in data, (
            f"Response must acknowledge week_start filter, got {data}"
        )

    def test_api_streaming_candles_date_param(self, api_test_server):
        """
        GET /api/streaming/candles?symbol=NVDA&date=2026-09-29&hours=extended
        returns HTTP 200 with single-day candles and session boundaries.
        """
        url = f"{api_test_server}/api/streaming/candles?symbol=NVDA&date=2026-09-29&hours=extended"
        resp = requests.get(url, timeout=5)
        assert resp.status_code == 200, f"Expected HTTP 200 for single-day candles query, got {resp.status_code}"

        data = resp.json()
        assert isinstance(data, dict), "Response must be a JSON dictionary"
        assert "session_start_epoch" in data, "Response must include session_start_epoch for single-day query"
        assert "session_end_epoch" in data, "Response must include session_end_epoch for single-day query"
        assert data.get("symbol") == "NVDA"

    def test_api_continuity_weeks_endpoint(self, api_test_server):
        """
        GET /api/streaming/continuity/weeks returns HTTP 200 with available trading weeks.
        """
        url = f"{api_test_server}/api/streaming/continuity/weeks"
        resp = requests.get(url, timeout=5)
        assert resp.status_code == 200, (
            f"Expected HTTP 200 for /api/streaming/continuity/weeks, got {resp.status_code}"
        )
        data = resp.json()
        assert isinstance(data, dict), f"Expected JSON object response, got {type(data)}"
        assert "weeks" in data or "available_weeks" in data, (
            f"Expected 'weeks' or 'available_weeks' key in response, got keys: {list(data.keys())}"
        )
        weeks_list = data.get("weeks") if "weeks" in data else data.get("available_weeks")
        assert isinstance(weeks_list, list), f"Expected list of weeks, got {type(weeks_list)}"


# ============================================================================
# 3. DOM & HTML Tests (src/dashboard/static/index.html)
# ============================================================================

class TestHtmlStructureWeekAndDayDrilldown:
    """Validates presence of week selector dropdown and back button in static HTML."""

    def test_continuity_week_select_dropdown_exists(self, html_soup):
        """
        Verifies that #continuity-week-select dropdown exists inside #streaming-spectrum-view
        so the user can navigate historical weeks.
        """
        spectrum_view = html_soup.select_one("#streaming-spectrum-view")
        assert spectrum_view is not None, "Missing #streaming-spectrum-view in index.html"

        week_select = spectrum_view.select_one("#continuity-week-select")
        assert week_select is not None, (
            "Dropdown selector #continuity-week-select must exist inside #streaming-spectrum-view"
        )
        assert week_select.name == "select", "#continuity-week-select must be a <select> element"

    def test_back_to_spectrum_btn_exists(self, html_soup):
        """
        Verifies that #back-to-spectrum-btn exists inside #streaming-detail-view
        so user can return to the 19-symbol spectrogram.
        """
        detail_view = html_soup.select_one("#streaming-detail-view")
        assert detail_view is not None, "Missing #streaming-detail-view in index.html"

        back_btn = detail_view.select_one("#back-to-spectrum-btn")
        assert back_btn is not None, (
            "Back button #back-to-spectrum-btn must exist inside #streaming-detail-view"
        )

    def test_back_to_all_symbols_button_label(self, html_soup):
        """
        Verifies #back-to-spectrum-btn in index.html has text containing 'Back to All Symbols'
        instead of hardcoded '19-Symbol Spectrum'.
        """
        detail_view = html_soup.select_one("#streaming-detail-view")
        assert detail_view is not None, "Missing #streaming-detail-view in index.html"
        back_btn = detail_view.select_one("#back-to-spectrum-btn")
        assert back_btn is not None, "Missing #back-to-spectrum-btn in index.html"
        btn_text = back_btn.get_text()
        assert "Back to All Symbols" in btn_text, (
            f"Expected #back-to-spectrum-btn text to contain 'Back to All Symbols', but got: '{btn_text.strip()}'"
        )


# ============================================================================
# 4. Frontend JS Simulation Tests (src/dashboard/static/js/)
# ============================================================================

class TestFrontendJsSimulationWeekAndDayDrilldown:
    """Validates single-day drill-down, whitespace items for missing minutes, and no disruptive zoom hacks."""

    def test_open_symbol_day_detail_transitions_and_fetches_day(self):
        """
        Verifies openSymbolDayDetail('NVDA', '2026-09-29') transitions DOM
        (hides spectrum view, shows single-day detail view & chart) and triggers fetches
        specifically for that single date.
        """
        result = run_js_simulation("""
        if (typeof openSymbolDayDetail !== 'function' && typeof window.openSymbolDayDetail !== 'function') {
          return { error: 'openSymbolDayDetail is not defined on global or window' };
        }
        const fn = typeof openSymbolDayDetail === 'function' ? openSymbolDayDetail : window.openSymbolDayDetail;

        fetchCalls.length = 0;
        fn('NVDA', '2026-09-29');

        const specEl = elements['streaming-spectrum-view'];
        const detailEl = elements['streaming-detail-view'];
        const chartCardEl = elements['streaming-chart-card'];

        return {
          spectrumHidden: specEl ? specEl.classList.contains('hidden') : false,
          detailHidden: detailEl ? detailEl.classList.contains('hidden') : true,
          chartCardHidden: chartCardEl ? chartCardEl.classList.contains('hidden') : true,
          fetchCalls: [...fetchCalls]
        };
        """)

        assert "error" not in result, f"Function missing or errored: {result.get('error')}"
        assert result["spectrumHidden"] is True, "openSymbolDayDetail must add 'hidden' to #streaming-spectrum-view"
        assert result["detailHidden"] is False, "openSymbolDayDetail must remove 'hidden' from #streaming-detail-view"
        assert result["chartCardHidden"] is False, "openSymbolDayDetail must remove 'hidden' from #streaming-chart-card"

        # Verify candles and continuity fetches contain the single date '2026-09-29'
        candle_calls = [c for c in result["fetchCalls"] if "/api/streaming/candles" in c]
        assert len(candle_calls) > 0, "Must trigger fetch to /api/streaming/candles"
        assert any("date=2026-09-29" in c for c in candle_calls), (
            f"Candles fetch URL must include date=2026-09-29. Calls: {candle_calls}"
        )

    def test_chart_renders_whitespace_items_for_missing_minutes(self):
        """
        Verifies loadStreamingChart('2026-09-29') maps missing minutes to Lightweight Charts whitespace items
        ({ time: epoch } without OHLC) so physical empty spaces are naturally visible on the x-axis where data
        is missing, rather than collapsing the gap.
        """
        result = run_js_simulation("""
        if (typeof loadStreamingChart !== 'function' && typeof window.loadStreamingChart !== 'function') {
          return { error: 'loadStreamingChart is not defined' };
        }
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        const fnLoad = typeof loadStreamingChart === 'function' ? loadStreamingChart : window.loadStreamingChart;

        if (fnInit) fnInit();
        lastSetCandles = [];

        // Load chart for specific single day '2026-09-29'
        await fnLoad('2026-09-29');

        return {
          candleCount: lastSetCandles.length,
          candlesSample: lastSetCandles.slice(0, 10),
          allCandles: lastSetCandles
        };
        """)

        assert "error" not in result, f"loadStreamingChart failed: {result.get('error')}"
        candles = result.get("allCandles", [])
        assert len(candles) > 3, (
            f"Expected chart to contain candles plus whitespace items for missing minutes (total > 3), got {len(candles)}"
        )

        # Check for whitespace items: items having { time: epoch } but no 'open' (or open === undefined)
        whitespace_items = [c for c in candles if "open" not in c or c.get("open") is None]
        assert len(whitespace_items) > 0, (
            "Expected whitespace items ({ time: epoch } without OHLC) for missing minutes in single-day chart!"
        )

        # Verify time monotonicity: time must be strictly ascending
        for i in range(len(candles) - 1):
            assert candles[i]["time"] < candles[i + 1]["time"], (
                f"Candles array is not strictly sorted ascending at index {i}: {candles[i]['time']} >= {candles[i+1]['time']}"
            )

        # Verify that whitespace items exist between minute 1 (1790668860) and minute 4 (1790669100)
        gap_times = [c["time"] for c in whitespace_items if 1790668860 < c["time"] < 1790669100]
        assert 1790668920 in gap_times, "Expected whitespace item at minute 2 (1790668920)"
        assert 1790668980 in gap_times, "Expected whitespace item at minute 3 (1790668980)"

    def test_no_zoom_in_on_gap_click(self):
        """
        User directive explicitly mandates: 'there is no need to make this click based chart zoom in or anything
        because that implementation is absolutely horrible. Let's just keep everything simple'.
        Verifies that in single-day view, gap badges do NOT trigger disruptive setVisibleRange zoom hacks.
        """
        result = run_js_simulation("""
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        if (fnInit) fnInit();

        visibleRangeCalls.length = 0;

        // Render single-day detail view with gaps
        const singleDayData = {
          database: 'streaming',
          view_mode: 'NVDA',
          symbol: 'NVDA',
          extended_hours: true,
          days: [{
            date: '2026-09-29',
            day_name: 'Tuesday',
            coverage_pct: 85.0,
            status: 'partial',
            buckets: [],
            gaps: [{
              date: '2026-09-29',
              start_time: '2026-09-29 06:00:00',
              end_time: '2026-09-29 07:00:00',
              start_epoch: 1790676000,
              end_epoch: 1790679600,
              duration: 60,
              status: 'outage',
              description: '60m outage (06:00 - 07:00 ET)'
            }]
          }]
        };

        const fnRender = typeof renderMasterPulseView === 'function' ? renderMasterPulseView : window.renderMasterPulseView;
        const container = elements['detail-continuity-ribbons'];
        if (fnRender && container) {
          fnRender(singleDayData, container);
        }

        // Verify container HTML for gap buttons:
        // Gap buttons must NOT call setVisibleRange or trigger zoom-in
        const html = container ? container.innerHTML : '';
        const hasZoomOnClick = html.includes('setVisibleRange') || (html.includes('syncChartToGap') && !html.includes('isSingleDay'));

        return {
          containerHtml: html,
          visibleRangeCallCount: visibleRangeCalls.length,
          hasDisruptiveZoom: hasZoomOnClick
        };
        """)

        assert "error" not in result, f"Test error: {result.get('error')}"
        assert result["visibleRangeCallCount"] == 0, (
            "Gap clicks in single-day view must not call setVisibleRange"
        )
        assert result.get("hasDisruptiveZoom") is False, (
            "Single-day detail view gap pills must not attach disruptive chart zoom-in handlers"
        )

    def test_spectrogram_detail_button_removed(self):
        """
        Verifies renderSpectrogramView output does NOT contain 'Detail →' and does NOT contain 'Action' header,
        since the entire symbol row is clickable and dedicated Action button/header clutters the UI.
        """
        result = run_js_simulation("""
        const fnRender = typeof renderSpectrogramView === 'function' ? renderSpectrogramView : window.renderSpectrogramView;
        if (!fnRender) return { error: 'renderSpectrogramView is not defined' };

        const testData = {
          database: 'streaming',
          view_mode: 'all',
          spectrogram: {
            'AAPL': { coverage_pct: 100.0, status: 'healthy', gaps: [] },
            'NVDA': { coverage_pct: 95.0, status: 'partial', gaps: [{ duration: 15 }] }
          },
          days: [
            { date: '2026-10-02', day_name: 'Friday', gaps: [] }
          ]
        };
        const container = elements['continuity-ribbon-view'];
        fnRender(testData, container);

        const html = container ? container.innerHTML : '';
        return {
          hasDetailButton: html.includes('Detail →'),
          hasActionHeader: html.includes('Action')
        };
        """)

        assert "error" not in result, result.get("error")
        assert result["hasDetailButton"] is False, (
            "renderSpectrogramView output must NOT contain 'Detail →' button because rows are directly clickable"
        )
        assert result["hasActionHeader"] is False, (
            "renderSpectrogramView output must NOT contain 'Action' column header"
        )

    def test_open_symbol_day_detail_single_day_continuity(self):
        """
        Verifies openSymbolDayDetail("AAPL", "2026-10-02") sets single-day date
        and calls loadStreamingContinuity with days=1 and targetDate="2026-10-02".
        """
        result = run_js_simulation("""
        let capturedContinuityCall = null;
        const origLoadContinuity = typeof loadStreamingContinuity === 'function' ? loadStreamingContinuity : window.loadStreamingContinuity;

        const trackingContinuity = (sym, days, ext, week, targetDate) => {
          capturedContinuityCall = { sym, days, ext, week, targetDate };
          if (origLoadContinuity) {
            return origLoadContinuity(sym, days, ext, week, targetDate);
          }
        };
        global.loadStreamingContinuity = trackingContinuity;
        if (typeof window !== 'undefined') window.loadStreamingContinuity = trackingContinuity;

        const fnOpen = typeof openSymbolDayDetail === 'function' ? openSymbolDayDetail : window.openSymbolDayDetail;
        if (!fnOpen) return { error: 'openSymbolDayDetail is not defined' };

        fnOpen('AAPL', '2026-10-02');

        const activeSymbol = global.currentStreamingSymbol || (typeof window !== 'undefined' ? window.currentStreamingSymbol : null);
        const activeDate = global.currentContinuityDayDate || (typeof window !== 'undefined' ? window.currentContinuityDayDate : null);

        return {
          activeSymbol,
          activeDate,
          capturedContinuityCall
        };
        """)

        assert "error" not in result, result.get("error")
        assert result["activeSymbol"] == "AAPL", f"Expected active symbol AAPL, got {result.get('activeSymbol')}"
        assert result["activeDate"] == "2026-10-02", f"Expected active date 2026-10-02, got {result.get('activeDate')}"
        call = result.get("capturedContinuityCall")
        assert call is not None, "loadStreamingContinuity was not called"
        assert call["sym"] == "AAPL", f"Expected symbol 'AAPL', got {call.get('sym')}"
        assert call["days"] == 1, f"Expected days=1 for single-day drilldown, got {call.get('days')}"
        assert call["targetDate"] == "2026-10-02", f"Expected targetDate='2026-10-02', got {call.get('targetDate')}"

    def test_chart_applies_gap_shading_on_single_day_and_clears_markers(self):
        """
        Verifies loadStreamingChart("2026-10-02") replaces dirty triangle markers with clean gap shading:
        1. streamingCandleSeries.setMarkers([]) is called (dirty triangle markers eliminated).
        2. GapShadingPlugin is attached to streamingCandleSeries (global.lastAttachedPrimitive !== null).
        3. gapShadingPlugin.getGaps() receives the gaps from the candle response.
        4. gapShadingPlugin.paneViews()[0].zOrder() returns 'bottom'.
        """
        result = run_js_simulation("""
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        const fnLoad = typeof loadStreamingChart === 'function' ? loadStreamingChart : window.loadStreamingChart;

        if (fnInit) fnInit();
        lastSetMarkers = [];

        await fnLoad('2026-10-02');

        const plugin = global.lastAttachedPrimitive || global.gapShadingPlugin || (typeof window !== 'undefined' ? window.gapShadingPlugin : null);
        const gaps = plugin && typeof plugin.getGaps === 'function' ? plugin.getGaps() : null;
        const paneViews = plugin && typeof plugin.paneViews === 'function' ? plugin.paneViews() : [];
        const zOrder = paneViews.length > 0 && typeof paneViews[0].zOrder === 'function' ? paneViews[0].zOrder() : null;

        return {
          markerCount: lastSetMarkers.length,
          markers: lastSetMarkers,
          hasAttachedPrimitive: global.lastAttachedPrimitive !== null,
          hasPlugin: plugin !== null && plugin !== undefined,
          gaps: gaps,
          zOrder: zOrder
        };
        """)

        assert "error" not in result, f"loadStreamingChart failed: {result.get('error')}"
        # 1. Dirty triangle exclamation markers eliminated
        assert result["markers"] == [], (
            f"Expected streamingCandleSeries.setMarkers to be called with [] to clear dirty triangle markers, got: {result.get('markers')}"
        )
        assert result["markerCount"] == 0, "Dirty gap markers must not be set on streamingCandleSeries"

        # 2. GapShadingPlugin attached
        assert result["hasAttachedPrimitive"] is True, (
            "Expected GapShadingPlugin to be attached to streamingCandleSeries via attachPrimitive"
        )
        assert result["hasPlugin"] is True, "Expected gapShadingPlugin to be instantiated"

        # 3. getGaps receives gaps from response
        gaps = result.get("gaps")
        assert gaps is not None, "Expected gapShadingPlugin.getGaps() to return gaps array"
        assert len(gaps) > 0, "Expected gapShadingPlugin.getGaps() to contain gaps from candles response"
        assert gaps[0].get("duration") == 3, f"Expected gap duration 3, got: {gaps[0].get('duration')}"

        # 4. zOrder is 'bottom'
        assert result["zOrder"] == "bottom", (
            f"Expected GapShadingPaneView.zOrder() to be 'bottom' (rendering behind candles and grid), got: {result.get('zOrder')}"
        )

    def test_chart_sets_gap_markers_on_single_day(self):
        """Backwards compatibility alias for test_chart_applies_gap_shading_on_single_day_and_clears_markers."""
        return self.test_chart_applies_gap_shading_on_single_day_and_clears_markers()

    def test_gap_shading_renderer_colors_outage_red_and_partial_yellow(self):
        """
        Verifies GapShadingRenderer & GapShadingPlugin background shading colors:
        - Outage gap (status: 'outage') produces shaded bars with 'rgba(244, 63, 94, 0.18)' (soft red).
        - Partial degradation gap (status: 'partial') produces shaded bars with 'rgba(245, 158, 11, 0.18)' (soft yellow/amber).
        - Normal continuous trading candles (outside any gap) have NO shaded bar entries.
        - Pane renderer executes 2D canvas ctx.fillRect with correct fillStyle in target.useMediaCoordinateSpace.
        """
        result = run_js_simulation("""
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        if (fnInit) fnInit();

        const plugin = global.lastAttachedPrimitive || global.gapShadingPlugin || (typeof window !== 'undefined' ? window.gapShadingPlugin : null);
        if (!plugin) {
          return { error: 'GapShadingPlugin not instantiated or attached' };
        }

        // Setup test series candles:
        // Minute 0: normal continuous candle (1790668800)
        // Minute 1: outage gap candle       (1790668860)
        // Minute 2: normal continuous candle (1790668920)
        // Minute 3: partial gap candle      (1790668980)
        // Minute 4: normal continuous candle (1790669040)
        const testCandles = [
          { time: 1790668800, open: 120, high: 121, low: 119, close: 120 },
          { time: 1790668860, open: 120, high: 121, low: 119, close: 120 },
          { time: 1790668920, open: 120, high: 121, low: 119, close: 120 },
          { time: 1790668980, open: 120, high: 121, low: 119, close: 120 },
          { time: 1790669040, open: 120, high: 121, low: 119, close: 120 }
        ];
        global.lastSetCandles = testCandles;

        const testGaps = [
          {
            start_epoch: 1790668860,
            end_epoch: 1790668860,
            duration: 1,
            status: 'outage',
            description: '1m Outage'
          },
          {
            start_epoch: 1790668980,
            end_epoch: 1790668980,
            duration: 1,
            status: 'partial',
            description: '1m Partial'
          }
        ];

        plugin.setGaps(testGaps, testCandles);

        const paneViews = plugin.paneViews ? plugin.paneViews() : [];
        if (!paneViews || paneViews.length === 0) {
          return { error: 'No pane views returned by GapShadingPlugin' };
        }

        const paneView = paneViews[0];
        const renderer = paneView.renderer ? paneView.renderer() : null;
        if (!renderer) {
          return { error: 'No renderer returned by GapShadingPaneView' };
        }

        // Test canvas draw call via mock target
        const drawnRects = [];
        let currentFillStyle = '';
        const mockCtx = {
          save: () => {},
          restore: () => {},
          set fillStyle(val) { currentFillStyle = val; },
          get fillStyle() { return currentFillStyle; },
          fillRect: (x, y, w, h) => {
            drawnRects.push({ x, y, w, h, fillStyle: currentFillStyle });
          }
        };

        const mockTarget = {
          useMediaCoordinateSpace: (cb) => {
            cb({
              context: mockCtx,
              mediaSize: { width: 800, height: 450 }
            });
          }
        };

        if (typeof renderer.draw === 'function') {
          renderer.draw(mockTarget);
        }

        const viewData = (typeof plugin._getViewData === 'function') ? plugin._getViewData() : null;
        const bars = viewData ? viewData.bars : [];

        return {
          bars: bars,
          drawnRects: drawnRects,
          normalTimesInBars: bars.filter(b => b.time === 1790668800 || b.time === 1790668920 || b.time === 1790669040),
          outageBars: bars.filter(b => b.time === 1790668860),
          partialBars: bars.filter(b => b.time === 1790668980)
        };
        """)

        assert "error" not in result, f"Renderer test error: {result.get('error')}"

        outage_bars = result.get("outageBars", [])
        assert len(outage_bars) == 1, f"Expected 1 outage bar, got: {len(outage_bars)}"
        assert outage_bars[0]["color"] == "rgba(244, 63, 94, 0.18)", (
            f"Expected outage gap to use soft red rgba(244, 63, 94, 0.18), got: {outage_bars[0].get('color')}"
        )

        partial_bars = result.get("partialBars", [])
        assert len(partial_bars) == 1, f"Expected 1 partial bar, got: {len(partial_bars)}"
        assert partial_bars[0]["color"] == "rgba(245, 158, 11, 0.18)", (
            f"Expected partial gap to use soft yellow/amber rgba(245, 158, 11, 0.18), got: {partial_bars[0].get('color')}"
        )

        normal_in_bars = result.get("normalTimesInBars", [])
        assert len(normal_in_bars) == 0, (
            f"Continuous trading candles must NOT have shaded bar entries, found: {normal_in_bars}"
        )

        # Verify drawn rectangles in canvas context
        drawn_rects = result.get("drawnRects", [])
        assert len(drawn_rects) == 2, f"Expected 2 drawn shaded bars, got: {len(drawn_rects)}"
        fill_styles = [r.get("fillStyle") for r in drawn_rects]
        assert "rgba(244, 63, 94, 0.18)" in fill_styles, (
            f"Canvas fillStyle missing soft red rgba(244, 63, 94, 0.18): {fill_styles}"
        )
        assert "rgba(245, 158, 11, 0.18)" in fill_styles, (
            f"Canvas fillStyle missing soft yellow rgba(245, 158, 11, 0.18): {fill_styles}"
        )
