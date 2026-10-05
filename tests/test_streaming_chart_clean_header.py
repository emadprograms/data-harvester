"""
Automated Test Suite for Clean Streaming Chart Header / Legend.

Verifies:
1. Backend (`src/dashboard/analytics.py` in `get_streaming_candles`):
   - When `date` is passed (e.g. date='2026-09-28'), returns:
     - `day_total_ticks`: Total ticks for that symbol across the entire ET calendar day (00:00–24:00 ET).
     - `session_total_ticks`: Total ticks within the requested session hours
       (04:00–20:00 ET for extended, 09:30–16:00 ET for regular).
   - Zero-tick days return `day_total_ticks: 0` and `session_total_ticks: 0`.
2. HTML (`src/dashboard/static/index.html`):
   - Inside `#streaming-chart-legend`, clutter is removed:
     - Badges removed: "STREAMING DB", "LIVE BUFFER", "TICK-RESAMPLED".
     - OHLCV hover spans removed: `#streaming-legend-open`, `#streaming-legend-high`,
       `#streaming-legend-low`, `#streaming-legend-close`, `#streaming-legend-volume`.
   - Clean header elements present:
     - `#streaming-legend-symbol` (e.g. "AAPL")
     - `#streaming-legend-duration` (e.g. "2026-09-28 • 04:00–20:00 ET (Extended)")
     - `#streaming-legend-ticks` (e.g. "33,453 ticks in database")
3. Frontend JS Simulation (`src/dashboard/static/js/chart.js`):
   - `loadStreamingChart('2026-09-28', 'extended')` populates clean header:
     - Symbol set to "AAPL" (no timeframe clutter like "(1M)").
     - Duration set to "2026-09-28 • 04:00–20:00 ET (Extended)".
     - Ticks set to "33,453 ticks in database".
   - Regular hours duration formatted as "09:30–16:00 ET (Regular)".
   - Crosshair hover / `updateStreamingLegend` does NOT overwrite the day ticks summary
     with single-candle ticks (e.g. "1 ticks") or clobber symbol name.
"""

import json
import os
import shutil
import subprocess
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from bs4 import BeautifulSoup

from src.dashboard.analytics import get_streaming_candles
from tests.support.lake_population import create_lake, publish_minutes

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

HTML_PATH = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "index.html"
JS_DIR = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "js"


def run_js_simulation(test_body_js: str, extra_setup_js: str = "") -> dict:
    """
    Executes a Node.js simulation of frontend chart and legend logic,
    evaluating src/dashboard/static/js/chart.js and src/dashboard/static/js/continuity.js.
    """
    node_bin = shutil.which("node")
    if not node_bin:
        pytest.skip("Node.js is required to execute frontend JS simulation tests")

    chart_js_path = JS_DIR / "chart.js"
    continuity_js_path = JS_DIR / "continuity.js"

    chart_js = chart_js_path.read_text(encoding="utf-8") if chart_js_path.exists() else ""
    continuity_js = continuity_js_path.read_text(encoding="utf-8") if continuity_js_path.exists() else ""

    script = f"""
const elements = {{}};
function mockElement(id, initialClasses = []) {{
  const classes = new Set(initialClasses);
  return {{
    id,
    className: initialClasses.join(' '),
    innerHTML: '',
    innerText: '',
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
    querySelector: (sel) => null,
    addEventListener: () => {{}}
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
elements['streaming-chart-loading'] = mockElement('streaming-chart-loading', ['hidden']);

// Header / Legend DOM nodes
elements['streaming-chart-legend'] = mockElement('streaming-chart-legend');
elements['streaming-legend-symbol'] = mockElement('streaming-legend-symbol');
elements['streaming-legend-duration'] = mockElement('streaming-legend-duration');
elements['streaming-legend-ticks'] = mockElement('streaming-legend-ticks');
elements['streaming-legend-time'] = mockElement('streaming-legend-time');
elements['streaming-legend-open'] = mockElement('streaming-legend-open');
elements['streaming-legend-high'] = mockElement('streaming-legend-high');
elements['streaming-legend-low'] = mockElement('streaming-legend-low');
elements['streaming-legend-close'] = mockElement('streaming-legend-close');
elements['streaming-legend-volume'] = mockElement('streaming-legend-volume');
elements['streaming-legend-change'] = mockElement('streaming-legend-change');

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
global.crosshairHandler = null;
global.API_BASE = '';
global.currentStreamingSymbol = 'AAPL';
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
          }},
          data: () => global.lastSetCandles || []
        }};
        global.mockCandleSeries = seriesObj;
        return seriesObj;
      }},
      addHistogramSeries: () => ({{
        setData: (volumes) => {{
          global.lastSetVolumes = volumes;
        }}
      }}),
      subscribeCrosshairMove: (cb) => {{
        global.crosshairHandler = cb;
      }}
    }};
    global.mockTvChart = chart;
    return chart;
  }}
}};

global.fetch = async (url) => {{
  global.fetchCalls.push(url);
  if (url.includes('/api/streaming/candles')) {{
    const urlObj = new URL(url, 'http://localhost');
    const isRegular = url.includes('hours=regular') || urlObj.searchParams.get('hours') === 'regular';
    const sym = urlObj.searchParams.get('symbol') || global.currentStreamingSymbol || 'AAPL';
    const date = urlObj.searchParams.get('date') || '2026-09-28';
    const hours = isRegular ? 'regular' : 'extended';
    return {{
      ok: true,
      json: async () => ({{
        symbol: sym,
        timeframe: '1m',
        database: 'streaming',
        date: date,
        hours: hours,
        day_total_ticks: 33453,
        session_total_ticks: isRegular ? 28000 : 33400,
        session_start_epoch: isRegular ? 1790688600 : 1790668800,
        session_end_epoch: isRegular ? 1790712000 : 1790726400,
        candles: [
          {{ time: 1790688600, open: 338.5, high: 338.5, low: 338.5, close: 338.5, volume: 1, tick_count: 1 }}
        ],
        gaps: []
      }})
    }};
  }}
  return {{
    ok: true,
    json: async () => ({{}})
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
# 1. Backend Analytics Tests: DuckDB Total Ticks Calculation
# ============================================================================

class TestStreamingCandlesTickCounts:
    """
    Verifies that get_streaming_candles returns `day_total_ticks` (full ET calendar day)
    and `session_total_ticks` (requested session hours: extended vs regular).
    """

    def test_get_streaming_candles_returns_day_and_session_ticks(self, tmp_path, monkeypatch):
        """
        Tests with in-memory DuckDB fixture that querying `get_streaming_candles(symbol, date=..., hours=...)`
        returns `day_total_ticks` and `session_total_ticks`.
        """
        lake = create_lake(tmp_path / "lake", symbols=["NVDA", "AAPL", "MSFT", "SPY", "TEST_SYM", "BOUNDARY_SYM", "CONSISTENCY_SYM", "ADBE", "AMD", "APP", "TSLA"])
        monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
        monkeypatch.setenv("DATA_DIR", str(lake))
        target_date = date(2026, 9, 28)

        # Populate ticks for AAPL on 2026-09-28:
        # - 02:30 ET: 5 ticks (overnight, within ET day, outside extended session)
        # - 06:15 ET: 15 ticks (pre-market, within ET day, inside extended session, outside regular)
        # - 11:30 ET: 50 ticks (regular market hours, within ET day, inside extended and regular)
        # - 18:45 ET: 20 ticks (after-hours, within ET day, inside extended session, outside regular)
        # - 21:15 ET: 10 ticks (evening, within ET day, outside extended session)
        # Total for full ET day = 5 + 15 + 50 + 20 + 10 = 100 ticks.
        # Extended session (04:00–20:00 ET) = 15 + 50 + 20 = 85 ticks.
        # Regular session (09:30–16:00 ET) = 50 ticks.
        publish_minutes(lake, target_date,
            "AAPL",
            [
                (2, 30, 5),
                (6, 15, 15),
                (11, 30, 50),
                (18, 45, 20),
                (21, 15, 10),
            ],
        )

        # Also populate ticks on a different day and for another symbol to ensure proper filtering:
        publish_minutes(lake, date(2026, 9, 29), "AAPL", [(10, 0, 40)])
        publish_minutes(lake, target_date, "MSFT", [(10, 0, 60)])

        # 1. Query extended session
        res_ext = get_streaming_candles("AAPL", date="2026-09-28", hours="extended")
        assert "day_total_ticks" in res_ext, "get_streaming_candles response missing 'day_total_ticks'"
        assert "session_total_ticks" in res_ext, "get_streaming_candles response missing 'session_total_ticks'"
        assert res_ext["day_total_ticks"] == 100, f"Expected 100 day_total_ticks, got {res_ext.get('day_total_ticks')}"
        assert res_ext["session_total_ticks"] == 85, f"Expected 85 session_total_ticks, got {res_ext.get('session_total_ticks')}"

        # 2. Query regular session
        res_reg = get_streaming_candles("AAPL", date="2026-09-28", hours="regular")
        assert "day_total_ticks" in res_reg, "get_streaming_candles response missing 'day_total_ticks'"
        assert "session_total_ticks" in res_reg, "get_streaming_candles response missing 'session_total_ticks'"
        assert res_reg["day_total_ticks"] == 100, f"Expected 100 day_total_ticks, got {res_reg.get('day_total_ticks')}"
        assert res_reg["session_total_ticks"] == 50, f"Expected 50 session_total_ticks, got {res_reg.get('session_total_ticks')}"

    def test_get_streaming_candles_zero_ticks_day(self, tmp_path, monkeypatch):
        """
        Tests that querying a day with 0 ticks returns `day_total_ticks: 0` and `session_total_ticks: 0`.
        """
        lake = create_lake(tmp_path / "lake", symbols=["NVDA", "AAPL", "MSFT", "SPY", "TEST_SYM", "BOUNDARY_SYM", "CONSISTENCY_SYM", "ADBE", "AMD", "APP", "TSLA"])
        monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
        monkeypatch.setenv("DATA_DIR", str(lake))
        res_zero = get_streaming_candles("AAPL", date="2026-10-05", hours="extended")

        assert "day_total_ticks" in res_zero, "get_streaming_candles response missing 'day_total_ticks'"
        assert "session_total_ticks" in res_zero, "get_streaming_candles response missing 'session_total_ticks'"
        assert res_zero["day_total_ticks"] == 0, f"Expected 0 day_total_ticks, got {res_zero.get('day_total_ticks')}"
        assert res_zero["session_total_ticks"] == 0, f"Expected 0 session_total_ticks, got {res_zero.get('session_total_ticks')}"
        assert res_zero["count"] == 0


# ============================================================================
# 2. HTML Legend Structure Tests: Removal of Clutter & Badges
# ============================================================================

class TestStreamingLegendHtml:
    """
    Verifies that index.html contains clean header elements (#streaming-legend-symbol,
    #streaming-legend-duration, #streaming-legend-ticks) and has removed bloated badges
    and hover OHLCV clutter.
    """

    @pytest.fixture(autouse=True)
    def setup_html(self):
        assert HTML_PATH.exists(), f"File not found: {HTML_PATH}"
        self.html_content = HTML_PATH.read_text(encoding="utf-8")
        self.soup = BeautifulSoup(self.html_content, "html.parser")
        self.legend = self.soup.find(id="streaming-chart-legend")
        assert self.legend is not None, "#streaming-chart-legend element not found in index.html"

    def test_streaming_chart_legend_required_elements_exist(self):
        """
        Asserts `#streaming-chart-legend` contains `#streaming-legend-symbol`,
        `#streaming-legend-duration`, and `#streaming-legend-ticks`.
        """
        sym_el = self.legend.find(id="streaming-legend-symbol")
        duration_el = self.legend.find(id="streaming-legend-duration")
        ticks_el = self.legend.find(id="streaming-legend-ticks")

        assert sym_el is not None, "Missing #streaming-legend-symbol element inside #streaming-chart-legend"
        assert duration_el is not None, "Missing #streaming-legend-duration element inside #streaming-chart-legend"
        assert ticks_el is not None, "Missing #streaming-legend-ticks element inside #streaming-chart-legend"

    def test_streaming_chart_legend_clutter_removed(self):
        """
        Asserts `#streaming-chart-legend` does NOT contain:
        - "STREAMING DB" badge
        - "LIVE BUFFER" badge
        - "TICK-RESAMPLED" badge
        - OHLCV individual hover elements (#streaming-legend-open, etc.)
        """
        legend_text = self.legend.get_text()

        # Clutter badges must be removed
        assert "STREAMING DB" not in legend_text, "Found bloated badge 'STREAMING DB' inside #streaming-chart-legend"
        assert "LIVE BUFFER" not in legend_text, "Found bloated badge 'LIVE BUFFER' inside #streaming-chart-legend"
        assert "TICK-RESAMPLED" not in legend_text, "Found bloated badge 'TICK-RESAMPLED' inside #streaming-chart-legend"

        # Individual candle OHLCV hover spans must be removed from the legend
        assert self.legend.find(id="streaming-legend-open") is None, "Found #streaming-legend-open inside #streaming-chart-legend"
        assert self.legend.find(id="streaming-legend-high") is None, "Found #streaming-legend-high inside #streaming-chart-legend"
        assert self.legend.find(id="streaming-legend-low") is None, "Found #streaming-legend-low inside #streaming-chart-legend"
        assert self.legend.find(id="streaming-legend-close") is None, "Found #streaming-legend-close inside #streaming-chart-legend"
        assert self.legend.find(id="streaming-legend-volume") is None, "Found #streaming-legend-volume inside #streaming-chart-legend"


# ============================================================================
# 3. Frontend JS Simulation Tests: Populating Clean Header & Guarding Against Overwrite
# ============================================================================

class TestStreamingLegendJsSimulation:
    """
    Simulates frontend execution in Node.js to verify header population and crosshair immutability.
    """

    def test_load_streaming_chart_populates_clean_header(self):
        """
        Node simulation verifying `loadStreamingChart('2026-09-28', 'extended')` sets:
        - Symbol (e.g. "AAPL", without "(1M)" clutter)
        - Duration (e.g. "2026-09-28 • 04:00–20:00 ET (Extended)")
        - Ticks (e.g. "33,453 ticks in database")
        """
        result = run_js_simulation("""
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        const fnLoad = typeof loadStreamingChart === 'function' ? loadStreamingChart : window.loadStreamingChart;

        if (fnInit) fnInit();
        global.currentStreamingSymbol = 'AAPL';
        await fnLoad('2026-09-28', 'extended');

        const symEl = document.getElementById('streaming-legend-symbol');
        const durEl = document.getElementById('streaming-legend-duration');
        const ticksEl = document.getElementById('streaming-legend-ticks');

        return {
          symbol: symEl ? symEl.innerText : '',
          duration: durEl ? durEl.innerText : '',
          ticks: ticksEl ? ticksEl.innerText : ''
        };
        """)

        # 1. Clean Symbol: should contain symbol name without (1M) timeframe clutter
        assert "AAPL" in result["symbol"], f"Symbol text did not contain AAPL: {result['symbol']}"
        assert "(1M)" not in result["symbol"] and "(1m)" not in result["symbol"], (
            f"Symbol text still has bloated timeframe clutter: {result['symbol']}"
        )

        # 2. Chart Duration: should include date, session hours, ET timezone, and Extended badge/label
        duration_text = result["duration"]
        assert "2026-09-28" in duration_text, f"Duration missing date: {duration_text}"
        assert ("04:00–20:00" in duration_text or "04:00-20:00" in duration_text), (
            f"Duration missing extended hours range (04:00–20:00): {duration_text}"
        )
        assert "ET" in duration_text, f"Duration missing ET timezone: {duration_text}"
        assert "Extended" in duration_text, f"Duration missing Extended session label: {duration_text}"

        # 3. Database Ticks Count: should display total ticks for that symbol on that day in database
        ticks_text = result["ticks"]
        assert "33,453" in ticks_text, f"Ticks text missing formatted count 33,453: {ticks_text}"
        assert "ticks in database" in ticks_text.lower(), (
            f"Ticks text missing 'ticks in database' explanation: {ticks_text}"
        )
        assert "1 ticks" not in ticks_text, (
            f"Ticks text incorrectly shows 1-candle tick count instead of day total: {ticks_text}"
        )

    def test_regular_hours_duration_formatting(self):
        """
        Node simulation verifying regular hours shows "09:30–16:00 ET (Regular)".
        """
        result = run_js_simulation("""
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        const fnLoad = typeof loadStreamingChart === 'function' ? loadStreamingChart : window.loadStreamingChart;

        if (fnInit) fnInit();
        global.currentStreamingSymbol = 'AAPL';
        await fnLoad('2026-09-28', 'regular');

        const durEl = document.getElementById('streaming-legend-duration');

        return {
          duration: durEl ? durEl.innerText : ''
        };
        """)

        duration_text = result["duration"]
        assert "2026-09-28" in duration_text, f"Duration missing date: {duration_text}"
        assert ("09:30–16:00" in duration_text or "09:30-16:00" in duration_text), (
            f"Duration missing regular hours range (09:30–16:00): {duration_text}"
        )
        assert "ET" in duration_text, f"Duration missing ET timezone: {duration_text}"
        assert "Regular" in duration_text, f"Duration missing Regular session label: {duration_text}"

    def test_crosshair_does_not_overwrite_day_summary(self):
        """
        Node simulation verifying crosshair movement / `updateStreamingLegend` does not
        clobber symbol or overwrite day ticks count with 1-candle ticks.
        """
        result = run_js_simulation("""
        const fnInit = typeof initStreamingChart === 'function' ? initStreamingChart : window.initStreamingChart;
        const fnLoad = typeof loadStreamingChart === 'function' ? loadStreamingChart : window.loadStreamingChart;
        const fnUpdate = typeof updateStreamingLegend === 'function' ? updateStreamingLegend : window.updateStreamingLegend;

        if (fnInit) fnInit();
        global.currentStreamingSymbol = 'AAPL';
        await fnLoad('2026-09-28', 'extended');

        const symBefore = document.getElementById('streaming-legend-symbol').innerText;
        const ticksBefore = document.getElementById('streaming-legend-ticks').innerText;

        // Hover over a candle having tick_count = 1
        const mockCandle = {
          time: 1790688600,
          open: 338.5,
          high: 338.5,
          low: 338.5,
          close: 338.5,
          volume: 1,
          tick_count: 1
        };

        // 1. Invoke updateStreamingLegend directly
        if (fnUpdate) {
          fnUpdate(mockCandle);
        }

        // 2. Trigger crosshair callback if subscribed
        if (global.crosshairHandler) {
          const map = new Map();
          map.set(global.mockCandleSeries, mockCandle);
          global.crosshairHandler({
            time: 1790688600,
            seriesData: map
          });
        }

        const symAfter = document.getElementById('streaming-legend-symbol').innerText;
        const durAfter = document.getElementById('streaming-legend-duration').innerText;
        const ticksAfter = document.getElementById('streaming-legend-ticks').innerText;

        return {
          symBefore,
          symAfter,
          ticksBefore,
          ticksAfter,
          durAfter
        };
        """)

        # Ticks summary must remain intact and NOT be replaced by single candle tick count
        assert "1 ticks" not in result["ticksAfter"], (
            f"Crosshair hover overwrote day total ticks with candle ticks: {result['ticksAfter']}"
        )
        assert "33,453" in result["ticksAfter"], (
            f"Ticks summary lost 33,453 count after crosshair move: {result['ticksAfter']}"
        )
        assert "ticks in database" in result["ticksAfter"].lower(), (
            f"Ticks summary lost 'ticks in database' text after crosshair move: {result['ticksAfter']}"
        )

        # Symbol must remain clean (not clobbered by AAPL (1M))
        assert "(1M)" not in result["symAfter"] and "(1m)" not in result["symAfter"], (
            f"Crosshair hover overwrote clean symbol with timeframe: {result['symAfter']}"
        )
        assert "AAPL" in result["symAfter"], f"Symbol missing AAPL: {result['symAfter']}"
