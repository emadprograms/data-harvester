"""
Automated Test Suite for Streaming Page Load & Spectrogram Initialization.

Verifies:
1. Default Continuity View State:
   - Upon script evaluation of src/dashboard/static/js/continuity.js:
     currentContinuityView === 'spectrum', window.currentContinuityView === 'spectrum',
     and currentContinuitySymbol === 'all'.
2. Extended Hours Spectrogram Page Load:
   - When switchDashboardView('streaming') runs on page load, loadStreamingContinuity
     is called with 'all' and extended=true (or currentContinuityExtended=true, not false).
   - Fetch query contains symbol=all, extended=true, hours=extended.
3. Chart Init Does Not Clobber Spectrogram:
   - When loadStreamingChart() runs on initial chart initialization, it does NOT fetch
     single-symbol NVDA continuity data or clobber #continuity-ribbon-view.
4. Spectrogram Rendered on Page Load:
   - #continuity-ribbon-view renders the 19-Symbol Spectrogram rows and does NOT contain
     renderMasterPulseView markup ("Healthy (Continuous)", "Partial Degradation", etc.).
5. Single-Day Drill-Down Renders in Detail Container:
   - openSymbolDayDetail('NVDA', '2026-09-28') renders into #detail-continuity-ribbons,
     and does NOT clobber the cached 19-symbol spectrogram in #continuity-ribbon-view.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

JS_DIR = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "js"
CONTINUITY_JS_PATH = JS_DIR / "continuity.js"
CHART_JS_PATH = JS_DIR / "chart.js"
APP_JS_PATH = JS_DIR / "app.js"

MONITORED_19_SYMBOLS = [
    "AAPL", "ADBE", "AMD", "AMZN", "APP",
    "AVGO", "BABA", "GOOGL", "META", "MSFT",
    "MU", "NDAQ", "NVDA", "ORCL", "PANW",
    "QCOM", "SHOP", "TSLA", "TSM"
]


def run_js_simulation(test_body_js: str, extra_setup_js: str = "", load_app_js: bool = True) -> dict:
    """
    Executes a Node.js simulation of frontend continuity, chart, and app routing logic,
    evaluating continuity.js, chart.js, and optionally app.js with a complete DOM and API mock.
    """
    node_bin = shutil.which("node")
    if not node_bin:
        pytest.skip("Node.js is required to execute frontend JS simulation tests")

    continuity_js = CONTINUITY_JS_PATH.read_text(encoding="utf-8") if CONTINUITY_JS_PATH.exists() else ""
    chart_js = CHART_JS_PATH.read_text(encoding="utf-8") if CHART_JS_PATH.exists() else ""
    app_js = APP_JS_PATH.read_text(encoding="utf-8") if (load_app_js and APP_JS_PATH.exists()) else ""

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
    checked: true,
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
    addEventListener: (evt, cb) => {{}}
  }};
}}

// Initialize expected DOM nodes
const elementIds = [
  'view-streaming', 'nav-streaming',
  'streaming-spectrum-view', 'streaming-detail-view', 'streaming-chart-card',
  'streaming-chart-container', 'streaming-chart-loading', 'streaming-symbol-select',
  'streaming-chart-limit-select', 'continuity-ribbon-canvas', 'continuity-ribbon-view',
  'continuity-incident-summary', 'continuity-week-select', 'continuity-extended-toggle',
  'continuity-hours-subtitle', 'detail-session-hours-badge', 'back-to-spectrum-btn',
  'detail-continuity-ribbons', 'streaming-detail-symbol-title', 'streaming-legend-symbol',
  'streaming-legend-duration', 'streaming-legend-ticks'
];
elementIds.forEach(id => {{ elements[id] = mockElement(id); }});
elements['streaming-detail-view'].classList.add('hidden');
elements['streaming-chart-card'].classList.add('hidden');
elements['streaming-chart-limit-select'].value = '10000';

global.document = {{
  getElementById: (id) => elements[id] || (elements[id] = mockElement(id)),
  querySelectorAll: (sel) => [],
  querySelector: (sel) => null
}};

global.window = global;
global.window.addEventListener = () => {{}};
global.setInterval = () => {{}};
global.setTimeout = setTimeout;

global.fetchCalls = [];
global.visibleRangeCalls = [];
global.fitContentCalls = [];
global.lastSetCandles = [];
global.lastSetVolumes = [];
global.lastSetMarkers = [];

global.API_BASE = '';
global.location = {{ origin: '' }};
global.currentStreamingSymbol = 'NVDA';
global.currentStreamingTimeframe = '1m';
global.currentStreamingLimit = 10000;
global.showToast = () => {{}};
global.fetchStreamingSymbols = () => {{}};
global.fetchStreamStatus = () => {{}};
global.fetchStreamTape = () => {{}};
global.pollHarvesterLogs = () => {{}};
global.fetchMarketSession = () => {{}};
global.fetchSystemHealth = () => {{}};
global.fetchSymbolsCoverage = () => {{}};

// State variables matching state.js
global.tvChart = null;
global.candleSeries = null;
global.volumeSeries = null;
global.tvStreamingChart = null;
global.streamingCandleSeries = null;
global.streamingVolumeSeries = null;
global.gapShadingPlugin = null;
global.loadedStreamingCandles = [];
global.streamingSymbolsList = [];
global.currentDashboardView = 'historical';
global.currentStreamingTab = 'chart';

var tvStreamingChart = null;
var streamingCandleSeries = null;
var streamingVolumeSeries = null;
var gapShadingPlugin = null;
var loadedStreamingCandles = [];
var streamingSymbolsList = [];
var currentDashboardView = 'historical';
var currentStreamingTab = 'chart';
var API_BASE = '';

let candleSeriesObj = null;
let volumeSeriesObj = null;

global.LightweightCharts = {{
  CrosshairMode: {{ Normal: 0, Magnet: 1 }},
  createChart: (container, options) => {{
    const chart = {{
      container,
      options,
      timeScale: () => ({{
        setVisibleRange: (range) => global.visibleRangeCalls.push(range),
        fitContent: () => global.fitContentCalls.push(true),
        getVisibleLogicalRange: () => ({{ from: 0, to: 1000 }}),
        timeToCoordinate: () => 100,
        options: () => ({{ barSpacing: 6 }}),
        width: () => 800
      }}),
      applyOptions: () => {{}},
      addCandlestickSeries: () => {{
        candleSeriesObj = {{
          setData: (candles) => {{ global.lastSetCandles = candles; }},
          setMarkers: (markers) => {{ global.lastSetMarkers = markers; }},
          attachPrimitive: () => {{}},
          data: () => global.lastSetCandles || []
        }};
        global.streamingCandleSeries = candleSeriesObj;
        return candleSeriesObj;
      }},
      addHistogramSeries: () => {{
        volumeSeriesObj = {{
          setData: (volumes) => {{ global.lastSetVolumes = volumes; }}
        }};
        global.streamingVolumeSeries = volumeSeriesObj;
        return volumeSeriesObj;
      }},
      subscribeCrosshairMove: () => {{}}
    }};
    global.mockTvChart = chart;
    global.tvStreamingChart = chart;
    return chart;
  }}
}};

const MONITORED_19 = {json.dumps(MONITORED_19_SYMBOLS)};
const specObj = {{}};
MONITORED_19.forEach(s => {{
  specObj[s] = {{ coverage_pct: 100.0, status: 'healthy', gaps: [] }};
}});

global.mockSpectrogramPayload = {{
  database: 'streaming',
  symbol: 'all',
  view_mode: 'all',
  week_start: '2026-09-28',
  extended_hours: true,
  hours: 'extended',
  available_weeks: [
    {{ week_start: '2026-09-28', week_end: '2026-10-02', label: 'Sep 28 – Oct 02, 2026 (Current)', is_current: true }}
  ],
  days: [
    {{ date: '2026-09-28', day_name: 'Monday', gaps: [] }},
    {{ date: '2026-09-29', day_name: 'Tuesday', gaps: [] }},
    {{ date: '2026-09-30', day_name: 'Wednesday', gaps: [] }},
    {{ date: '2026-10-01', day_name: 'Thursday', gaps: [] }},
    {{ date: '2026-10-02', day_name: 'Friday', gaps: [] }}
  ],
  summary: {{ total_gaps: 0, total_outage_minutes: 0, average_coverage: 100 }},
  spectrogram: specObj
}};

global.mockSingleDayNvdaPayload = {{
  database: 'streaming',
  symbol: 'NVDA',
  view_mode: 'NVDA',
  target_date: '2026-09-28',
  extended_hours: true,
  hours: 'extended',
  days: [{{
    date: '2026-09-28',
    day_name: 'Monday',
    coverage_pct: 55.94,
    status: 'partial',
    buckets: [],
    gaps: [{{
      date: '2026-09-28',
      symbol: 'NVDA',
      start_time: '2026-09-28 04:00:00',
      end_time: '2026-09-28 10:53:00',
      start_epoch: 1790668800,
      end_epoch: 1790693580,
      duration: 413,
      status: 'outage',
      description: '413m gap on NVDA (04:00 - 10:53 ET)'
    }}]
  }}],
  summary: {{ total_gaps: 1, total_outage_minutes: 413, average_coverage: 55.94 }}
}};

global.fetch = async (url) => {{
  global.fetchCalls.push(url);
  if (url.includes('/api/streaming/candles')) {{
    return {{
      ok: true,
      json: async () => ({{
        symbol: global.currentStreamingSymbol || 'NVDA',
        timeframe: '1m',
        session_start_epoch: 1790668800,
        session_end_epoch: 1790726400,
        candles: [
          {{ time: 1790668800, open: 120.0, high: 121.0, low: 119.5, close: 120.5, volume: 100 }}
        ],
        gaps: []
      }})
    }};
  }}
  if (url.includes('/api/streaming/symbols')) {{
    return {{
      ok: true,
      json: async () => ({{
        symbols: MONITORED_19
      }})
    }};
  }}
  if (url.includes('symbol=NVDA') || url.includes('date=2026-09-28')) {{
    return {{
      ok: true,
      json: async () => global.mockSingleDayNvdaPayload
    }};
  }}
  return {{
    ok: true,
    json: async () => global.mockSpectrogramPayload
  }};
}};

{extra_setup_js}

{continuity_js}

{chart_js}

{app_js}

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


class TestStreamingPageLoadSpectrum:
    """Automated test suite verifying 19-Symbol Spectrogram initialization on page load."""

    def test_default_continuity_view_state_is_spectrum(self):
        """
        Asserts that upon script evaluation of continuity.js:
        - currentContinuityView === 'spectrum'
        - window.currentContinuityView === 'spectrum'
        - currentContinuitySymbol === 'all'
        """
        result = run_js_simulation("""
        return {
          currentContinuityView: typeof currentContinuityView !== 'undefined' ? currentContinuityView : null,
          windowCurrentContinuityView: (typeof window !== 'undefined' && window.currentContinuityView !== undefined) ? window.currentContinuityView : null,
          currentContinuitySymbol: typeof currentContinuitySymbol !== 'undefined' ? currentContinuitySymbol : null,
          windowCurrentContinuitySymbol: (typeof window !== 'undefined' && window.currentContinuitySymbol !== undefined) ? window.currentContinuitySymbol : null,
          currentContinuityExtended: typeof currentContinuityExtended !== 'undefined' ? currentContinuityExtended : null
        };
        """, load_app_js=False)

        assert "error" not in result, f"Simulation errored: {result.get('error')}"
        assert result["currentContinuityView"] == "spectrum", (
            f"currentContinuityView must be initialized to 'spectrum', got '{result['currentContinuityView']}'"
        )
        assert result["windowCurrentContinuityView"] == "spectrum", (
            f"window.currentContinuityView must be 'spectrum', got '{result['windowCurrentContinuityView']}'"
        )
        assert result["currentContinuitySymbol"] == "all", (
            f"currentContinuitySymbol must be 'all', got '{result['currentContinuitySymbol']}'"
        )

    def test_page_load_loads_extended_hours_spectrogram(self):
        """
        Verifies that when switchDashboardView('streaming') runs on page load:
        - loadStreamingContinuity is called with 'all' and extended=true (not hardcoded false).
        - The fetch URL for continuity contains symbol=all, extended=true, and hours=extended.
        """
        result = run_js_simulation("""
        const originalLoadContinuity = loadStreamingContinuity;
        const loadContinuityCalls = [];
        loadStreamingContinuity = function(symbol, days, extended, weekStart, targetDate) {
          loadContinuityCalls.push({ symbol, days, extended, weekStart, targetDate });
          return originalLoadContinuity.apply(this, arguments);
        };

        fetchCalls.length = 0;
        switchDashboardView('streaming');
        await new Promise(r => setTimeout(r, 60));

        const continuityFetchCalls = fetchCalls.filter(c => c.includes('/api/streaming/continuity'));

        return {
          loadContinuityCalls,
          continuityFetchCalls
        };
        """)

        assert "error" not in result, f"Simulation errored: {result.get('error')}"
        calls = result.get("loadContinuityCalls", [])
        assert len(calls) > 0, "switchDashboardView('streaming') must call loadStreamingContinuity"

        # Verify initial call for spectrogram is for 'all' symbols with extended=true (or not false)
        initial_call = calls[0]
        assert initial_call["symbol"] == "all", (
            f"Expected initial loadStreamingContinuity symbol to be 'all', got '{initial_call['symbol']}'"
        )
        assert initial_call["extended"] is not False, (
            f"loadStreamingContinuity must NOT be called with extended=false on page load! Got: {initial_call['extended']}"
        )
        assert initial_call["extended"] is True, (
            f"Expected loadStreamingContinuity extended to be true, got: {initial_call['extended']}"
        )

        fetch_urls = result.get("continuityFetchCalls", [])
        assert len(fetch_urls) > 0, "Must trigger fetch to /api/streaming/continuity"
        initial_fetch = fetch_urls[0]
        assert "symbol=all" in initial_fetch, f"Initial continuity fetch must be symbol=all, got: {initial_fetch}"
        assert "extended=true" in initial_fetch, (
            f"Initial continuity fetch must have extended=true (not extended=false), got: {initial_fetch}"
        )
        assert "hours=extended" in initial_fetch, (
            f"Initial continuity fetch must have hours=extended, got: {initial_fetch}"
        )

    def test_load_streaming_chart_does_not_clobber_spectrogram_with_nvda(self):
        """
        Verifies that when loadStreamingChart() runs during page load / initial chart setup:
        - It does NOT trigger a fetch to /api/streaming/continuity with symbol=NVDA.
        - It does NOT clobber #continuity-ribbon-view with single-symbol Master Pulse.
        """
        result = run_js_simulation("""
        // First initialize chart and load spectrogram
        initStreamingChart();
        await loadStreamingContinuity('all', 5, true);
        const specBefore = elements['continuity-ribbon-view'].innerHTML;

        fetchCalls.length = 0;
        await loadStreamingChart();
        await new Promise(r => setTimeout(r, 60));

        const specAfter = elements['continuity-ribbon-view'].innerHTML;
        const nvdaContinuityCalls = fetchCalls.filter(c =>
          c.includes('/api/streaming/continuity') && c.includes('symbol=NVDA')
        );

        return {
          nvdaContinuityCalls,
          specBeforeHasSpectrogram: specBefore.includes('19-Symbol Spectrogram'),
          specAfterHasSpectrogram: specAfter.includes('19-Symbol Spectrogram'),
          specAfterHasMasterPulseClobber: specAfter.includes('Healthy (Continuous)') || specAfter.includes('Partial Degradation')
        };
        """)

        assert "error" not in result, f"Simulation errored: {result.get('error')}"
        assert result["nvdaContinuityCalls"] == [], (
            f"loadStreamingChart() must NOT fetch symbol=NVDA continuity on page load! Found calls: {result['nvdaContinuityCalls']}"
        )
        assert result["specAfterHasMasterPulseClobber"] is False, (
            "loadStreamingChart() must NOT clobber #continuity-ribbon-view with Master Pulse View markup ('Healthy (Continuous)')!"
        )
        assert result["specAfterHasSpectrogram"] is True, (
            "#continuity-ribbon-view must preserve the 19-Symbol Spectrogram after loadStreamingChart() runs"
        )

    def test_ribbon_view_renders_19_symbol_spectrogram_on_page_load(self):
        """
        Verifies that after full page load (switchDashboardView('streaming') + chart init):
        - #continuity-ribbon-view renders the 19-Symbol Spectrogram header and symbol rows.
        - #continuity-ribbon-view does NOT contain renderMasterPulseView markup ('Healthy (Continuous)').
        """
        result = run_js_simulation("""
        switchDashboardView('streaming');
        await new Promise(r => setTimeout(r, 100));

        const ribbonHtml = elements['continuity-ribbon-view'].innerHTML;

        return {
          ribbonHtmlLength: ribbonHtml.length,
          hasSpectrogramHeader: ribbonHtml.includes('19-Symbol Spectrogram'),
          hasNvdaRow: ribbonHtml.includes('NVDA'),
          hasAaplRow: ribbonHtml.includes('AAPL'),
          hasMasterPulseMarkup: ribbonHtml.includes('Healthy (Continuous)') || ribbonHtml.includes('Partial Degradation') || ribbonHtml.includes('Outage / Blackout')
        };
        """)

        assert "error" not in result, f"Simulation errored: {result.get('error')}"
        assert result["hasMasterPulseMarkup"] is False, (
            "#continuity-ribbon-view contains Master Pulse markup ('Healthy (Continuous)') instead of 19-Symbol Spectrogram on page load!"
        )
        assert result["hasSpectrogramHeader"] is True, (
            "Expected #continuity-ribbon-view to contain '19-Symbol Spectrogram' header on page load"
        )
        assert result["hasNvdaRow"] is True, "Expected #continuity-ribbon-view to contain row for NVDA"
        assert result["hasAaplRow"] is True, "Expected #continuity-ribbon-view to contain row for AAPL"

    def test_single_day_drilldown_renders_in_detail_container(self):
        """
        Verifies that clicking a symbol/day (openSymbolDayDetail('NVDA', '2026-09-28')):
        - Renders the single-day continuity ribbon in #detail-continuity-ribbons.
        - Does NOT clobber the cached 19-symbol spectrogram in #continuity-ribbon-view.
        - cachedAllContinuityData remains intact.
        """
        result = run_js_simulation("""
        // 1. Initial page load into spectrogram view
        await loadStreamingContinuity('all', 5, true);
        const specBefore = elements['continuity-ribbon-view'].innerHTML;

        // 2. Drill down into single day NVDA 2026-09-28
        openSymbolDayDetail('NVDA', '2026-09-28');
        await new Promise(r => setTimeout(r, 100));

        const detailHtml = elements['detail-continuity-ribbons'].innerHTML;
        const specAfter = elements['continuity-ribbon-view'].innerHTML;
        const cachedAll = (typeof window !== 'undefined' && window.cachedAllContinuityData)
          ? window.cachedAllContinuityData
          : (typeof cachedAllContinuityData !== 'undefined' ? cachedAllContinuityData : null);

        return {
          detailHtmlLength: detailHtml.length,
          detailHasNvdaGap: detailHtml.includes('413m gap on NVDA'),
          detailHasMonday: detailHtml.includes('Monday') || detailHtml.includes('2026-09-28'),
          specAfterHasSpectrogram: specAfter.includes('19-Symbol Spectrogram'),
          specAfterHasMasterPulseClobber: specAfter.includes('Healthy (Continuous)'),
          cachedAllIsAll: cachedAll ? (cachedAll.view_mode === 'all' || cachedAll.symbol === 'all') : false
        };
        """)

        assert "error" not in result, f"Simulation errored: {result.get('error')}"
        assert result["detailHtmlLength"] > 0, (
            "#detail-continuity-ribbons must NOT be empty after openSymbolDayDetail('NVDA', '2026-09-28')!"
        )
        assert result["detailHasNvdaGap"] is True, (
            "Expected #detail-continuity-ribbons to contain '413m gap on NVDA' for 2026-09-28 single-day view"
        )
        assert result["specAfterHasMasterPulseClobber"] is False, (
            "#continuity-ribbon-view must NOT be clobbered with Master Pulse markup during single-day drilldown!"
        )
        assert result["specAfterHasSpectrogram"] is True, (
            "#continuity-ribbon-view must retain the 19-Symbol Spectrogram during single-day drilldown"
        )
        assert result["cachedAllIsAll"] is True, (
            "cachedAllContinuityData must not be overwritten by single-symbol drilldown payload"
        )
