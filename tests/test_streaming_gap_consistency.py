"""
Automated Test Suite for Streaming Gap Consistency & Alignment.

Verifies:
1. One-minute gaps are detected by get_streaming_candles (fixes `missing_min >= 2` bug in analytics.py).
2. Session boundary gaps (leading gaps from session_start to first candle, trailing gaps from last candle to session_end)
   are captured by get_streaming_candles.
3. Spectrogram in continuity.js renders gap markers proportionally based on actual gap timing without hardcoded
   `width: 25%` or `left: 30%` blocks.
4. Single-day drilldown in chart.js synchronizes hours mode with continuity (requesting `hours=regular` from regular
   spectrogram drilldown instead of unconditionally forcing `&hours=extended`).
5. Gaps reported by get_streaming_continuity_analysis and get_streaming_candles are consistent across ADBE, AMD, and APP
   on 2026-09-15 in regular market hours.
"""
import json
import os
import re
import shutil
import subprocess
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from src.dashboard.analytics import (
    get_streaming_candles,
    get_streaming_continuity_analysis,
)
from tests.support.lake_population import create_lake, publish_minutes

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

def _lake_session(tmp_path, monkeypatch, symbols):
    """A lake holding the test symbols, selected for the analytics functions."""
    lake = create_lake(tmp_path / "lake", symbols=symbols)
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
    monkeypatch.setenv("DATA_DIR", str(lake))
    return lake


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

// DOM elements
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
global.API_BASE = '';
global.currentStreamingSymbol = 'NVDA';
global.currentStreamingTimeframe = '1m';
global.currentStreamingLimit = 10000;
global.showToast = () => {{}};

global.LightweightCharts = {{
  createChart: (container, options) => ({{
    container,
    options,
    timeScale: () => ({{
      setVisibleRange: (range) => global.visibleRangeCalls.push(range),
      fitContent: () => global.fitContentCalls.push(true)
    }}),
    applyOptions: () => {{}},
    addCandlestickSeries: () => ({{
      setData: (candles) => {{ global.lastSetCandles = candles; }},
      setMarkers: (markers) => {{ global.lastSetMarkers = markers; }}
    }}),
    addHistogramSeries: () => ({{
      setData: (volumes) => {{ global.lastSetVolumes = volumes; }}
    }}),
    subscribeCrosshairMove: () => {{}}
  }})
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
          {{ time: 1790668800, open: 120.0, high: 121.0, low: 119.5, close: 120.5, volume: 100 }}
        ],
        gaps: []
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
      available_weeks: [],
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
# 1. test_streaming_candles_detects_one_minute_gaps
# ============================================================================

def test_streaming_candles_detects_one_minute_gaps(tmp_path, monkeypatch):
    """
    Verifies that get_streaming_candles detects 1-minute missing intervals in `gaps`.
    Currently fails because analytics.py:391 has `if missing_min >= 2:`,
    which filters out single-minute gaps (missing_min == 1).
    """
    lake = _lake_session(tmp_path, monkeypatch, ["TEST_SYM"])
    test_date = date(2026, 9, 15)

    # Regular hours 09:30 to 16:00:
    # Build continuous sequence except missing 09:32 (1 min) and missing 11:15 (1 min)
    minutes = []
    # 09:30 to 16:00 is (9, 30) to (15, 59)
    cur = datetime(test_date.year, test_date.month, test_date.day, 9, 30, tzinfo=ET)
    end = datetime(test_date.year, test_date.month, test_date.day, 16, 0, tzinfo=ET)
    while cur < end:
        hh, mm = cur.hour, cur.minute
        # Intentionally skip 09:32 (single minute gap) and 11:15 (single minute gap)
        if not (hh == 9 and mm == 32) and not (hh == 11 and mm == 15):
            minutes.append((hh, mm))
        cur += timedelta(minutes=1)

    publish_minutes(lake, test_date, "TEST_SYM", minutes)

    candles_res = get_streaming_candles(
        "TEST_SYM",
        timeframe="1m",
        date="2026-09-15",
        hours="regular",
    )

    gaps = candles_res.get("gaps", [])
    gap_durations = [g["duration"] for g in gaps]
    gap_starts = [g.get("start_str") for g in gaps]

    # Must contain both 1-minute gaps
    assert 1 in gap_durations, (
        f"get_streaming_candles dropped 1-minute gaps! Gaps found: {gaps}"
    )
    assert "09:32" in gap_starts, (
        f"Missing 1-minute gap at 09:32 not found in gaps: {gaps}"
    )
    assert "11:15" in gap_starts, (
        f"Missing 1-minute gap at 11:15 not found in gaps: {gaps}"
    )


# ============================================================================
# 2. test_streaming_candles_detects_session_boundary_gaps
# ============================================================================

def test_streaming_candles_detects_session_boundary_gaps(tmp_path, monkeypatch):
    """
    Verifies that get_streaming_candles detects leading gaps (session_start to candles[0])
    and trailing gaps (candles[-1] to session_end).
    """
    lake = _lake_session(tmp_path, monkeypatch, ["BOUNDARY_SYM"])
    test_date = date(2026, 9, 15)

    # Session is regular hours: 09:30:00 to 16:00:00 ET.
    # Case: First candle starts 5 minutes late (09:35 ET) -> leading gap of 5 minutes (09:30 - 09:35 ET).
    #       Last candle ends 10 minutes early (15:50 ET) -> trailing gap of 10 minutes (15:50 - 16:00 ET).
    minutes = []
    cur = datetime(test_date.year, test_date.month, test_date.day, 9, 35, tzinfo=ET)
    end = datetime(test_date.year, test_date.month, test_date.day, 15, 50, tzinfo=ET)
    while cur <= end:
        minutes.append((cur.hour, cur.minute))
        cur += timedelta(minutes=1)

    publish_minutes(lake, test_date, "BOUNDARY_SYM", minutes)

    candles_res = get_streaming_candles(
        "BOUNDARY_SYM",
        timeframe="1m",
        date="2026-09-15",
        hours="regular",
    )

    gaps = candles_res.get("gaps", [])
    assert len(gaps) >= 2, (
        f"Expected at least 2 boundary gaps (leading and trailing), but found {len(gaps)}: {gaps}"
    )

    # Check leading gap
    leading_gap = next((g for g in gaps if g.get("start_str") == "09:30"), None)
    assert leading_gap is not None, (
        f"Leading gap starting at 09:30 was not detected! Gaps: {gaps}"
    )
    assert leading_gap["duration"] == 5, (
        f"Leading gap duration should be 5 minutes (09:30 to 09:35), got {leading_gap}"
    )

    # Check trailing gap
    trailing_gap = next(
        (g for g in gaps if g.get("end_str") in ("16:00", "15:59") or g.get("start_str") in ("15:50", "15:51")),
        None
    )
    assert trailing_gap is not None, (
        f"Trailing gap ending at session close (16:00) was not detected! Gaps: {gaps}"
    )
    assert trailing_gap["duration"] in (9, 10), (
        f"Trailing gap duration should be 9-10 minutes, got {trailing_gap}"
    )


# ============================================================================
# 3. test_spectrogram_renders_proportional_gap_markers_no_hardcoded_25pct
# ============================================================================

def test_spectrogram_renders_proportional_gap_markers_no_hardcoded_25pct():
    """
    Verifies that renderSpectrogramView in src/dashboard/static/js/continuity.js:
    1. Does NOT contain hardcoded `width: 25%` or `left: 30%`.
    2. Renders gap markers dynamically and proportionally based on actual gap timing.
    Currently fails because continuity.js:444 & 454 contain hardcoded:
      `<div class=\"absolute inset-y-0 bg-rose-500\" style=\"left: 30%; width: 25%;\"></div>`
    """
    js_path = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "js" / "continuity.js"
    assert js_path.exists(), f"continuity.js not found at {js_path}"
    content = js_path.read_text(encoding="utf-8")

    # Static assertion: hardcoded 25% width and 30% left must be completely removed
    assert "width: 25%" not in content, (
        "continuity.js contains hardcoded 'width: 25%' for gap rendering!"
    )
    assert "left: 30%" not in content, (
        "continuity.js contains hardcoded 'left: 30%' for gap rendering!"
    )

    # Dynamic JS simulation: render a spectrogram with a 1-minute gap and a 15-minute gap
    test_js = """
    const testData = {
      database: 'streaming',
      view_mode: 'all',
      symbol: 'all',
      monitored_symbols_count: 2,
      hours: 'regular',
      extended_hours: false,
      days: [
        {
          date: '2026-09-15',
          day_name: 'Tuesday',
          gaps: [
            { symbol: 'ADBE', start_str: '11:39', end_str: '11:40', duration: 1, duration_minutes: 1 },
            { symbol: 'XYZ', start_str: '13:00', end_str: '13:15', duration: 15, duration_minutes: 15 }
          ]
        }
      ],
      spectrogram: {
        'ADBE': {
          symbol: 'ADBE',
          coverage_pct: 99.74,
          status: 'partial',
          gaps: [{ symbol: 'ADBE', start_str: '11:39', end_str: '11:40', duration: 1 }]
        },
        'XYZ': {
          symbol: 'XYZ',
          coverage_pct: 96.15,
          status: 'partial',
          gaps: [{ symbol: 'XYZ', start_str: '13:00', end_str: '13:15', duration: 15 }]
        }
      }
    };

    renderSpectrogramView(testData);
    const container = document.getElementById('streaming-spectrum-view');
    return {
      renderedHtml: container.innerHTML
    };
    """
    res = run_js_simulation(test_js)
    html = res.get("renderedHtml", "")

    assert "width: 25%" not in html, "Rendered HTML still contains hardcoded 'width: 25%'"
    assert "left: 30%" not in html, "Rendered HTML still contains hardcoded 'left: 30%'"

    # Extract all style attributes for rose gap divs
    gap_styles = re.findall(r'bg-rose-500[^>]*style="([^"]+)"', html)
    assert len(gap_styles) >= 2, (
        f"Expected gap markers rendered for ADBE and XYZ, found styles: {gap_styles}"
    )

    # Verify that width is NOT 25% for 1-minute gap (1 min / 390 min is ~0.26%, not 25%)
    for st in gap_styles:
        assert "25%" not in st, f"Found hardcoded 25% in style: {st}"
        assert "width:" in st and "left:" in st, f"Gap marker must specify dynamic left and width, got: {st}"


# ============================================================================
# 4. test_single_day_drilldown_hours_synchronized_to_regular
# ============================================================================

def test_single_day_drilldown_hours_synchronized_to_regular():
    """
    Verifies that when drilling down to a single day from the regular week spectrogram:
    1. openSymbolDayDetail and loadStreamingChart do NOT force `&hours=extended`.
    2. loadStreamingChart requests regular hours (`hours=regular` or matching active hours),
       so 5.5 hours of empty pre-market whitespace is not prepended to regular hours chart.
    Currently fails because chart.js:487 unconditionally appends `&hours=extended`.
    """
    js_path = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "js" / "chart.js"
    assert js_path.exists(), f"chart.js not found at {js_path}"
    content = js_path.read_text(encoding="utf-8")

    # Static assertion: unconditional appending of `&hours=extended` when targetDate is set must be removed
    assert not re.search(r'fetchUrl\s*\+=\s*`&date=\$\{.*?\}&hours=extended`', content), (
        "chart.js unconditionally appends '&hours=extended' when targetDate is present!"
    )

    # Dynamic JS simulation: trigger openSymbolDayDetail from regular view
    test_js = """
    global.fetchCalls = [];
    global.currentContinuityExtended = false;
    openSymbolDayDetail('ADBE', '2026-09-15');

    // Wait for any async calls to be scheduled
    await new Promise(r => setTimeout(r, 50));

    return {
      fetchCalls: global.fetchCalls
    };
    """
    res = run_js_simulation(test_js)
    fetch_calls = res.get("fetchCalls", [])

    candle_calls = [c for c in fetch_calls if "/api/streaming/candles" in c]
    assert len(candle_calls) > 0, f"Expected streaming candles fetch call, got: {fetch_calls}"

    for c in candle_calls:
        assert "hours=extended" not in c, (
            f"Drilldown from regular spectrogram must NOT request hours=extended! Call: {c}"
        )
        assert "date=2026-09-15" in c, f"Target date not included in candle call: {c}"


# ============================================================================
# 5. test_continuity_and_candles_gap_consistency
# ============================================================================

def test_continuity_and_candles_gap_consistency(tmp_path, monkeypatch):
    """
    Verifies that the gaps detected by get_streaming_continuity_analysis and
    get_streaming_candles match in count, duration, and timestamps for regular market hours.
    Tested on a synthetic session with a known gap pattern.
    """
    lake = _lake_session(tmp_path, monkeypatch, ["CONSISTENCY_SYM"])
    test_date = date(2026, 9, 15)

    # Build ticks with:
    # - Leading gap: 09:30 to 09:33 missing (3 min)
    # - Mid-day 1-minute gap at 12:00
    # - Mid-day 4-minute gap at 14:00 to 14:04
    # - Trailing gap: 15:55 to 16:00 missing (5 min)
    minutes = []
    cur = datetime(test_date.year, test_date.month, test_date.day, 9, 33, tzinfo=ET)
    end = datetime(test_date.year, test_date.month, test_date.day, 15, 55, tzinfo=ET)
    while cur < end:
        hh, mm = cur.hour, cur.minute
        if (hh == 12 and mm == 0) or (hh == 14 and 0 <= mm < 4):
            cur += timedelta(minutes=1)
            continue
        minutes.append((hh, mm))
        cur += timedelta(minutes=1)

    publish_minutes(lake, test_date, "CONSISTENCY_SYM", minutes)

    cont_res = get_streaming_continuity_analysis(
        days=1,
        symbol="CONSISTENCY_SYM",
        target_date="2026-09-15",
        include_extended=False,
    )
    cand_res = get_streaming_candles(
        "CONSISTENCY_SYM",
        timeframe="1m",
        date="2026-09-15",
        hours="regular",
    )

    cont_gaps = cont_res.get("days", [{}])[0].get("gaps", []) if cont_res.get("days") else []
    cand_gaps = cand_res.get("gaps", [])

    assert len(cont_gaps) == len(cand_gaps), (
        f"Gap count mismatch! Continuity found {len(cont_gaps)} gaps, Candles found {len(cand_gaps)} gaps.\n"
        f"Continuity gaps: {cont_gaps}\nCandles gaps: {cand_gaps}"
    )

    for i in range(len(cont_gaps)):
        cg = cont_gaps[i]
        kg = cand_gaps[i]
        assert cg["duration"] == kg["duration"], (
            f"Gap duration mismatch at index {i}: continuity={cg['duration']}, candle={kg['duration']}"
        )
        assert cg["start_str"] == kg["start_str"], (
            f"Gap start mismatch at index {i}: continuity={cg['start_str']}, candle={kg['start_str']}"
        )
