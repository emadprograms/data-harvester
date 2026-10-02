"""
Automated Test Suite for Streaming Extended Hours Toggle & Differential Gap Thresholds.

Verifies:
1. Differential Gap Thresholds Backend:
   - In RTH (09:30-16:00 ET): 1-minute gaps are flagged in both continuity and candles.
   - In Pre-Market (04:00-09:30 ET): 3m gap is ignored (< 5m), while 6m gap is flagged (>= 5m).
   - In After-Hours (16:00-20:00 ET): 4m gap is ignored (< 5m), while 7m gap is flagged (>= 5m).
   - Crossing gaps touching RTH (e.g. 09:28-09:32, 4m duration): flagged because RTH minutes are missing.
   - No first-trade assumption: timer starts at 04:00 AM sharp; ticks starting at 09:30 ET produce a
     330m leading gap and 330 whitespace items on chart.
2. Extended Hours Header Toggle HTML:
   - Checkbox `#continuity-extended-toggle` exists in header of `#streaming-continuity-card`.
   - Toggle is checked by default.
   - `#continuity-hours-subtitle` and `#detail-session-hours-badge` exist in index.html.
3. Frontend JS Global Extended State:
   - `window.currentContinuityExtended === true` by default.
   - `toggleExtendedHours(false)` updates state to false and triggers fetch with extended=false.
   - `toggleExtendedHours(true)` updates state to true and triggers fetch with extended=true.
4. Spectrogram Dynamic Scaling:
   - When Extended ON: scales 04:00-20:00 ET (960 min): 04:00 at 0.00%, 12:00 at 50.00%, 16:00 at 75.00%.
   - When Extended OFF: scales 09:30-16:00 ET (390 min): 09:30 at 0.00%, 12:45 at 50.00%, 16:00 at 100.00%.
5. Single-Day Drill-Down Extended Hours Synchronization:
   - `openSymbolDayDetail` passes `extended=true` and `hours=extended` when toggle is ON.
   - `openSymbolDayDetail` passes `extended=false` and `hours=regular` when toggle is OFF.
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

from src.database.connection import DuckDBClient
from src.database.schema import init_streaming_db
from src.dashboard.analytics import (
    get_streaming_candles,
    get_streaming_continuity_analysis,
)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

HTML_PATH = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "index.html"
JS_DIR = Path(__file__).resolve().parent.parent / "src" / "dashboard" / "static" / "js"


def create_in_memory_streaming_db() -> DuckDBClient:
    """Creates an in-memory DuckDB client initialized with the streaming schema."""
    client = DuckDBClient(":memory:", read_only=False)
    init_streaming_db(client)
    return client


def populate_ticks_for_minutes(
    client: DuckDBClient,
    session_date: date,
    symbol: str,
    minutes_list: list[tuple[int, int]],
    base_price: float = 100.0,
):
    """
    Populates ticks for specific (hour, minute) ET times.
    """
    rows = []
    for hh, mm in minutes_list:
        dt_et = datetime(session_date.year, session_date.month, session_date.day, hh, mm, 0, tzinfo=ET)
        dt_utc = dt_et.astimezone(UTC)
        is_reg = (hh > 9 or (hh == 9 and mm >= 30)) and (hh < 16)
        is_pre = (hh < 9) or (hh == 9 and mm < 30)
        session = "REG" if is_reg else ("PRE" if is_pre else "POST")
        rows.append((
            dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
            symbol,
            base_price,
            10.0,
            base_price - 0.05,
            base_price + 0.05,
            "TEST_SOURCE",
            session,
        ))
    if rows:
        client.executemany(
            "INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def run_js_simulation(test_body_js: str, extra_setup_js: str = "") -> dict:
    """
    Executes a Node.js simulation of frontend continuity and chart logic,
    evaluating src/dashboard/static/js/continuity.js and src/dashboard/static/js/chart.js.
    """
    node_bin = shutil.which("node")
    if not node_bin:
        pytest.skip("Node.js is required to execute frontend JS simulation tests")

    continuity_js_path = JS_DIR / "continuity.js"
    chart_js_path = JS_DIR / "chart.js"

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
elements['continuity-extended-toggle'] = mockElement('continuity-extended-toggle');
elements['continuity-hours-subtitle'] = mockElement('continuity-hours-subtitle');
elements['detail-session-hours-badge'] = mockElement('detail-session-hours-badge');

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
    const isExtended = url.includes('hours=extended');
    // Default epochs for mock: 04:00 ET (1790668800) vs 09:30 ET (1790688600), 20:00 ET (1790726400)
    const sEpoch = isExtended ? 1790668800 : 1790688600;
    const eEpoch = isExtended ? 1790726400 : 1790712000;
    return {{
      ok: true,
      json: async () => ({{
        symbol: global.currentStreamingSymbol || 'NVDA',
        timeframe: '1m',
        database: 'streaming',
        session_start_epoch: sEpoch,
        session_end_epoch: eEpoch,
        candles: [
          {{ time: 1790688600, open: 120.0, high: 121.0, low: 119.5, close: 120.5, volume: 100 }}
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
# 1. TestDifferentialGapThresholdsBackend
# ============================================================================

class TestDifferentialGapThresholdsBackend:
    """
    Verifies the backend differential gap threshold logic:
    - RTH (09:30-16:00 ET): all gaps >= 1m are flagged.
    - Pre-Market (04:00-09:30 ET): gaps < 5m are ignored, gaps >= 5m are flagged.
    - After-Hours (16:00-20:00 ET): gaps < 5m are ignored, gaps >= 5m are flagged.
    - Crossing Gaps (touching 09:30-16:00 ET): flagged if any regular minute is missing.
    - No First Trade Assumption: leading gap from 04:00 to 09:30 is flagged as 330m gap.
    """

    def test_rth_all_one_minute_gaps_flagged(self):
        """
        In regular trading hours (09:30-16:00 ET), all gaps >= 1 minute MUST be flagged
        in both get_streaming_continuity_analysis and get_streaming_candles.
        """
        client = create_in_memory_streaming_db()
        test_date = date(2026, 9, 15)
        sym = "NVDA"

        # Continuous ticks from 09:30 to 15:59 except missing 10:00 (1 minute gap)
        minutes = []
        cur = datetime(test_date.year, test_date.month, test_date.day, 9, 30, tzinfo=ET)
        end = datetime(test_date.year, test_date.month, test_date.day, 16, 0, tzinfo=ET)
        while cur < end:
            hh, mm = cur.hour, cur.minute
            if not (hh == 10 and mm == 0):
                minutes.append((hh, mm))
            cur += timedelta(minutes=1)

        populate_ticks_for_minutes(client, test_date, sym, minutes)

        # 1. Check get_streaming_continuity_analysis (regular hours mode)
        cont_regular = get_streaming_continuity_analysis(
            client=client, target_date="2026-09-15", include_extended=False, symbol=sym
        )
        reg_gaps = cont_regular.get("summary", {}).get("gaps", [])
        gap_1m = [g for g in reg_gaps if g.get("duration") == 1 or g.get("duration_minutes") == 1]
        assert len(gap_1m) == 1, (
            f"Expected 1-minute RTH gap flagged in continuity (regular mode), got gaps: {reg_gaps}"
        )
        assert gap_1m[0].get("start_str") == "10:00"

        # 2. Check get_streaming_candles (regular hours mode)
        candles_regular = get_streaming_candles(
            symbol=sym, date="2026-09-15", hours="regular", client=client
        )
        c_gaps = candles_regular.get("gaps", [])
        c_gap_1m = [g for g in c_gaps if g.get("duration") == 1]
        assert len(c_gap_1m) == 1, (
            f"Expected 1-minute RTH gap flagged in candles (regular mode), got gaps: {c_gaps}"
        )
        assert c_gap_1m[0].get("start_str") == "10:00"

        # 3. Check get_streaming_continuity_analysis (extended hours mode)
        # Even with extended=True, RTH 1-minute gap MUST be flagged
        cont_ext = get_streaming_continuity_analysis(
            client=client, target_date="2026-09-15", include_extended=True, symbol=sym
        )
        ext_gaps = cont_ext.get("summary", {}).get("gaps", [])
        ext_rth_1m = [g for g in ext_gaps if g.get("duration") == 1 or g.get("duration_minutes") == 1]
        assert len(ext_rth_1m) == 1, (
            f"Expected 1-minute RTH gap flagged in continuity (extended mode), got gaps: {ext_gaps}"
        )

        # 4. Check get_streaming_candles (extended hours mode)
        candles_ext = get_streaming_candles(
            symbol=sym, date="2026-09-15", hours="extended", client=client
        )
        ce_gaps = candles_ext.get("gaps", [])
        ce_rth_1m = [g for g in ce_gaps if g.get("duration") == 1]
        assert len(ce_rth_1m) == 1, (
            f"Expected 1-minute RTH gap flagged in candles (extended mode), got gaps: {ce_gaps}"
        )

    def test_extended_pre_market_gaps_differential_threshold(self):
        """
        In pre-market (04:00-09:30 ET), gaps < 5 minutes MUST BE IGNORED.
        Only gaps >= 5 minutes are flagged.
        Tests: 3m gap is ignored, 6m gap is flagged.
        """
        client = create_in_memory_streaming_db()
        test_date = date(2026, 9, 15)
        sym = "NVDA"

        # Construct ticks from 04:00 to 16:00:
        # - 3m gap in pre-market: missing 05:01, 05:02, 05:03 (duration = 3 min) -> must be IGNORED
        # - 6m gap in pre-market: missing 06:01, 06:02, 06:03, 06:04, 06:05, 06:06 (duration = 6 min) -> must be FLAGGED
        # - Continuous otherwise
        minutes = []
        cur = datetime(test_date.year, test_date.month, test_date.day, 4, 0, tzinfo=ET)
        end = datetime(test_date.year, test_date.month, test_date.day, 16, 0, tzinfo=ET)
        while cur < end:
            hh, mm = cur.hour, cur.minute
            is_3m_gap = (hh == 5 and 1 <= mm <= 3)
            is_6m_gap = (hh == 6 and 1 <= mm <= 6)
            if not is_3m_gap and not is_6m_gap:
                minutes.append((hh, mm))
            cur += timedelta(minutes=1)

        populate_ticks_for_minutes(client, test_date, sym, minutes)

        # 1. get_streaming_continuity_analysis (extended=True)
        cont_res = get_streaming_continuity_analysis(
            client=client, target_date="2026-09-15", include_extended=True, symbol=sym
        )
        cont_gaps = cont_res.get("summary", {}).get("gaps", [])
        gap_3m = [g for g in cont_gaps if g.get("duration") == 3 or g.get("duration_minutes") == 3]
        gap_6m = [g for g in cont_gaps if g.get("duration") == 6 or g.get("duration_minutes") == 6]

        assert len(gap_3m) == 0, (
            f"3-minute pre-market gap MUST BE IGNORED (< 5m threshold), but was flagged: {gap_3m}"
        )
        assert len(gap_6m) == 1, (
            f"6-minute pre-market gap MUST BE FLAGGED (>= 5m threshold), but got: {gap_6m}"
        )
        assert gap_6m[0].get("start_str") == "06:01"

        # 2. get_streaming_candles (hours=extended)
        candles_res = get_streaming_candles(
            symbol=sym, date="2026-09-15", hours="extended", client=client
        )
        candle_gaps = candles_res.get("gaps", [])
        c_gap_3m = [g for g in candle_gaps if g.get("duration") == 3]
        c_gap_6m = [g for g in candle_gaps if g.get("duration") == 6]

        assert len(c_gap_3m) == 0, (
            f"3-minute pre-market gap MUST BE IGNORED in candles (< 5m threshold), but got: {c_gap_3m}"
        )
        assert len(c_gap_6m) == 1, (
            f"6-minute pre-market gap MUST BE FLAGGED in candles (>= 5m threshold), but got: {c_gap_6m}"
        )
        assert c_gap_6m[0].get("start_str") == "06:01"

    def test_extended_after_hours_gaps_differential_threshold(self):
        """
        In after-hours (16:00-20:00 ET), gaps < 5 minutes MUST BE IGNORED.
        Only gaps >= 5 minutes are flagged.
        Tests: 4m gap is ignored, 7m gap is flagged.
        """
        client = create_in_memory_streaming_db()
        test_date = date(2026, 9, 15)
        sym = "NVDA"

        # Construct ticks from 09:30 to 20:00:
        # - Continuous in RTH (09:30 to 16:00)
        # - 4m gap in after-hours: missing 16:31, 16:32, 16:33, 16:34 (duration = 4 min) -> must be IGNORED
        # - 7m gap in after-hours: missing 17:31, 17:32, 17:33, 17:34, 17:35, 17:36, 17:37 (duration = 7 min) -> must be FLAGGED
        # - Continuous through 19:59
        minutes = []
        cur = datetime(test_date.year, test_date.month, test_date.day, 9, 30, tzinfo=ET)
        end = datetime(test_date.year, test_date.month, test_date.day, 20, 0, tzinfo=ET)
        while cur < end:
            hh, mm = cur.hour, cur.minute
            is_4m_gap = (hh == 16 and 31 <= mm <= 34)
            is_7m_gap = (hh == 17 and 31 <= mm <= 37)
            if not is_4m_gap and not is_7m_gap:
                minutes.append((hh, mm))
            cur += timedelta(minutes=1)

        populate_ticks_for_minutes(client, test_date, sym, minutes)

        # 1. get_streaming_continuity_analysis (extended=True)
        cont_res = get_streaming_continuity_analysis(
            client=client, target_date="2026-09-15", include_extended=True, symbol=sym
        )
        cont_gaps = cont_res.get("summary", {}).get("gaps", [])
        gap_4m = [g for g in cont_gaps if g.get("duration") == 4 or g.get("duration_minutes") == 4]
        gap_7m = [g for g in cont_gaps if g.get("duration") == 7 or g.get("duration_minutes") == 7]

        assert len(gap_4m) == 0, (
            f"4-minute after-hours gap MUST BE IGNORED (< 5m threshold), but was flagged: {gap_4m}"
        )
        assert len(gap_7m) == 1, (
            f"7-minute after-hours gap MUST BE FLAGGED (>= 5m threshold), but got: {gap_7m}"
        )
        assert gap_7m[0].get("start_str") == "17:31"

        # 2. get_streaming_candles (hours=extended)
        candles_res = get_streaming_candles(
            symbol=sym, date="2026-09-15", hours="extended", client=client
        )
        candle_gaps = candles_res.get("gaps", [])
        c_gap_4m = [g for g in candle_gaps if g.get("duration") == 4]
        c_gap_7m = [g for g in candle_gaps if g.get("duration") == 7]

        assert len(c_gap_4m) == 0, (
            f"4-minute after-hours gap MUST BE IGNORED in candles (< 5m threshold), but got: {c_gap_4m}"
        )
        assert len(c_gap_7m) == 1, (
            f"7-minute after-hours gap MUST BE FLAGGED in candles (>= 5m threshold), but got: {c_gap_7m}"
        )
        assert c_gap_7m[0].get("start_str") == "17:31"

    def test_crossing_gaps_touching_rth_flagged(self):
        """
        Crossing Gaps (touching 09:30-16:00 ET): Flagged if any regular trading minute is missing.
        Tests: a 4m gap starting at 09:28 and ending at 09:32 (missing 09:28, 09:29, 09:30, 09:31)
        touches RTH at 09:30, so despite duration (4m) < 5m, it MUST BE FLAGGED.
        """
        client = create_in_memory_streaming_db()
        test_date = date(2026, 9, 15)
        sym = "NVDA"

        # Continuous ticks from 04:00 to 16:00 except missing 09:28, 09:29, 09:30, 09:31 (duration 4 min)
        minutes = []
        cur = datetime(test_date.year, test_date.month, test_date.day, 4, 0, tzinfo=ET)
        end = datetime(test_date.year, test_date.month, test_date.day, 16, 0, tzinfo=ET)
        while cur < end:
            hh, mm = cur.hour, cur.minute
            is_crossing_gap = (hh == 9 and 28 <= mm <= 31)
            if not is_crossing_gap:
                minutes.append((hh, mm))
            cur += timedelta(minutes=1)

        populate_ticks_for_minutes(client, test_date, sym, minutes)

        # 1. get_streaming_continuity_analysis (extended=True)
        cont_res = get_streaming_continuity_analysis(
            client=client, target_date="2026-09-15", include_extended=True, symbol=sym
        )
        cont_gaps = cont_res.get("summary", {}).get("gaps", [])
        crossing_cont = [g for g in cont_gaps if g.get("start_str") == "09:28" and (g.get("duration") == 4 or g.get("duration_minutes") == 4)]
        assert len(crossing_cont) == 1, (
            f"Crossing gap touching RTH (09:28-09:32, 4m) MUST BE FLAGGED in continuity, got: {cont_gaps}"
        )

        # 2. get_streaming_candles (hours=extended)
        candles_res = get_streaming_candles(
            symbol=sym, date="2026-09-15", hours="extended", client=client
        )
        candle_gaps = candles_res.get("gaps", [])
        crossing_candles = [g for g in candle_gaps if g.get("start_str") == "09:28" and g.get("duration") == 4]
        assert len(crossing_candles) == 1, (
            f"Crossing gap touching RTH (09:28-09:32, 4m) MUST BE FLAGGED in candles, got: {candle_gaps}"
        )

    def test_no_first_trade_assumption_leading_gap(self):
        """
        NO first trade assumptions: timer starts at 04:00 AM sharp.
        If ticks start at 09:30 ET, the 04:00-09:30 interval (330 min) is flagged as a 330m gap
        and returns 330 whitespace items to the chart.
        """
        client = create_in_memory_streaming_db()
        test_date = date(2026, 9, 15)
        sym = "NVDA"

        # Ticks only exist from 09:30 to 16:00 ET (NO pre-market trades at all from 04:00 to 09:29)
        minutes = []
        cur = datetime(test_date.year, test_date.month, test_date.day, 9, 30, tzinfo=ET)
        end = datetime(test_date.year, test_date.month, test_date.day, 16, 0, tzinfo=ET)
        while cur < end:
            minutes.append((cur.hour, cur.minute))
            cur += timedelta(minutes=1)

        populate_ticks_for_minutes(client, test_date, sym, minutes)

        # 1. Backend continuity: leading 04:00 to 09:30 gap is 330 minutes
        cont_res = get_streaming_continuity_analysis(
            client=client, target_date="2026-09-15", include_extended=True, symbol=sym
        )
        cont_gaps = cont_res.get("summary", {}).get("gaps", [])
        leading_cont_gaps = [g for g in cont_gaps if g.get("start_str") == "04:00" and (g.get("duration") == 330 or g.get("duration_minutes") == 330)]
        assert len(leading_cont_gaps) == 1, (
            f"Expected leading gap of 330m from 04:00 in continuity, got: {cont_gaps}"
        )

        # 2. Backend candles: leading 04:00 to 09:30 gap is 330 minutes
        candles_res = get_streaming_candles(
            symbol=sym, date="2026-09-15", hours="extended", client=client
        )
        candle_gaps = candles_res.get("gaps", [])
        leading_candle_gaps = [g for g in candle_gaps if g.get("start_str") == "04:00" and g.get("duration") == 330]
        assert len(leading_candle_gaps) == 1, (
            f"Expected leading gap of 330m from 04:00 in candles, got: {candle_gaps}"
        )

        # 3. Frontend chart: 330 whitespace items generated before first candle
        s_epoch = int(datetime(2026, 9, 15, 4, 0, tzinfo=ET).timestamp())
        open_epoch = int(datetime(2026, 9, 15, 9, 30, tzinfo=ET).timestamp())
        e_epoch = int(datetime(2026, 9, 15, 20, 0, tzinfo=ET).timestamp())

        setup_js = f"""
        global.fetch = async (url) => {{
          global.fetchCalls.push(url);
          if (url.includes('/api/streaming/candles')) {{
            return {{
              ok: true,
              json: async () => ({{
                symbol: 'NVDA',
                timeframe: '1m',
                database: 'streaming',
                session_start_epoch: {s_epoch},
                session_end_epoch: {e_epoch},
                candles: [
                  {{ time: {open_epoch}, open: 120.0, high: 121.0, low: 119.5, close: 120.5, volume: 100 }}
                ],
                gaps: [
                  {{ start_epoch: {s_epoch}, end_epoch: {open_epoch - 60}, duration: 330, start_str: '04:00', end_str: '09:29' }}
                ]
              }})
            }};
          }}
          return {{ ok: true, json: async () => ({{}}) }};
        }};
        """

        test_js = f"""
        initStreamingChart();
        await loadStreamingChart('2026-09-15', 'extended');

        const candles = global.lastSetCandles || [];
        const whitespaceItems = candles.filter(c => c.open === undefined);
        const leadingWhitespace = candles.filter(c => c.open === undefined && c.time < {open_epoch});

        return {{
          totalCandles: candles.length,
          whitespaceCount: whitespaceItems.length,
          leadingWhitespaceCount: leadingWhitespace.length,
          firstCandleWithOHLC: candles.find(c => c.open !== undefined)
        }};
        """
        sim_res = run_js_simulation(test_js, extra_setup_js=setup_js)
        leading_ws = sim_res.get("leadingWhitespaceCount", 0)
        assert leading_ws == 330, (
            f"Expected exactly 330 whitespace items from 04:00 to 09:30, got: {leading_ws} in {sim_res}"
        )


# ============================================================================
# 2. TestExtendedHoursHeaderToggleHtml
# ============================================================================

class TestExtendedHoursHeaderToggleHtml:
    """
    Verifies that the HTML header in src/dashboard/static/index.html includes:
    - Checkbox `#continuity-extended-toggle` inside `#streaming-continuity-card`.
    - Checkbox is checked by default (`checked` attribute).
    - `#continuity-hours-subtitle` and `#detail-session-hours-badge` exist.
    """

    def test_extended_toggle_checkbox_exists_in_continuity_header(self):
        """
        Asserts `#continuity-extended-toggle` checkbox exists within `#streaming-continuity-card`.
        """
        assert HTML_PATH.exists(), f"index.html not found at {HTML_PATH}"
        html = HTML_PATH.read_text(encoding="utf-8")

        # Must have input element with id="continuity-extended-toggle"
        match = re.search(r'<input[^>]*id=["\']continuity-extended-toggle["\'][^>]*>', html) or \
                re.search(r'<input[^>]*type=["\']checkbox["\'][^>]*id=["\']continuity-extended-toggle["\'][^>]*>', html)
        assert match is not None, (
            "index.html must contain <input type='checkbox' id='continuity-extended-toggle'> inside the continuity header"
        )

        # Must be inside #streaming-continuity-card
        card_start = html.find('id="streaming-continuity-card"')
        assert card_start != -1, "#streaming-continuity-card not found in index.html"
        card_sub = html[card_start:card_start + 1500]
        assert "continuity-extended-toggle" in card_sub, (
            "#continuity-extended-toggle must be placed in the header of #streaming-continuity-card"
        )

    def test_extended_toggle_checked_by_default(self):
        """
        Asserts `#continuity-extended-toggle` is checked by default (default state: ON).
        """
        assert HTML_PATH.exists(), f"index.html not found at {HTML_PATH}"
        html = HTML_PATH.read_text(encoding="utf-8")

        match = re.search(r'<input[^>]*id=["\']continuity-extended-toggle["\'][^>]*>', html)
        assert match is not None, "id='continuity-extended-toggle' input not found in index.html"
        tag = match.group(0)
        assert "checked" in tag, (
            f"#continuity-extended-toggle must be checked by default, found tag: {tag}"
        )

    def test_hours_subtitle_and_detail_badge_ids_exist(self):
        """
        Asserts `#continuity-hours-subtitle` and `#detail-session-hours-badge` exist in index.html
        for dynamic session hours display.
        """
        assert HTML_PATH.exists(), f"index.html not found at {HTML_PATH}"
        html = HTML_PATH.read_text(encoding="utf-8")

        assert 'id="continuity-hours-subtitle"' in html, (
            "index.html must contain an element with id='continuity-hours-subtitle' in continuity header"
        )
        assert 'id="detail-session-hours-badge"' in html, (
            "index.html must contain an element with id='detail-session-hours-badge' in detail view header"
        )


# ============================================================================
# 3. TestFrontendJsGlobalExtendedState
# ============================================================================

class TestFrontendJsGlobalExtendedState:
    """
    Verifies global extended state and toggle handler in continuity.js:
    - currentContinuityExtended defaults to true.
    - toggleExtendedHours(false) updates state to false and triggers fetch with extended=false.
    - toggleExtendedHours(true) updates state to true and triggers fetch with extended=true.
    """

    def test_current_continuity_extended_default_true(self):
        """
        Verifies window.currentContinuityExtended defaults to true.
        """
        test_js = """
        return {
          windowExtended: typeof window !== 'undefined' ? window.currentContinuityExtended : null,
          globalExtended: typeof global !== 'undefined' ? global.currentContinuityExtended : null,
          isDefined: typeof currentContinuityExtended !== 'undefined'
        };
        """
        res = run_js_simulation(test_js)
        assert res.get("isDefined") is True, "currentContinuityExtended is not defined in continuity.js"
        assert res.get("windowExtended") is True, (
            f"window.currentContinuityExtended must default to true, got: {res.get('windowExtended')}"
        )
        assert res.get("globalExtended") is True, (
            f"global.currentContinuityExtended must default to true, got: {res.get('globalExtended')}"
        )

    def test_toggle_extended_hours_updates_state_and_triggers_fetch(self):
        """
        Verifies toggleExtendedHours updates global state and triggers fresh fetch with extended param.
        """
        test_js = """
        if (typeof toggleExtendedHours !== 'function') {
          return { error: 'toggleExtendedHours is not a function' };
        }

        global.fetchCalls = [];
        // 1. Toggle OFF
        toggleExtendedHours(false);
        const stateAfterOff = global.currentContinuityExtended;
        const fetchAfterOff = [...global.fetchCalls];

        global.fetchCalls = [];
        // 2. Toggle ON
        toggleExtendedHours(true);
        const stateAfterOn = global.currentContinuityExtended;
        const fetchAfterOn = [...global.fetchCalls];

        return {
          stateAfterOff,
          fetchAfterOff,
          stateAfterOn,
          fetchAfterOn
        };
        """
        res = run_js_simulation(test_js)
        assert "error" not in res, res.get("error")

        # 1. State after turning OFF
        assert res.get("stateAfterOff") is False, (
            f"toggleExtendedHours(false) should set currentContinuityExtended to false, got: {res.get('stateAfterOff')}"
        )
        off_fetches = res.get("fetchAfterOff", [])
        assert any("extended=false" in f for f in off_fetches), (
            f"toggleExtendedHours(false) must trigger fetch with extended=false, calls: {off_fetches}"
        )

        # 2. State after turning ON
        assert res.get("stateAfterOn") is True, (
            f"toggleExtendedHours(true) should set currentContinuityExtended to true, got: {res.get('stateAfterOn')}"
        )
        on_fetches = res.get("fetchAfterOn", [])
        assert any("extended=true" in f for f in on_fetches), (
            f"toggleExtendedHours(true) must trigger fetch with extended=true, calls: {on_fetches}"
        )


# ============================================================================
# 4. TestSpectrogramDynamicScaling
# ============================================================================

class TestSpectrogramDynamicScaling:
    """
    Verifies spectrogram gap marker positioning across session modes:
    - Extended ON: 960 minutes (04:00 to 20:00 ET).
      04:00 at 0.00%, 12:00 at 50.00%, 16:00 at 75.00%.
    - Extended OFF: 390 minutes (09:30 to 16:00 ET).
      09:30 at 0.00%, 12:45 at 50.00%, 16:00 at 100.00%.
    """

    def test_spectrogram_scales_across_960_minutes_when_extended_on(self):
        """
        When Extended Hours is ON, gap markers scale across 960 minutes:
        04:00 -> 0.00%
        12:00 -> 50.00% (480 / 960)
        16:00 -> 75.00% (720 / 960)
        """
        test_js = """
        global.currentContinuityExtended = true;
        if (typeof window !== 'undefined') window.currentContinuityExtended = true;

        const container = mockElement('test-container');
        const mockData = {
          view_mode: 'all',
          days: [
            {
              date: '2026-09-15',
              day_name: 'Tuesday',
              coverage_pct: 90.0,
              status: 'partial',
              gaps: [
                { symbol: 'NVDA', start_str: '04:00', duration: 10, description: '10m gap' },
                { symbol: 'NVDA', start_str: '12:00', duration: 10, description: '10m gap' },
                { symbol: 'NVDA', start_str: '16:00', duration: 10, description: '10m gap' }
              ]
            }
          ],
          spectrogram: {
            'NVDA': {
              symbol: 'NVDA',
              coverage_pct: 90.0,
              status: 'partial',
              gaps: []
            }
          }
        };

        renderSpectrogramView(mockData, container);
        return {
          html: container.innerHTML
        };
        """
        res = run_js_simulation(test_js)
        html = res.get("html", "")
        lefts = re.findall(r'left:\s*([0-9.]+)%', html)
        assert len(lefts) == 3, f"Expected 3 gap markers rendered, found left styles: {lefts} in html:\n{html}"

        left_floats = [float(x) for x in lefts]
        assert abs(left_floats[0] - 0.00) < 0.5, (
            f"04:00 in 960m extended window should be at 0.00%, got: {left_floats[0]}%"
        )
        assert abs(left_floats[1] - 50.00) < 0.5, (
            f"12:00 in 960m extended window should be at 50.00%, got: {left_floats[1]}%"
        )
        assert abs(left_floats[2] - 75.00) < 0.5, (
            f"16:00 in 960m extended window should be at 75.00%, got: {left_floats[2]}%"
        )

    def test_spectrogram_scales_across_390_minutes_when_extended_off(self):
        """
        When Extended Hours is OFF, gap markers scale across 390 minutes:
        09:30 -> 0.00%
        12:45 -> 50.00% (195 / 390)
        16:00 -> 100.00% (390 / 390)
        """
        test_js = """
        global.currentContinuityExtended = false;
        if (typeof window !== 'undefined') window.currentContinuityExtended = false;

        const container = mockElement('test-container');
        const mockData = {
          view_mode: 'all',
          days: [
            {
              date: '2026-09-15',
              day_name: 'Tuesday',
              coverage_pct: 90.0,
              status: 'partial',
              gaps: [
                { symbol: 'NVDA', start_str: '09:30', duration: 5, description: '5m gap' },
                { symbol: 'NVDA', start_str: '12:45', duration: 5, description: '5m gap' },
                { symbol: 'NVDA', start_str: '16:00', duration: 5, description: '5m gap' }
              ]
            }
          ],
          spectrogram: {
            'NVDA': {
              symbol: 'NVDA',
              coverage_pct: 90.0,
              status: 'partial',
              gaps: []
            }
          }
        };

        renderSpectrogramView(mockData, container);
        return {
          html: container.innerHTML
        };
        """
        res = run_js_simulation(test_js)
        html = res.get("html", "")
        lefts = re.findall(r'left:\s*([0-9.]+)%', html)
        assert len(lefts) == 3, f"Expected 3 gap markers rendered, found left styles: {lefts} in html:\n{html}"

        left_floats = [float(x) for x in lefts]
        assert abs(left_floats[0] - 0.00) < 0.5, (
            f"09:30 in 390m regular window should be at 0.00%, got: {left_floats[0]}%"
        )
        assert abs(left_floats[1] - 50.00) < 0.5, (
            f"12:45 in 390m regular window should be at 50.00%, got: {left_floats[1]}%"
        )
        assert abs(left_floats[2] - 100.00) < 0.5, (
            f"16:00 in 390m regular window should be at 100.00%, got: {left_floats[2]}%"
        )


# ============================================================================
# 5. TestSingleDayDrilldownExtendedHoursSync
# ============================================================================

class TestSingleDayDrilldownExtendedHoursSync:
    """
    Verifies that openSymbolDayDetail synchronizes with the global extended hours state:
    - When toggle is ON: passes extended=true to continuity and hours=extended to chart.
    - When toggle is OFF: passes extended=false to continuity and hours=regular to chart.
    """

    def test_open_symbol_day_detail_passes_extended_when_toggle_on(self):
        """
        When currentContinuityExtended is true, openSymbolDayDetail requests extended data:
        - continuity: extended=true
        - chart: hours=extended
        """
        test_js = """
        global.currentContinuityExtended = true;
        if (typeof window !== 'undefined') window.currentContinuityExtended = true;

        global.fetchCalls = [];
        openSymbolDayDetail('NVDA', '2026-09-15');

        await new Promise(r => setTimeout(r, 50));

        return {
          fetchCalls: global.fetchCalls
        };
        """
        res = run_js_simulation(test_js)
        fetch_calls = res.get("fetchCalls", [])

        # 1. Continuity fetch
        cont_calls = [c for c in fetch_calls if "/api/streaming/continuity" in c]
        assert len(cont_calls) > 0, f"Expected continuity fetch, got calls: {fetch_calls}"
        assert any("extended=true" in c for c in cont_calls), (
            f"Drilldown with toggle ON must pass extended=true to continuity! Calls: {cont_calls}"
        )
        assert any("date=2026-09-15" in c for c in cont_calls), (
            f"Drilldown must include date=2026-09-15! Calls: {cont_calls}"
        )

        # 2. Candle fetch
        candle_calls = [c for c in fetch_calls if "/api/streaming/candles" in c]
        assert len(candle_calls) > 0, f"Expected candle fetch, got calls: {fetch_calls}"
        assert any("hours=extended" in c for c in candle_calls), (
            f"Drilldown with toggle ON must pass hours=extended to candles! Calls: {candle_calls}"
        )
        assert any("date=2026-09-15" in c for c in candle_calls), (
            f"Candle fetch must include date=2026-09-15! Calls: {candle_calls}"
        )

    def test_open_symbol_day_detail_passes_regular_when_toggle_off(self):
        """
        When currentContinuityExtended is false, openSymbolDayDetail requests regular data:
        - continuity: extended=false
        - chart: hours=regular
        """
        test_js = """
        global.currentContinuityExtended = false;
        if (typeof window !== 'undefined') window.currentContinuityExtended = false;

        global.fetchCalls = [];
        openSymbolDayDetail('NVDA', '2026-09-15');

        await new Promise(r => setTimeout(r, 50));

        return {
          fetchCalls: global.fetchCalls
        };
        """
        res = run_js_simulation(test_js)
        fetch_calls = res.get("fetchCalls", [])

        # 1. Continuity fetch
        cont_calls = [c for c in fetch_calls if "/api/streaming/continuity" in c]
        assert len(cont_calls) > 0, f"Expected continuity fetch, got calls: {fetch_calls}"
        assert any("extended=false" in c for c in cont_calls), (
            f"Drilldown with toggle OFF must pass extended=false to continuity! Calls: {cont_calls}"
        )
        assert any("date=2026-09-15" in c for c in cont_calls), (
            f"Drilldown must include date=2026-09-15! Calls: {cont_calls}"
        )

        # 2. Candle fetch
        candle_calls = [c for c in fetch_calls if "/api/streaming/candles" in c]
        assert len(candle_calls) > 0, f"Expected candle fetch, got calls: {fetch_calls}"
        assert any("hours=regular" in c for c in candle_calls), (
            f"Drilldown with toggle OFF must pass hours=regular to candles! Calls: {candle_calls}"
        )
        assert not any("hours=extended" in c for c in candle_calls), (
            f"Drilldown with toggle OFF must NOT pass hours=extended to candles! Calls: {candle_calls}"
        )
