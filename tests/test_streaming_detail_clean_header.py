"""
Automated Test Suite for Clean Streaming Detail Header and Badge Removal.

Covers User Directive:
"- Daily Continuity & Candlestick Chart
Extended Hours (04:00–20:00 ET) remvoe this."

Verifies:
1. Static HTML (src/dashboard/static/index.html):
   - id="detail-session-hours-badge" exists for backwards-compatibility with existing tests.
   - detail-session-hours-badge tag has class="hidden" to hide the badge visually.
   - "Daily Continuity & Candlestick Chart" does NOT appear anywhere in index.html.
   - "NVDA - Extended Session Continuity & Candlestick Chart" does NOT appear in index.html.
   - id="streaming-detail-symbol-title" exists.
2. Frontend JS - Day-Specific Detail View (src/dashboard/static/js/chart.js):
   - openSymbolDayDetail('NVDA', '2026-09-28') sets streaming-detail-symbol-title to 'NVDA • 2026-09-28'.
   - openSymbolDayDetail('NVDA', null) sets streaming-detail-symbol-title to 'NVDA • Session'.
   - Title text does NOT contain 'Daily Continuity & Candlestick Chart'.
3. Frontend JS - Symbol Drilldown View (src/dashboard/static/js/chart.js):
   - openSymbolDetail('TSLA') sets streaming-detail-symbol-title to 'TSLA'.
   - Title text does NOT contain 'Continuity & Candlestick Chart' or 'Extended Session Continuity'.
4. Frontend JS - Master Pulse Legend Bar (src/dashboard/static/js/continuity.js):
   - renderMasterPulseView does NOT render 'Extended Hours (04:00–20:00 ET)' or 'Regular Market Hours (09:30–16:00 ET)'.
   - renderMasterPulseView maintains 'Healthy (Continuous)', 'Partial Degradation', and 'Outage / Blackout'.
5. Frontend JS - Extended Hours Toggle (src/dashboard/static/js/continuity.js):
   - toggleExtendedHours(false) and toggleExtendedHours(true) ensure detail-session-hours-badge remains hidden.
"""

import json
import os
from pathlib import Path
import re
import shutil
import subprocess

from bs4 import BeautifulSoup
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
HTML_PATH = REPO_ROOT / "src" / "dashboard" / "static" / "index.html"
JS_DIR = REPO_ROOT / "src" / "dashboard" / "static" / "js"


def get_html_content() -> str:
    """Reads index.html content."""
    assert HTML_PATH.exists(), f"index.html not found at {HTML_PATH}"
    return HTML_PATH.read_text(encoding="utf-8")


def get_soup() -> BeautifulSoup:
    """Parses index.html with BeautifulSoup."""
    return BeautifulSoup(get_html_content(), "html.parser")


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
  let text = '';
  return {{
    id,
    className: initialClasses.join(' '),
    innerHTML: '',
    get innerText() {{ return text !== '' ? text : (this.innerHTML ? this.innerHTML.replace(/<[^>]*>/g, '') : ''); }},
    set innerText(v) {{ text = String(v); }},
    get textContent() {{ return text !== '' ? text : (this.innerHTML ? this.innerHTML.replace(/<[^>]*>/g, '') : ''); }},
    set textContent(v) {{ text = String(v); }},
    style: {{}},
    value: '',
    options: [],
    checked: true,
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

// Initialize DOM mock nodes
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
elements['continuity-extended-toggle'] = mockElement('continuity-extended-toggle');
elements['continuity-hours-subtitle'] = mockElement('continuity-hours-subtitle');
elements['detail-session-hours-badge'] = mockElement('detail-session-hours-badge');
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
global.currentContinuitySymbol = 'NVDA';
global.currentContinuityDays = 1;
global.currentContinuityExtended = true;
global.showToast = () => {{}};

global.LightweightCharts = {{
  CrosshairMode: {{ Normal: 0, Magnet: 1 }},
  createChart: (container, options) => ({{
    container,
    options,
    timeScale: () => ({{
      setVisibleRange: () => {{}},
      fitContent: () => {{}},
      getVisibleLogicalRange: () => ({{ from: 0, to: 1000 }}),
      timeToCoordinate: () => 100,
      options: () => ({{ barSpacing: 6 }}),
      width: () => 800
    }}),
    applyOptions: () => {{}},
    addCandlestickSeries: () => ({{
      setData: () => {{}},
      setMarkers: () => {{}},
      attachPrimitive: () => {{}},
      data: () => []
    }}),
    addHistogramSeries: () => ({{
      setData: () => {{}}
    }}),
    subscribeCrosshairMove: () => {{}}
  }})
}};

global.fetch = async (url) => {{
  global.fetchCalls.push(url);
  return {{
    ok: true,
    json: async () => ({{
      symbol: global.currentStreamingSymbol || 'NVDA',
      days: []
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
# 1. Test Static index.html Detail Header
# ============================================================================

def test_static_index_html_detail_header():
    """
    Validates src/dashboard/static/index.html detail view header:
    - #detail-session-hours-badge exists with class 'hidden'.
    - 'Daily Continuity & Candlestick Chart' is not present in index.html.
    - 'NVDA - Extended Session Continuity & Candlestick Chart' is not present in index.html.
    - #streaming-detail-symbol-title exists.
    """
    html = get_html_content()
    soup = get_soup()

    # 1. detail-session-hours-badge element must exist
    badge_el = soup.find(id="detail-session-hours-badge")
    assert badge_el is not None, "id='detail-session-hours-badge' must exist in index.html for compatibility"

    # 2. detail-session-hours-badge must have 'hidden' class to visually remove it
    badge_classes = badge_el.get("class", [])
    assert "hidden" in badge_classes, (
        f"#detail-session-hours-badge must have class 'hidden' to remove visual clutter, found classes: {badge_classes}"
    )

    # 3. 'Daily Continuity & Candlestick Chart' must NOT be in index.html
    assert "Daily Continuity & Candlestick Chart" not in html, (
        "'Daily Continuity & Candlestick Chart' must be removed from index.html"
    )

    # 4. 'NVDA - Extended Session Continuity & Candlestick Chart' (or &amp;) must NOT be in index.html
    assert "NVDA - Extended Session Continuity" not in html, (
        "'NVDA - Extended Session Continuity & Candlestick Chart' must be removed from index.html"
    )

    # 5. streaming-detail-symbol-title must exist
    title_el = soup.find(id="streaming-detail-symbol-title")
    assert title_el is not None, "id='streaming-detail-symbol-title' must exist in index.html"


# ============================================================================
# 2. Test Open Symbol Day Detail Title Format (chart.js)
# ============================================================================

def test_open_symbol_day_detail_title_format():
    """
    Verifies openSymbolDayDetail in chart.js sets a clean title:
    - openSymbolDayDetail('NVDA', '2026-09-28') -> 'NVDA • 2026-09-28'
    - openSymbolDayDetail('NVDA', null) -> 'NVDA • Session'
    - 'Daily Continuity & Candlestick Chart' is omitted.
    """
    test_js = """
    if (typeof openSymbolDayDetail !== 'function') {
      return { error: 'openSymbolDayDetail is not defined' };
    }

    // Call with specific date
    openSymbolDayDetail('NVDA', '2026-09-28');
    const titleWithDate = elements['streaming-detail-symbol-title'].innerText;

    // Call with null date
    openSymbolDayDetail('NVDA', null);
    const titleWithNullDate = elements['streaming-detail-symbol-title'].innerText;

    return {
      titleWithDate,
      titleWithNullDate
    };
    """
    res = run_js_simulation(test_js)
    assert "error" not in res, f"JS execution error: {res.get('error')}"

    # Verify title with specific date
    title_with_date = res.get("titleWithDate", "")
    assert title_with_date == "NVDA • 2026-09-28", (
        f"Expected title 'NVDA • 2026-09-28', got: '{title_with_date}'"
    )
    assert "Daily Continuity & Candlestick Chart" not in title_with_date, (
        f"'Daily Continuity & Candlestick Chart' must be removed from day detail title, got: '{title_with_date}'"
    )

    # Verify title with null date
    title_with_null = res.get("titleWithNullDate", "")
    assert title_with_null == "NVDA • Session", (
        f"Expected title 'NVDA • Session', got: '{title_with_null}'"
    )
    assert "Daily Continuity & Candlestick Chart" not in title_with_null, (
        f"'Daily Continuity & Candlestick Chart' must be removed from day detail title, got: '{title_with_null}'"
    )


# ============================================================================
# 3. Test Open Symbol Detail Title Format (chart.js)
# ============================================================================

def test_open_symbol_detail_title_format():
    """
    Verifies openSymbolDetail in chart.js sets clean symbol title:
    - openSymbolDetail('TSLA') -> 'TSLA'
    - 'Continuity & Candlestick Chart' is omitted.
    """
    test_js = """
    if (typeof openSymbolDetail !== 'function') {
      return { error: 'openSymbolDetail is not defined' };
    }

    openSymbolDetail('TSLA');
    const title = elements['streaming-detail-symbol-title'].innerText;

    return { title };
    """
    res = run_js_simulation(test_js)
    assert "error" not in res, f"JS execution error: {res.get('error')}"

    title = res.get("title", "")
    assert title == "TSLA", (
        f"Expected clean symbol title 'TSLA', got: '{title}'"
    )
    assert "Continuity & Candlestick Chart" not in title, (
        f"'Continuity & Candlestick Chart' must be removed from symbol detail title, got: '{title}'"
    )
    assert "Extended Session Continuity" not in title, (
        f"'Extended Session Continuity' must be removed from symbol detail title, got: '{title}'"
    )


# ============================================================================
# 4. Test Render Master Pulse Legend No Hours Label (continuity.js)
# ============================================================================

def test_render_master_pulse_legend_no_hours_label():
    """
    Verifies renderMasterPulseView in continuity.js:
    - Does NOT contain 'Extended Hours (04:00–20:00 ET)'
    - Does NOT contain 'Regular Market Hours (09:30–16:00 ET)'
    - Still contains status legend indicators: 'Healthy (Continuous)', 'Partial Degradation', 'Outage / Blackout'
    """
    test_js = """
    const fnRender = typeof renderMasterPulseView === 'function' ? renderMasterPulseView : window.renderMasterPulseView;
    if (!fnRender) {
      return { error: 'renderMasterPulseView is not defined' };
    }

    const singleDayDataExtended = {
      days: [{
        day_name: 'Monday',
        date: '2026-09-28',
        status: 'healthy',
        coverage_pct: 100,
        gaps: []
      }],
      extended_hours: true,
      target_date: '2026-09-28'
    };

    const containerExt = mockElement('detail-continuity-ribbons');
    fnRender(singleDayDataExtended, containerExt);
    const htmlExtended = containerExt.innerHTML;

    const singleDayDataRegular = {
      days: [{
        day_name: 'Monday',
        date: '2026-09-28',
        status: 'healthy',
        coverage_pct: 100,
        gaps: []
      }],
      extended_hours: false,
      hours: 'regular',
      target_date: '2026-09-28'
    };

    const containerReg = mockElement('detail-continuity-ribbons-reg');
    fnRender(singleDayDataRegular, containerReg);
    const htmlRegular = containerReg.innerHTML;

    return {
      htmlExtended,
      htmlRegular
    };
    """
    res = run_js_simulation(test_js)
    assert "error" not in res, f"JS execution error: {res.get('error')}"

    html_extended = res.get("htmlExtended", "")
    html_regular = res.get("htmlRegular", "")

    # Extended hours check
    assert "Extended Hours (04:00–20:00 ET)" not in html_extended, (
        f"Master pulse legend bar must not contain 'Extended Hours (04:00–20:00 ET)', found in: {html_extended}"
    )
    assert "Regular Market Hours (09:30–16:00 ET)" not in html_extended, (
        f"Master pulse legend bar must not contain 'Regular Market Hours', found in: {html_extended}"
    )

    # Regular hours check
    assert "Extended Hours (04:00–20:00 ET)" not in html_regular, (
        f"Master pulse legend bar must not contain 'Extended Hours (04:00–20:00 ET)', found in: {html_regular}"
    )
    assert "Regular Market Hours (09:30–16:00 ET)" not in html_regular, (
        f"Master pulse legend bar must not contain 'Regular Market Hours', found in: {html_regular}"
    )

    # Status indicators must still be present
    for status_label in ["Healthy (Continuous)", "Partial Degradation", "Outage / Blackout"]:
        assert status_label in html_extended, (
            f"Master pulse legend bar must still contain '{status_label}' in extended view"
        )
        assert status_label in html_regular, (
            f"Master pulse legend bar must still contain '{status_label}' in regular view"
        )


# ============================================================================
# 5. Test Toggle Keeps Detail Badge Hidden (continuity.js)
# ============================================================================

def test_toggle_keeps_detail_badge_hidden():
    """
    Verifies toggleExtendedHours in continuity.js ensures detail-session-hours-badge
    consistently has the 'hidden' class applied when toggling extended hours on/off.
    """
    test_js = """
    const fnToggle = typeof toggleExtendedHours === 'function' ? toggleExtendedHours : window.toggleExtendedHours;
    if (!fnToggle) {
      return { error: 'toggleExtendedHours is not defined' };
    }

    const badge = elements['detail-session-hours-badge'];

    // 1. Ensure hidden is enforced when toggle to false
    badge.classList.remove('hidden');
    fnToggle(false);
    const hiddenAfterToggleFalse = badge.classList.contains('hidden');

    // 2. Ensure hidden is enforced when toggle to true
    badge.classList.remove('hidden');
    fnToggle(true);
    const hiddenAfterToggleTrue = badge.classList.contains('hidden');

    return {
      hiddenAfterToggleFalse,
      hiddenAfterToggleTrue
    };
    """
    res = run_js_simulation(test_js)
    assert "error" not in res, f"JS execution error: {res.get('error')}"

    assert res.get("hiddenAfterToggleFalse") is True, (
        "toggleExtendedHours(false) must ensure detail-session-hours-badge has class 'hidden'"
    )
    assert res.get("hiddenAfterToggleTrue") is True, (
        "toggleExtendedHours(true) must ensure detail-session-hours-badge has class 'hidden'"
    )
