"""Phase 46 DASH-02 — the dashboard is a single Parquet-only view.

The Historical Dashboard (navigation, chart container, source selector, storage
stat) is removed; only the streaming/lake view remains.

Written before the implementation (TDD): every test here is expected to fail
until the frontend is reduced to one view.
"""
from __future__ import annotations

import re
from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "src" / "dashboard" / "static"

REMOVED_UI = (
    "nav-historical",
    "view-historical",
    "Historical Dashboard",
    "legend-db-badge",
    "historical.duckdb",
)

KEPT_UI = (
    'id="view-streaming"',
    "nav-streaming",
    "streaming-chart-container",
)


def _html() -> str:
    return (STATIC / "index.html").read_text(encoding="utf-8")


def test_historical_dashboard_markup_is_removed() -> None:
    html = _html()
    for token in REMOVED_UI:
        assert token not in html, f"historical UI remains in index.html: {token}"


def test_streaming_view_survives_and_is_visible_by_default() -> None:
    html = _html()
    for token in KEPT_UI:
        assert token in html, f"missing surviving UI element: {token}"

    match = re.search(r'<section id="view-streaming"[^>]*>', html)
    assert match, "streaming view section not found"
    assert "hidden" not in match.group(0), "the single view must not start hidden"


def test_state_module_has_no_historical_source() -> None:
    js = (STATIC / "js" / "state.js").read_text(encoding="utf-8")
    assert "'historical'" not in js
    assert '"historical"' not in js


def test_no_static_module_calls_a_historical_endpoint() -> None:
    offenders = []
    for js_file in sorted((STATIC / "js").glob("*.js")):
        text = js_file.read_text(encoding="utf-8")
        if "/api/historical" in text:
            offenders.append(js_file.name)
    assert not offenders, f"static modules still call historical endpoints: {offenders}"
