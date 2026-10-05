"""
Automated Integration and Unit Tests for Streaming Symbols Management.

User Request:
"now ONLY for streaming.db symbols and its symbols. add another tab in the streaming section
of the dashboard that allows me to add or remove the symbols from being monitored. If any symbols
is added. It should be tracked. If any symbol is removed. then do not leave leftovers in the database.
clean it up. and delete everything related to it in the database. the symbols I see in that table.
There should be data only for those symbols in the database."

Test Coverage:
1. REST API Endpoints:
   - POST /api/streaming/symbols and POST /api/symbols?source=streaming
     - Registers the symbol in the lake registry
     - Triggers streamer reload signal file (.stream_reload.signal)
     - Returns {"success": True}
   - DELETE /api/streaming/symbols/<symbol> and DELETE /api/symbols/<symbol>?source=streaming
     - Fences the subscription (status PENDING_PURGE, active false) in the lake registry
     - Triggers streamer reload signal file (.stream_reload.signal)
     - Returns {"success": True}; tick partitions are dropped by the compaction purge
   - GET /api/streaming/symbols and GET /api/symbols?source=streaming
     - Returns list of streaming symbols
2. Dashboard HTML Structure (src/dashboard/static/index.html):
   - #view-streaming contains sub-tab button for Monitored Symbols (#streaming-tab-btn-symbols)
   - #view-streaming contains corresponding tab panel (#streaming-tab-symbols or #stream-tab-symbols)
   - Panel contains Add Symbol interface:
     - Symbol input field (#add-streaming-symbol-input)
     - Capital ticker input field (#add-streaming-capital-input)
     - Add button (#add-streaming-symbol-btn)
   - Panel contains Monitored Symbols table:
     - Table element (#streaming-symbols-table)
     - Table body (#streaming-symbols-table-body)
3. Frontend JS Logic (src/dashboard/static/js/):
   - Check app.js or tables.js defines functions to:
     - Render/load streaming symbols table (loadStreamingSymbolsTable or renderStreamingSymbolsTable)
     - Add streaming symbol (addStreamingSymbol or handleAddStreamingSymbol)
     - Remove/delete streaming symbol (deleteStreamingSymbol or handleDeleteStreamingSymbol)
     - Refresh streaming symbol select dropdown (#streaming-symbol-select) when modified
"""
import glob
import os
import re
import socket
import threading
import time
from datetime import date
import pytest
import requests
from bs4 import BeautifulSoup

def _reload_signal_path():
    """Path of the lake's streamer reload signal (the disk-database fallback is gone)."""
    from src.storage.config import resolve_tick_lake_root
    from src.storage.registry import SymbolRegistry

    return SymbolRegistry(root=resolve_tick_lake_root()).signal_path


from src.dashboard.server import create_dashboard_server
from tests.support.lake_population import create_lake, publish_minutes


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
# 3. REST API Endpoints: Streaming Symbols Management
# ============================================================================

class TestRESTAPIStreamingSymbols:
    """
    Tests REST API endpoints for streaming symbols:
    - GET /api/streaming/symbols
    - GET /api/symbols?source=streaming
    - POST /api/streaming/symbols
    - POST /api/symbols?source=streaming
    - DELETE /api/streaming/symbols/<symbol>
    - DELETE /api/symbols/<symbol>?source=streaming
    Verifies HTTP response, database updates, tick purging, and reload signal file triggering.
    """

    def test_get_streaming_symbols_dedicated_endpoint(self, api_test_server):
        """GET /api/streaming/symbols returns streaming symbols."""
        resp = requests.get(f"{api_test_server}/api/streaming/symbols")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("database") == "streaming"
        assert "symbols" in data
        assert isinstance(data["symbols"], list)

    def test_get_streaming_symbols_source_param(self, api_test_server):
        """GET /api/symbols?source=streaming returns streaming symbols."""
        resp = requests.get(f"{api_test_server}/api/symbols?source=streaming")
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("database") == "streaming"
        assert "symbols" in data
        assert isinstance(data["symbols"], list)

    def test_post_streaming_symbols_dedicated_endpoint(self, api_test_server):
        """
        POST /api/streaming/symbols:
        - Adds symbol to streaming_database_symbols
        - Triggers streamer reload signal file (.stream_reload.signal)
        - Returns {"success": True}
        """
        test_sym = "TEST_API_STRM_01"
        try:
            # Check or reset signal file
            before_ts = time.time() - 0.5
            resp = requests.post(
                f"{api_test_server}/api/streaming/symbols",
                json={"display_name": test_sym, "capital_ticker": test_sym},
            )
            assert resp.status_code in (200, 201), resp.text
            data = resp.json()
            assert data.get("success") is True
            assert data.get("database") == "streaming"

            # Check symbol is in GET
            get_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
            names = [s.get("display_name") for s in get_resp.json().get("symbols", [])]
            assert test_sym in names

            # Check signal file triggered
            assert os.path.exists(_reload_signal_path()), "Streamer reload signal file must be created/touched on symbol add"
            assert os.path.getmtime(_reload_signal_path()) >= before_ts
        finally:
            requests.delete(f"{api_test_server}/api/streaming/symbols/{test_sym}")

    def test_post_symbols_source_streaming(self, api_test_server):
        """
        POST /api/symbols?source=streaming:
        - Adds symbol to streaming_database_symbols
        - Triggers streamer reload signal file
        - Returns {"success": True}
        """
        test_sym = "TEST_API_SRC_01"
        try:
            before_ts = time.time() - 0.5
            resp = requests.post(
                f"{api_test_server}/api/symbols?source=streaming",
                json={"display_name": test_sym, "capital_ticker": test_sym},
            )
            assert resp.status_code in (200, 201), resp.text
            data = resp.json()
            assert data.get("success") is True

            # Verify in streaming inventory
            get_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
            names = [s.get("display_name") for s in get_resp.json().get("symbols", [])]
            assert test_sym in names

            assert os.path.exists(_reload_signal_path())
            assert os.path.getmtime(_reload_signal_path()) >= before_ts
        finally:
            requests.delete(f"{api_test_server}/api/symbols/{test_sym}?source=streaming")

    def test_delete_streaming_symbol_fences_and_triggers_signal(self, api_test_server):
        """
        DELETE /api/streaming/symbols/<symbol>:
        - Fences the subscription and marks the symbol PENDING_PURGE
        - Triggers the streamer reload signal file
        - Returns {"success": True}

        The tick partitions themselves are removed by the compaction purge that
        follows the fence (see tests/storage/test_compaction.py), not by the
        HTTP request.
        """
        test_sym = "TEST_API_DEL_PURGE"
        try:
            add_resp = requests.post(
                f"{api_test_server}/api/streaming/symbols",
                json={"display_name": test_sym, "capital_ticker": test_sym},
            )
            assert add_resp.status_code in (200, 201), add_resp.text

            before_ts = time.time() - 0.5
            del_resp = requests.delete(f"{api_test_server}/api/streaming/symbols/{test_sym}")
            assert del_resp.status_code == 200, del_resp.text
            del_data = del_resp.json()
            assert del_data.get("success") is True
            assert del_data.get("status") == "PENDING_PURGE"

            entries = {
                s["display_name"]: s
                for s in requests.get(f"{api_test_server}/api/streaming/symbols").json()["symbols"]
            }
            assert entries[test_sym]["status"] == "PENDING_PURGE"
            assert entries[test_sym]["active"] is False

            assert os.path.exists(_reload_signal_path())
            assert os.path.getmtime(_reload_signal_path()) >= before_ts
        finally:
            requests.delete(f"{api_test_server}/api/streaming/symbols/{test_sym}")

    def test_delete_symbols_source_streaming_fences_and_signals(self, api_test_server):
        """
        DELETE /api/symbols/<symbol>?source=streaming:
        - Fences the subscription and marks the symbol PENDING_PURGE
        - Triggers the streamer reload signal file
        - Returns {"success": True}
        """
        test_sym = "TEST_API_DEL_SRC"
        try:
            add_resp = requests.post(
                f"{api_test_server}/api/symbols?source=streaming",
                json={"display_name": test_sym, "capital_ticker": test_sym},
            )
            assert add_resp.status_code in (200, 201), add_resp.text

            before_ts = time.time() - 0.5
            del_resp = requests.delete(f"{api_test_server}/api/symbols/{test_sym}?source=streaming")
            assert del_resp.status_code == 200, del_resp.text
            del_data = del_resp.json()
            assert del_data.get("success") is True
            assert del_data.get("status") == "PENDING_PURGE"

            entries = {
                s["display_name"]: s
                for s in requests.get(f"{api_test_server}/api/symbols?source=streaming").json()["symbols"]
            }
            assert entries[test_sym]["active"] is False

            assert os.path.exists(_reload_signal_path())
            assert os.path.getmtime(_reload_signal_path()) >= before_ts
        finally:
            requests.delete(f"{api_test_server}/api/symbols/{test_sym}?source=streaming")


    def test_api_delete_symbol_keeps_other_symbols_data(self, api_test_server, tmp_path, monkeypatch):
        """Fencing one symbol never touches another symbol's ticks in the lake."""
        lake = create_lake(
            tmp_path / "lake",
            symbols=["TEST_API_KEEP", "TEST_API_DROP"],
        )
        monkeypatch.setenv("TICK_LAKE_ROOT", str(lake))
        monkeypatch.setenv("DATA_DIR", str(lake))
        publish_minutes(lake, date(2026, 10, 1), "TEST_API_KEEP", minutes_list=[(11, 2)])
        publish_minutes(lake, date(2026, 10, 1), "TEST_API_DROP", minutes_list=[(11, 2)])

        del_resp = requests.delete(f"{api_test_server}/api/streaming/symbols/TEST_API_DROP")
        assert del_resp.status_code == 200, del_resp.text
        assert del_resp.json().get("status") == "PENDING_PURGE"

        from src.storage.registry import SymbolRegistry
        from src.storage.reader import TickLakeReader

        registry = SymbolRegistry(root=lake)
        assert registry.get_symbol("TEST_API_DROP").status == "PENDING_PURGE"

        # The kept symbol's data is untouched by the fence
        reader = TickLakeReader(root=lake)
        ticks = reader.query_ticks(symbol="TEST_API_KEEP")
        assert len(ticks) == 1, "Other symbol tick data must be preserved"
        assert ticks[0]["symbol"] == "TEST_API_KEEP"


class TestDashboardHTMLStreamingSymbolsStructure:
    """
    Tests index.html for the required UI components in #view-streaming:
    - Sub-tab button for Monitored Symbols (#streaming-tab-btn-symbols)
    - Sub-tab panel for Monitored Symbols (#streaming-tab-symbols or #stream-tab-symbols)
    - Add Symbol interface:
      - Symbol input field (#add-streaming-symbol-input)
      - Capital ticker input field (#add-streaming-capital-input)
      - Add button (#add-streaming-symbol-btn)
    - Monitored Symbols table:
      - Table element (#streaming-symbols-table)
      - Table body (#streaming-symbols-table-body)
    """

    def test_streaming_view_contains_monitored_symbols_sub_tab_button(self, html_soup):
        """
        #view-streaming must contain a sub-tab navigation button for Monitored Symbols
        (e.g. #streaming-tab-btn-symbols or button with text matching /symbol/i).
        """
        strm_view = html_soup.select_one("#view-streaming")
        assert strm_view is not None, "#view-streaming container must exist"

        # Check for sub-tab button by id or text
        tab_btn = (
            strm_view.select_one("#streaming-tab-btn-symbols, #streaming-tab-btn-symbol, #stream-tab-btn-symbols")
            or strm_view.find(lambda el: el.name == "button" and "symbol" in el.get_text().lower())
        )
        assert tab_btn is not None, (
            "Sub-tab button for Monitored Symbols (#streaming-tab-btn-symbols or button with 'Symbol') "
            "must exist in #view-streaming navigation."
        )

    def test_streaming_view_contains_monitored_symbols_tab_panel(self, html_soup):
        """
        #view-streaming must contain the corresponding tab panel for Monitored Symbols
        (e.g. #streaming-tab-symbols or #stream-tab-symbols).
        """
        strm_view = html_soup.select_one("#view-streaming")
        assert strm_view is not None, "#view-streaming container must exist"

        tab_panel = strm_view.select_one("#streaming-tab-symbols, #stream-tab-symbols, #streaming-symbols-panel")
        assert tab_panel is not None, (
            "Tab panel for Monitored Symbols (#streaming-tab-symbols or #stream-tab-symbols) "
            "must exist inside #view-streaming."
        )

    def test_streaming_symbols_panel_contains_add_symbol_interface(self, html_soup):
        """
        The Monitored Symbols panel must contain an Add Symbol interface with:
        - Symbol input field (#add-streaming-symbol-input)
        - Capital ticker input field (#add-streaming-capital-input)
        - Add button (#add-streaming-symbol-btn)
        """
        sym_input = html_soup.select_one("#add-streaming-symbol-input, #streaming-symbol-input, #input-streaming-symbol")
        assert sym_input is not None, (
            "Add Symbol interface must contain a Symbol input field (#add-streaming-symbol-input)."
        )

        cap_input = html_soup.select_one("#add-streaming-capital-input, #streaming-capital-input, #input-streaming-capital")
        assert cap_input is not None, (
            "Add Symbol interface must contain a Capital ticker input field (#add-streaming-capital-input)."
        )

        add_btn = html_soup.select_one("#add-streaming-symbol-btn, #streaming-symbol-add-btn, #btn-add-streaming-symbol")
        assert add_btn is not None, (
            "Add Symbol interface must contain an Add button (#add-streaming-symbol-btn)."
        )

    def test_streaming_symbols_panel_contains_table_and_body(self, html_soup):
        """
        The Monitored Symbols panel must contain a Monitored Symbols table:
        - Table element (#streaming-symbols-table)
        - Table body (#streaming-symbols-table-body)
        """
        table_el = html_soup.select_one("#streaming-symbols-table, #table-streaming-symbols")
        assert table_el is not None, (
            "Monitored Symbols panel must contain a table element (#streaming-symbols-table)."
        )

        tbody_el = html_soup.select_one("#streaming-symbols-table-body, #streaming-symbols-body")
        assert tbody_el is not None, (
            "Monitored Symbols table must contain a tbody element (#streaming-symbols-table-body)."
        )


# ============================================================================
# 3. Frontend JS Logic: Streaming Symbols Management
# ============================================================================

class TestFrontendJSStreamingSymbolsLogic:
    """
    Tests frontend JavaScript modules (src/dashboard/static/js/) to verify:
    - Function to render/load the streaming symbols table (loadStreamingSymbolsTable / renderStreamingSymbolsTable)
    - Function to add a streaming symbol (addStreamingSymbol / handleAddStreamingSymbol)
    - Function to remove/delete a streaming symbol (deleteStreamingSymbol / removeStreamingSymbol / handleDeleteStreamingSymbol)
    - Refreshing the streaming symbol select dropdown (#streaming-symbol-select) when symbols are modified
    """

    def test_js_defines_streaming_symbols_table_loader(self, js_sources):
        """JS must define a function to render or load the streaming symbols table."""
        combined_js = "\n".join(js_sources.values())
        has_table_loader = bool(re.search(
            r"\b(loadStreamingSymbolsTable|renderStreamingSymbolsTable|fetchStreamingSymbolsTable|loadStreamingSymbols|renderStreamingSymbols)\b",
            combined_js,
        ))
        assert has_table_loader, (
            "Frontend JS must define a function to load or render the streaming symbols table "
            "(e.g. loadStreamingSymbolsTable or renderStreamingSymbolsTable in tables.js or app.js)."
        )

    def test_js_defines_add_streaming_symbol_handler(self, js_sources):
        """JS must define a function to add a streaming symbol."""
        combined_js = "\n".join(js_sources.values())
        has_add_func = bool(re.search(
            r"\b(addStreamingSymbol|handleAddStreamingSymbol|submitStreamingSymbol|saveStreamingSymbol)\b",
            combined_js,
        ))
        assert has_add_func, (
            "Frontend JS must define a function to add a streaming symbol "
            "(e.g. addStreamingSymbol or handleAddStreamingSymbol)."
        )

    def test_js_defines_delete_streaming_symbol_handler(self, js_sources):
        """JS must define a function to remove/delete a streaming symbol."""
        combined_js = "\n".join(js_sources.values())
        has_delete_func = bool(re.search(
            r"\b(deleteStreamingSymbol|removeStreamingSymbol|handleDeleteStreamingSymbol|removeStreamingSymbolFromDb)\b",
            combined_js,
        ))
        assert has_delete_func, (
            "Frontend JS must define a function to delete/remove a streaming symbol "
            "(e.g. deleteStreamingSymbol or handleDeleteStreamingSymbol)."
        )

    def test_js_refreshes_streaming_symbol_select_dropdown(self, js_sources):
        """Modifying streaming symbols must trigger a refresh of #streaming-symbol-select or call fetchStreamingSymbols."""
        combined_js = "\n".join(js_sources.values())
        # Check that streaming symbol mutation or loading references #streaming-symbol-select or fetchStreamingSymbols
        has_refresh_call = bool(re.search(
            r"(?:addStreamingSymbol|deleteStreamingSymbol|handleDeleteStreamingSymbol|handleAddStreamingSymbol|loadStreamingSymbols)[\s\S]{0,500}?(?:fetchStreamingSymbols|streaming-symbol-select)",
            combined_js,
        )) or bool(re.search(
            r"fetchStreamingSymbols\(\)",
            combined_js,
        ))
        assert has_refresh_call, (
            "Modifying streaming symbols must trigger a refresh of #streaming-symbol-select (e.g. calling fetchStreamingSymbols)."
        )
