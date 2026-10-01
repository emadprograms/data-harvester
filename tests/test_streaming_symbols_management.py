"""
Automated Integration and Unit Tests for Streaming Symbols Management.

User Request:
"now ONLY for streaming.db symbols and its symbols. add another tab in the streaming section
of the dashboard that allows me to add or remove the symbols from being monitored. If any symbols
is added. It should be tracked. If any symbol is removed. then do not leave leftovers in the database.
clean it up. and delete everything related to it in the database. the symbols I see in that table.
There should be data only for those symbols in the database."

Test Coverage:
1. Backend Database Operation Purity:
   - remove_streaming_symbol_from_db(display_name):
     - Must delete symbol from streaming_database_symbols
     - MUST delete all records for that symbol from tick_data (and ticks / streaming_ticks views)
     - Must leave NO leftovers in tick_data for that symbol
     - Must NOT delete tick records for other symbols
     - Handles symbols with 0 ticks cleanly
     - Handles non-existent symbols gracefully
   - add_streaming_symbol_to_db(display_name, capital_ticker=..., databento_ticker=...):
     - Adds or updates the symbol in streaming_database_symbols
2. Database Invariant Purity:
   - Invariant assertion:
     SELECT COUNT(*) FROM tick_data WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols) == 0
   - Holds after symbol additions, tick ingestions, and symbol removals
   - Successfully flags any orphan or untracked ticks
   - Live streaming database must adhere to strict invariant
3. REST API Endpoints:
   - POST /api/streaming/symbols and POST /api/symbols?source=streaming
     - Adds symbol to streaming_database_symbols
     - Triggers streamer reload signal file (.stream_reload.signal)
     - Returns {"success": True}
   - DELETE /api/streaming/symbols/<symbol> and DELETE /api/symbols/<symbol>?source=streaming
     - Deletes symbol from streaming_database_symbols
     - Completely purges all tick data for that symbol from tick_data
     - Triggers streamer reload signal file (.stream_reload.signal)
     - Returns {"success": True}
   - GET /api/streaming/symbols and GET /api/symbols?source=streaming
     - Returns list of streaming symbols
4. Dashboard HTML Structure (src/dashboard/static/index.html):
   - #view-streaming contains sub-tab button for Monitored Symbols (#streaming-tab-btn-symbols)
   - #view-streaming contains corresponding tab panel (#streaming-tab-symbols or #stream-tab-symbols)
   - Panel contains Add Symbol interface:
     - Symbol input field (#add-streaming-symbol-input)
     - Capital ticker input field (#add-streaming-capital-input)
     - Add button (#add-streaming-symbol-btn)
   - Panel contains Monitored Symbols table:
     - Table element (#streaming-symbols-table)
     - Table body (#streaming-symbols-table-body)
5. Frontend JS Logic (src/dashboard/static/js/):
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
from datetime import datetime, timezone
import pytest
import requests
from bs4 import BeautifulSoup

from src.database.connection import (
    DuckDBClient,
    get_streaming_db_connection,
    DEFAULT_DATA_DIR,
    DEFAULT_STREAMING_DB_PATH,
)
from src.database.schema import init_streaming_db
from src.database.operations import (
    add_streaming_symbol_to_db,
    remove_streaming_symbol_from_db,
    get_streaming_database_symbols_from_db,
    get_streaming_symbol_inventory_list,
)
from src.dashboard.server import create_dashboard_server, RELOAD_SIGNAL_FILE


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


@pytest.fixture
def mem_streaming_db():
    """Provides an isolated in-memory DuckDB database with streaming schema initialized."""
    client = DuckDBClient(":memory:", read_only=False)
    init_streaming_db(client)
    yield client
    if client.conn:
        try:
            client.conn.close()
        except Exception:
            pass


# ============================================================================
# 1. Backend Database Operation Purity
# ============================================================================

class TestBackendDatabaseOperationPurity:
    """
    Tests backend database operations for managing streaming symbols:
    - add_streaming_symbol_to_db
    - remove_streaming_symbol_from_db
    Enforces that symbol removal completely purges all corresponding tick records
    from tick_data and views without leaving any leftovers or affecting other symbols.
    """

    def test_add_streaming_symbol_inserts_and_updates(self, mem_streaming_db):
        """add_streaming_symbol_to_db adds or updates symbol in streaming_database_symbols."""
        ok = add_streaming_symbol_to_db(
            display_name="TEST_ADD_01",
            capital_ticker="TEST_CAP_01",
            databento_ticker="TEST_DBN_01",
            binance_ticker="TEST_BIN_01",
            client=mem_streaming_db,
        )
        assert ok is True

        res = mem_streaming_db.execute(
            "SELECT display_name, capital_ticker, databento_ticker, binance_ticker FROM streaming_database_symbols WHERE display_name = 'TEST_ADD_01'"
        ).fetchall()
        assert len(res) == 1
        assert res[0] == ("TEST_ADD_01", "TEST_CAP_01", "TEST_DBN_01", "TEST_BIN_01")

        # Test updating the existing symbol
        ok_upd = add_streaming_symbol_to_db(
            display_name="TEST_ADD_01",
            capital_ticker="UPDATED_CAP",
            databento_ticker="UPDATED_DBN",
            client=mem_streaming_db,
        )
        assert ok_upd is True

        res_upd = mem_streaming_db.execute(
            "SELECT display_name, capital_ticker, databento_ticker FROM streaming_database_symbols WHERE display_name = 'TEST_ADD_01'"
        ).fetchall()
        assert len(res_upd) == 1
        assert res_upd[0] == ("TEST_ADD_01", "UPDATED_CAP", "UPDATED_DBN")

    def test_remove_streaming_symbol_deletes_from_symbols_table(self, mem_streaming_db):
        """remove_streaming_symbol_from_db removes the symbol from streaming_database_symbols."""
        add_streaming_symbol_to_db("SYM_TO_REMOVE", capital_ticker="SYM_TO_REMOVE", client=mem_streaming_db)
        exists_before = mem_streaming_db.execute(
            "SELECT count(*) FROM streaming_database_symbols WHERE display_name = 'SYM_TO_REMOVE'"
        ).fetchone()[0]
        assert exists_before == 1

        removed = remove_streaming_symbol_from_db("SYM_TO_REMOVE", client=mem_streaming_db)
        assert removed is True

        exists_after = mem_streaming_db.execute(
            "SELECT count(*) FROM streaming_database_symbols WHERE display_name = 'SYM_TO_REMOVE'"
        ).fetchone()[0]
        assert exists_after == 0

    def test_remove_streaming_symbol_purges_all_ticks_from_tick_data(self, mem_streaming_db):
        """
        CRITICAL PURITY REQUIREMENT:
        remove_streaming_symbol_from_db must purge all tick data for that symbol from tick_data.
        Must leave NO leftovers in tick_data (SELECT COUNT(*) WHERE symbol = ? must be 0).
        """
        sym = "PURGE_TICKS_SYM"
        add_streaming_symbol_to_db(sym, capital_ticker=sym, client=mem_streaming_db)

        # Seed tick records for this symbol
        mem_streaming_db.execute("""
            INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
            ('2026-10-01 10:00:00', 'PURGE_TICKS_SYM', 100.5, 10.0, 100.4, 100.6, 'CAPITAL', 'REG'),
            ('2026-10-01 10:00:01', 'PURGE_TICKS_SYM', 100.6, 20.0, 100.5, 100.7, 'CAPITAL', 'REG'),
            ('2026-10-01 10:00:02', 'PURGE_TICKS_SYM', 100.7, 30.0, 100.6, 100.8, 'CAPITAL', 'REG'),
            ('2026-10-01 10:00:03', 'PURGE_TICKS_SYM', 100.8, 15.0, 100.7, 100.9, 'CAPITAL', 'REG'),
            ('2026-10-01 10:00:04', 'PURGE_TICKS_SYM', 100.9, 25.0, 100.8, 101.0, 'CAPITAL', 'REG')
        """)

        count_before = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [sym]
        ).fetchone()[0]
        assert count_before == 5, f"Expected 5 ticks before removal, got {count_before}"

        # Remove the symbol
        removed = remove_streaming_symbol_from_db(sym, client=mem_streaming_db)
        assert removed is True

        # Verify symbol is deleted from symbol registry
        sym_count = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM streaming_database_symbols WHERE display_name = ?", [sym]
        ).fetchone()[0]
        assert sym_count == 0, "Symbol registry must not contain deleted symbol"

        # Verify tick_data has NO leftovers for this symbol
        count_after = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [sym]
        ).fetchone()[0]
        assert count_after == 0, (
            f"LEFTOVER DATA VIOLATION: Expected 0 ticks in tick_data for {sym} after removal, but found {count_after}. "
            "remove_streaming_symbol_from_db MUST delete all tick_data records for the removed symbol."
        )

    def test_remove_streaming_symbol_purges_ticks_views(self, mem_streaming_db):
        """
        Purging tick_data must also reflect in views (ticks and streaming_ticks).
        """
        sym = "VIEW_PURGE_SYM"
        add_streaming_symbol_to_db(sym, capital_ticker=sym, client=mem_streaming_db)

        mem_streaming_db.execute("""
            INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
            ('2026-10-01 10:01:00', 'VIEW_PURGE_SYM', 50.0, 1.0, 49.9, 50.1, 'CAPITAL', 'REG'),
            ('2026-10-01 10:01:05', 'VIEW_PURGE_SYM', 50.2, 2.0, 50.1, 50.3, 'CAPITAL', 'REG')
        """)

        # Remove symbol
        remove_streaming_symbol_from_db(sym, client=mem_streaming_db)

        # Check views
        ticks_view_count = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM ticks WHERE symbol = ?", [sym]
        ).fetchone()[0]
        assert ticks_view_count == 0, "ticks view must contain 0 rows for removed symbol"

        streaming_ticks_view_count = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM streaming_ticks WHERE symbol = ?", [sym]
        ).fetchone()[0]
        assert streaming_ticks_view_count == 0, "streaming_ticks view must contain 0 rows for removed symbol"

    def test_remove_streaming_symbol_preserves_other_symbols_ticks(self, mem_streaming_db):
        """
        Deleting one symbol must NEVER delete or corrupt tick data for other monitored symbols.
        """
        sym_keep = "KEEP_SYM"
        sym_drop = "DROP_SYM"
        add_streaming_symbol_to_db(sym_keep, capital_ticker=sym_keep, client=mem_streaming_db)
        add_streaming_symbol_to_db(sym_drop, capital_ticker=sym_drop, client=mem_streaming_db)

        # Insert ticks for both
        mem_streaming_db.execute("""
            INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
            ('2026-10-01 10:02:00', 'KEEP_SYM', 200.0, 10.0, 199.9, 200.1, 'CAPITAL', 'REG'),
            ('2026-10-01 10:02:01', 'KEEP_SYM', 200.5, 15.0, 200.4, 200.6, 'CAPITAL', 'REG'),
            ('2026-10-01 10:02:02', 'KEEP_SYM', 201.0, 20.0, 200.9, 201.1, 'CAPITAL', 'REG'),
            ('2026-10-01 10:02:00', 'DROP_SYM', 300.0, 5.0, 299.8, 300.2, 'CAPITAL', 'REG'),
            ('2026-10-01 10:02:05', 'DROP_SYM', 301.0, 8.0, 300.8, 301.2, 'CAPITAL', 'REG')
        """)

        # Remove DROP_SYM
        remove_streaming_symbol_from_db(sym_drop, client=mem_streaming_db)

        # DROP_SYM must be purged
        drop_ticks = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [sym_drop]
        ).fetchone()[0]
        assert drop_ticks == 0, "DROP_SYM ticks must be purged"

        # KEEP_SYM must remain completely intact
        keep_ticks = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [sym_keep]
        ).fetchone()[0]
        assert keep_ticks == 3, f"Expected 3 ticks preserved for KEEP_SYM, got {keep_ticks}"

        keep_sym_registered = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM streaming_database_symbols WHERE display_name = ?", [sym_keep]
        ).fetchone()[0]
        assert keep_sym_registered == 1, "KEEP_SYM must remain registered in streaming_database_symbols"

    def test_remove_streaming_symbol_with_zero_ticks_succeeds(self, mem_streaming_db):
        """Removing a registered symbol that has no tick data succeeds without error."""
        sym = "NO_TICKS_SYM"
        add_streaming_symbol_to_db(sym, client=mem_streaming_db)
        ok = remove_streaming_symbol_from_db(sym, client=mem_streaming_db)
        assert ok is True

        count = mem_streaming_db.execute(
            "SELECT COUNT(*) FROM streaming_database_symbols WHERE display_name = ?", [sym]
        ).fetchone()[0]
        assert count == 0

    def test_remove_nonexistent_symbol_handles_gracefully(self, mem_streaming_db):
        """Removing a symbol that does not exist handles gracefully and does not raise exceptions."""
        result = remove_streaming_symbol_from_db("TOTALLY_NONEXISTENT_XYZ", client=mem_streaming_db)
        # Should complete without crashing (returns True or False)
        assert isinstance(result, bool)


# ============================================================================
# 2. Database Invariant Purity
# ============================================================================

class TestDatabaseInvariantPurity:
    """
    Enforces the core user requirement:
    "the symbols I see in that table. There should be data only for those symbols in the database."

    Invariant:
    SELECT COUNT(*) FROM tick_data WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols) == 0
    """

    def test_database_invariant_holds_on_monitored_ticks(self, mem_streaming_db):
        """When ticks are recorded only for registered symbols, invariant count is 0."""
        add_streaming_symbol_to_db("ALPHA", client=mem_streaming_db)
        add_streaming_symbol_to_db("BETA", client=mem_streaming_db)

        mem_streaming_db.execute("""
            INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
            ('2026-10-01 10:03:00', 'ALPHA', 10.0, 1.0, 9.9, 10.1, 'CAPITAL', 'REG'),
            ('2026-10-01 10:03:01', 'BETA', 20.0, 2.0, 19.9, 20.1, 'CAPITAL', 'REG')
        """)

        untracked_count = mem_streaming_db.execute("""
            SELECT COUNT(*) FROM tick_data
            WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols)
        """).fetchone()[0]
        assert untracked_count == 0, f"Expected 0 untracked ticks, got {untracked_count}"

    def test_database_invariant_holds_after_symbol_removal(self, mem_streaming_db):
        """
        After removing a symbol, invariant MUST strictly hold (count == 0).
        If any leftover ticks remain for the deleted symbol, invariant is violated.
        """
        add_streaming_symbol_to_db("DELTA", client=mem_streaming_db)
        add_streaming_symbol_to_db("GAMMA", client=mem_streaming_db)

        mem_streaming_db.execute("""
            INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
            ('2026-10-01 10:04:00', 'DELTA', 100.0, 10.0, 99.9, 100.1, 'CAPITAL', 'REG'),
            ('2026-10-01 10:04:01', 'GAMMA', 200.0, 20.0, 199.9, 200.1, 'CAPITAL', 'REG')
        """)

        # Remove DELTA
        remove_streaming_symbol_from_db("DELTA", client=mem_streaming_db)

        # Invariant check: MUST BE ZERO
        untracked = mem_streaming_db.execute("""
            SELECT COUNT(*) FROM tick_data
            WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols)
        """).fetchone()[0]

        assert untracked == 0, (
            f"INVARIANT VIOLATION: Found {untracked} tick records in tick_data whose symbol is NOT in "
            "streaming_database_symbols. When DELTA was removed, all its tick_data rows should have been purged."
        )

    def test_database_invariant_detects_untracked_leftover_ticks(self, mem_streaming_db):
        """Demonstrates that the invariant query accurately detects untracked leftover ticks."""
        mem_streaming_db.execute("""
            INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
            ('2026-10-01 10:05:00', 'GHOST_SYMBOL', 999.0, 1.0, 998.0, 1000.0, 'CAPITAL', 'REG')
        """)

        untracked = mem_streaming_db.execute("""
            SELECT COUNT(*) FROM tick_data
            WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols)
        """).fetchone()[0]

        assert untracked > 0, "Invariant query must detect untracked symbol in tick_data"

    def test_live_streaming_database_has_no_orphan_ticks(self):
        """
        INVARIANT TEST ON LIVE STREAMING DATABASE:
        The active data/streaming.duckdb must contain data ONLY for symbols present in streaming_database_symbols.
        Any untracked ticks (such as leftover test records) must be completely cleaned up.
        """
        client = get_streaming_db_connection(read_only=True)
        try:
            res = client.execute("""
                SELECT COUNT(*) FROM tick_data
                WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols)
            """).fetchone()
            orphan_count = res[0] if res else 0

            if orphan_count > 0:
                orphans = [row[0] for row in client.execute("""
                    SELECT DISTINCT symbol FROM tick_data
                    WHERE symbol NOT IN (SELECT display_name FROM streaming_database_symbols)
                """).fetchall()]
                assert orphan_count == 0, (
                    f"INVARIANT VIOLATION: Active streaming.duckdb contains {orphan_count} tick records "
                    f"for unmonitored symbol(s): {orphans}. Database must contain data ONLY for monitored symbols."
                )
        finally:
            client.close()


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
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("success") is True
            assert data.get("database") == "streaming"

            # Check symbol is in GET
            get_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
            names = [s.get("display_name") for s in get_resp.json().get("symbols", [])]
            assert test_sym in names

            # Check signal file triggered
            assert os.path.exists(RELOAD_SIGNAL_FILE), "Streamer reload signal file must be created/touched on symbol add"
            assert os.path.getmtime(RELOAD_SIGNAL_FILE) >= before_ts
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
            assert resp.status_code == 200
            data = resp.json()
            assert data.get("success") is True

            # Verify in streaming inventory
            get_resp = requests.get(f"{api_test_server}/api/streaming/symbols")
            names = [s.get("display_name") for s in get_resp.json().get("symbols", [])]
            assert test_sym in names

            assert os.path.exists(RELOAD_SIGNAL_FILE)
            assert os.path.getmtime(RELOAD_SIGNAL_FILE) >= before_ts
        finally:
            requests.delete(f"{api_test_server}/api/symbols/{test_sym}?source=streaming")

    def test_delete_streaming_symbol_purges_ticks_and_triggers_signal(self, api_test_server):
        """
        DELETE /api/streaming/symbols/<symbol>:
        - Deletes symbol from streaming_database_symbols
        - Completely purges all tick data for that symbol from tick_data
        - Triggers streamer reload signal file (.stream_reload.signal)
        - Returns {"success": True}
        """
        test_sym = "TEST_API_DEL_PURGE"
        db_client = get_streaming_db_connection(read_only=False)
        try:
            # 1. Add symbol and insert tick data
            add_streaming_symbol_to_db(test_sym, capital_ticker=test_sym, client=db_client)
            db_client.execute(f"""
                INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
                ('2026-10-01 11:00:00', '{test_sym}', 150.0, 10.0, 149.9, 150.1, 'CAPITAL', 'REG'),
                ('2026-10-01 11:00:05', '{test_sym}', 150.5, 12.0, 150.4, 150.6, 'CAPITAL', 'REG')
            """)
            ticks_before = db_client.execute(
                "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [test_sym]
            ).fetchone()[0]
            assert ticks_before == 2

            # 2. Issue DELETE request
            before_ts = time.time() - 0.5
            del_resp = requests.delete(f"{api_test_server}/api/streaming/symbols/{test_sym}")
            assert del_resp.status_code == 200
            del_data = del_resp.json()
            assert del_data.get("success") is True

            # 3. Verify symbol is removed from streaming registry
            sym_count = db_client.execute(
                "SELECT COUNT(*) FROM streaming_database_symbols WHERE display_name = ?", [test_sym]
            ).fetchone()[0]
            assert sym_count == 0, "Symbol must be deleted from streaming_database_symbols"

            # 4. CRITICAL: Verify all ticks are purged from tick_data
            ticks_after = db_client.execute(
                "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [test_sym]
            ).fetchone()[0]
            assert ticks_after == 0, (
                f"LEFTOVER DATA VIOLATION: DELETE /api/streaming/symbols/{test_sym} left {ticks_after} records in tick_data. "
                "Deleting a streaming symbol MUST completely clean up and purge its tick data."
            )

            # 5. Verify reload signal triggered
            assert os.path.exists(RELOAD_SIGNAL_FILE)
            assert os.path.getmtime(RELOAD_SIGNAL_FILE) >= before_ts
        finally:
            # Clean up in case of failure
            try:
                db_client.execute("DELETE FROM streaming_database_symbols WHERE display_name = ?", [test_sym])
                db_client.execute("DELETE FROM tick_data WHERE symbol = ?", [test_sym])
            except Exception:
                pass
            db_client.close()

    def test_delete_symbols_source_streaming_purges_ticks_and_signals(self, api_test_server):
        """
        DELETE /api/symbols/<symbol>?source=streaming:
        - Deletes symbol from streaming_database_symbols
        - Completely purges all tick data for that symbol from tick_data
        - Triggers streamer reload signal file (.stream_reload.signal)
        - Returns {"success": True}
        """
        test_sym = "TEST_API_DEL_SRC"
        db_client = get_streaming_db_connection(read_only=False)
        try:
            add_streaming_symbol_to_db(test_sym, capital_ticker=test_sym, client=db_client)
            db_client.execute(f"""
                INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
                ('2026-10-01 11:01:00', '{test_sym}', 75.0, 5.0, 74.9, 75.1, 'CAPITAL', 'REG')
            """)

            before_ts = time.time() - 0.5
            del_resp = requests.delete(f"{api_test_server}/api/symbols/{test_sym}?source=streaming")
            assert del_resp.status_code == 200
            del_data = del_resp.json()
            assert del_data.get("success") is True

            # Check ticks purged
            ticks_after = db_client.execute(
                "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [test_sym]
            ).fetchone()[0]
            assert ticks_after == 0, (
                f"LEFTOVER DATA VIOLATION: DELETE /api/symbols/{test_sym}?source=streaming left {ticks_after} records in tick_data."
            )

            assert os.path.exists(RELOAD_SIGNAL_FILE)
            assert os.path.getmtime(RELOAD_SIGNAL_FILE) >= before_ts
        finally:
            try:
                db_client.execute("DELETE FROM streaming_database_symbols WHERE display_name = ?", [test_sym])
                db_client.execute("DELETE FROM tick_data WHERE symbol = ?", [test_sym])
            except Exception:
                pass
            db_client.close()

    def test_api_delete_symbol_preserves_other_symbols_ticks(self, api_test_server):
        """Deleting a symbol via API does not remove tick records of other symbols."""
        sym_keep = "TEST_API_KEEP"
        sym_drop = "TEST_API_DROP"
        db_client = get_streaming_db_connection(read_only=False)
        try:
            add_streaming_symbol_to_db(sym_keep, capital_ticker=sym_keep, client=db_client)
            add_streaming_symbol_to_db(sym_drop, capital_ticker=sym_drop, client=db_client)

            db_client.execute(f"""
                INSERT INTO tick_data (timestamp, symbol, price, volume, bid, ask, source, session) VALUES
                ('2026-10-01 11:02:00', '{sym_keep}', 100.0, 10.0, 99.9, 100.1, 'CAPITAL', 'REG'),
                ('2026-10-01 11:02:01', '{sym_drop}', 200.0, 20.0, 199.9, 200.1, 'CAPITAL', 'REG')
            """)

            requests.delete(f"{api_test_server}/api/streaming/symbols/{sym_drop}")

            keep_count = db_client.execute(
                "SELECT COUNT(*) FROM tick_data WHERE symbol = ?", [sym_keep]
            ).fetchone()[0]
            assert keep_count == 1, "Other symbol tick data must be preserved"
        finally:
            try:
                db_client.execute("DELETE FROM streaming_database_symbols WHERE display_name IN (?, ?)", [sym_keep, sym_drop])
                db_client.execute("DELETE FROM tick_data WHERE symbol IN (?, ?)", [sym_keep, sym_drop])
            except Exception:
                pass
            db_client.close()


# ============================================================================
# 4. Dashboard HTML Structure: Streaming Section Monitored Symbols Tab
# ============================================================================

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
# 5. Frontend JS Logic: Streaming Symbols Management
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
