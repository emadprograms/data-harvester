"""
Data Harvester Dashboard Server.
Provides a lightweight, multi-threaded REST API and serves the interactive JavaScript web UI.
Runs on localhost:8000 with zero external framework dependencies.
"""
import os
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timezone, timedelta

from src.database.operations import (
    get_symbol_inventory_list,
    add_symbol_to_db,
    remove_symbol_from_db,
    get_symbol_map_from_db,
)
from src.utils.integrity import (
    get_database_health_report,
    detect_1m_gaps,
    detect_stream_quiet_intervals,
    validate_ohlcv_anomalies,
    analyze_price_drift,
)
from src.dashboard.analytics import (
    get_candles,
    get_historical_candles,
    get_streaming_candles,
    get_historical_overview,
    get_symbols_coverage,
    get_stream_tape,
    get_ticks,
    get_stream_status,
    get_market_session_info,
)
from src.dashboard.harvester_job import harvester_manager
from src.database.connection import get_historical_db_connection

logger = logging.getLogger("dashboard_server")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
INDEX_PATH = os.path.join(STATIC_DIR, "index.html")
RELOAD_SIGNAL_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", ".stream_reload.signal")


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Multi-threaded HTTP server so concurrent requests do not block each other."""
    daemon_threads = True
    allow_reuse_address = True


class DashboardRequestHandler(BaseHTTPRequestHandler):
    """Handles REST API and static UI serving."""

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text, content_type="text/html", status=200):
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, data: bytes, content_type="application/octet-stream", status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(data)

    def _serve_static_file(self, rel_path: str):
        # Normalize and unquote to safely resolve relative path
        norm_rel = os.path.normpath(unquote(rel_path)).lstrip("/\\")
        file_path = os.path.abspath(os.path.join(STATIC_DIR, norm_rel))
        static_dir_abs = os.path.abspath(STATIC_DIR)

        # Strictly enforce directory traversal prevention
        try:
            common = os.path.commonpath([static_dir_abs, file_path])
        except ValueError:
            self._send_json({"error": "Forbidden: Path traversal detected"}, status=403)
            return

        if common != static_dir_abs:
            self._send_json({"error": "Forbidden: Path traversal detected"}, status=403)
            return

        if not os.path.isfile(file_path):
            self._send_json({"error": "Not Found", "file": rel_path}, status=404)
            return

        mime_types = {
            ".html": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".ico": "image/x-icon",
            ".txt": "text/plain",
        }
        _, ext = os.path.splitext(file_path)
        content_type = mime_types.get(ext.lower(), "application/octet-stream")

        try:
            with open(file_path, "rb") as f:
                data = f.read()
            self._send_bytes(data, content_type=content_type)
        except Exception as e:
            self._send_json({"error": f"Error reading static file: {str(e)}"}, status=500)

    def do_HEAD(self):
        # Support HEAD requests identically to GET for monitoring and status checks
        self.do_GET()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS, HEAD")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # 1. Static Web Dashboard Route
        if path in ["/", "/index.html"]:
            if os.path.exists(INDEX_PATH):
                with open(INDEX_PATH, "r", encoding="utf-8") as f:
                    content = f.read()
                self._send_text(content, content_type="text/html")
            else:
                self._send_text("<h1>Dashboard UI under construction</h1>", status=200)
            return

        # 1b. Static Assets (/static/*, /css/*, /js/*, /favicon.ico)
        if path.startswith("/static/"):
            rel = path[len("/static/"):]
            self._serve_static_file(rel)
            return
        elif path.startswith("/js/") or path.startswith("/css/") or path == "/favicon.ico":
            self._serve_static_file(path.lstrip("/"))
            return

        # 2. API: System & Database Health Status
        if path == "/api/status":
            report = get_database_health_report()
            self._send_json(report)
            return

        # 3. API: Symbol Inventory
        if path == "/api/symbols":
            symbols = get_symbol_inventory_list()
            self._send_json({"symbols": symbols, "total": len(symbols)})
            return

        # 4. API: Data Integrity & Health Audits
        if path == "/api/integrity":
            target_symbol = query.get("symbol", [None])[0]
            start_param = query.get("start", [None])[0]
            end_param = query.get("end", [None])[0]

            smap = get_symbol_map_from_db()
            symbol_list = [target_symbol] if target_symbol else list(smap.keys())[:5]

            now_utc = datetime.now(timezone.utc)
            gap_results = []
            quiet_results = []
            drift_results = []

            # Smart date range resolution per symbol or user-provided
            client = get_historical_db_connection()

            for sym in symbol_list:
                sym_start = None
                sym_end = None

                if start_param and end_param:
                    try:
                        sym_start = datetime.fromisoformat(start_param.replace("Z", "+00:00"))
                        sym_end = datetime.fromisoformat(end_param.replace("Z", "+00:00"))
                    except Exception:
                        pass

                if not sym_start or not sym_end:
                    # Discover latest recorded timestamp for this symbol
                    if client:
                        try:
                            max_ts_row = client.execute("SELECT MAX(timestamp) FROM market_data WHERE symbol = ?", [sym]).fetchone()
                            if max_ts_row and max_ts_row[0]:
                                max_dt = max_ts_row[0]
                                if isinstance(max_dt, str):
                                    max_dt = datetime.strptime(max_dt.split('.')[0], "%Y-%m-%d %H:%M:%S")
                                if max_dt.tzinfo is None:
                                    max_dt = max_dt.replace(tzinfo=timezone.utc)
                                sym_end = max_dt
                                sym_start = max_dt - timedelta(days=5)
                        except Exception:
                            pass

                if not sym_start or not sym_end:
                    sym_end = now_utc
                    sym_start = now_utc - timedelta(days=5)

                gap_results.append(detect_1m_gaps(sym, sym_start, sym_end, client=client))
                quiet_results.append(detect_stream_quiet_intervals(sym, lookback_minutes=60, threshold_seconds=120))
                drift_results.append(analyze_price_drift(sym))

            if client:
                client.close()

            anomaly_result = validate_ohlcv_anomalies(symbol=target_symbol)

            all_passed = (
                anomaly_result["passed"] and
                all(g.get("passed", True) for g in gap_results) and
                all(d.get("passed", True) for d in drift_results)
            )

            audit_response = {
                "overall_passed": all_passed,
                "timestamp": now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
                "symbols_audited": symbol_list,
                "anomalies": anomaly_result,
                "gaps": gap_results,
                "quiet_intervals": quiet_results,
                "drift": drift_results
            }
            self._send_json(audit_response)
            return

        # 5. API: OHLCV Candlestick Query (with native time_bucket)
        if path in ["/api/candles", "/api/historical/candles", "/api/streaming/candles"]:
            sym = query.get("symbol", ["SPY"])[0]
            tf = query.get("timeframe", query.get("tf", ["1m"]))[0]
            start = query.get("start", [None])[0]
            end = query.get("end", [None])[0]
            db_source = query.get("source", query.get("db", ["historical"]))[0]
            if path == "/api/historical/candles":
                db_source = "historical"
            elif path == "/api/streaming/candles":
                db_source = "streaming"

            try:
                limit = int(query.get("limit", [1000])[0])
            except ValueError:
                limit = 1000
            res = get_candles(sym, tf, start, end, limit, db_source=db_source)
            self._send_json(res)
            return

        # 5b. API: Dedicated Historical Database Overview
        if path == "/api/historical/overview":
            res = get_historical_overview()
            self._send_json(res)
            return

        # 6. API: Symbol Coverage & Health Summary
        if path == "/api/symbols/coverage":
            cov = get_symbols_coverage()
            self._send_json(cov)
            return

        # 7. API: Live Stream Tape
        if path == "/api/stream/tape":
            sym = query.get("symbol", [None])[0]
            try:
                limit = int(query.get("limit", [50])[0])
            except ValueError:
                limit = 50
            tape = get_stream_tape(sym, limit)
            self._send_json(tape)
            return

        # 7b. API: Raw Ticks Query
        if path == "/api/ticks":
            sym = query.get("symbol", [None])[0]
            start = query.get("start", query.get("start_time", [None]))[0]
            end = query.get("end", query.get("end_time", [None]))[0]
            try:
                limit = int(query.get("limit", [10000])[0])
            except ValueError:
                limit = 10000
            try:
                offset = int(query.get("offset", [0])[0])
            except ValueError:
                offset = 0
            direction = query.get("direction", ["asc"])[0]

            ticks_res = get_ticks(sym, start, end, limit, offset, direction)
            self._send_json(ticks_res.get("ticks", []))
            return

        # 8. API: Streamer Process & Health Status
        if path == "/api/stream/status":
            st = get_stream_status()
            self._send_json(st)
            return

        # 9. API: Market Session Status & Clock
        if path == "/api/market/session":
            mkt = get_market_session_info()
            self._send_json(mkt)
            return

        # 10. API: Harvester Job Status
        if path == "/api/harvester/status":
            job = harvester_manager.get_status()
            self._send_json(job)
            return

        # 11. API: Harvester Job Full Logs
        if path == "/api/harvester/logs":
            logs = harvester_manager.get_all_logs()
            self._send_json({"logs": logs, "total_lines": len(logs)})
            return

        self._send_json({"error": "Not Found", "path": path}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}

        # 1. Add Symbol
        if path == "/api/symbols":
            disp = payload.get("display_name", "").strip().upper()
            if not disp:
                self._send_json({"success": False, "error": "display_name is required"}, status=400)
                return

            c_ticker = payload.get("capital_ticker") or disp
            m_ticker = payload.get("massive_ticker") or disp
            y_ticker = payload.get("yahoo_ticker")
            b_ticker = payload.get("binance_ticker")

            success = add_symbol_to_db(
                display_name=disp,
                yahoo_ticker=y_ticker,
                massive_ticker=m_ticker,
                binance_ticker=b_ticker,
                capital_ticker=c_ticker
            )
            if success:
                # Trigger live reload signal
                self._trigger_reload_signal()
                self._send_json({"success": True, "message": f"Symbol {disp} added and streamer signaled"})
            else:
                self._send_json({"success": False, "error": "Database write error"}, status=500)
            return

        # 2. Trigger Streamer Reload
        if path == "/api/streamer/reload":
            self._trigger_reload_signal()
            self._send_json({"success": True, "message": "Live reload signal triggered successfully"})
            return

        # 3. Trigger Harvest Job
        if path == "/api/harvester/run":
            target_date = payload.get("date")
            if target_date:
                target_date = target_date.strip()
            ok, msg, job_status = harvester_manager.start_job(target_date)
            status_code = 200 if ok else 409
            self._send_json({"success": ok, "message": msg, "job": job_status}, status=status_code)
            return

        self._send_json({"error": "Not Found", "path": path}, status=404)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/symbols/"):
            raw_symbol = path.replace("/api/symbols/", "").strip()
            display_name = unquote(raw_symbol).upper()

            if not display_name:
                self._send_json({"success": False, "error": "Symbol display name is required"}, status=400)
                return

            success = remove_symbol_from_db(display_name)
            if success:
                self._trigger_reload_signal()
                self._send_json({"success": True, "message": f"Symbol {display_name} deleted and streamer signaled"})
            else:
                self._send_json({"success": False, "error": "Failed to remove symbol"}, status=500)
            return

        self._send_json({"error": "Not Found", "path": path}, status=404)

    def _trigger_reload_signal(self):
        """Creates or touches signal file for running streamer to pick up."""
        try:
            os.makedirs(os.path.dirname(RELOAD_SIGNAL_FILE), exist_ok=True)
            with open(RELOAD_SIGNAL_FILE, "w") as f:
                f.write(datetime.now(timezone.utc).isoformat())
        except Exception as e:
            logger.warning(f"Could not write reload signal file: {e}")

    def log_message(self, format, *args):
        # Override to suppress default noisy console access log during testing
        return


def create_dashboard_server(host="127.0.0.1", port=8000):
    """Creates a ThreadedHTTPServer instance."""
    return ThreadedHTTPServer((host, port), DashboardRequestHandler)


def run_dashboard_server(host="0.0.0.0", port=None):
    """Starts the dashboard server loop with optional port fallback."""
    if port is None:
        import argparse
        parser = argparse.ArgumentParser(description="Data Harvester Dashboard Server")
        parser.add_argument("--port", type=int, default=None, help="Port to listen on")
        parser.add_argument("--host", type=str, default=None, help="Host to bind to")
        args, _ = parser.parse_known_args()
        port = args.port or int(os.getenv("DASHBOARD_PORT") or os.getenv("PORT") or 8000)
        if args.host:
            host = args.host

    server = None
    target_ports = [port]
    if port == 8000:
        target_ports.extend([8001, 8080])

    for p in target_ports:
        try:
            server = create_dashboard_server(host=host, port=p)
            port = p
            break
        except OSError as e:
            if e.errno == 48 and len(target_ports) > 1:
                logger.warning(f"Port {p} is in use, attempting next port...")
                continue
            raise

    print(f"🚀 Data Harvester Dashboard running on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard server...")
    finally:
        if server:
            server.server_close()


if __name__ == "__main__":
    run_dashboard_server()
