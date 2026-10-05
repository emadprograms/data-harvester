"""
Data Harvester Dashboard Server.
Provides a lightweight, multi-threaded REST API and serves the interactive JavaScript web UI.
Runs on localhost:8420 with zero external framework dependencies.
"""
import os
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timezone, timedelta

from src.storage.registry import (
    SymbolRegistry,
    RegistryError,
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    SymbolPendingPurgeError,
    SymbolAlreadyExistsError,
    SymbolNotFoundError,
    InvalidSymbolError,
    touch_stream_reload_signal,
)


def _registry_symbols(reg):
    """Symbols from the lake registry; an uninitialised lake reads as empty."""
    try:
        return [s.to_dict() for s in reg.get_symbols()]
    except RegistryError:
        return []


def _get_lake_registry():
    try:
        from src.storage.config import resolve_tick_lake_root
        lake_root = resolve_tick_lake_root()
        return SymbolRegistry(root=lake_root)
    except Exception:
        env_root = os.environ.get("TICK_LAKE_ROOT") or os.environ.get("DATA_DIR")
        if env_root:
            try:
                return SymbolRegistry(root=env_root)
            except Exception:
                pass
        return None
from src.utils.integrity import (
    detect_stream_quiet_intervals,
)
from src.dashboard.analytics import (
    _get_lake_reader,
    get_candles,
    get_streaming_candles,
    get_stream_tape,
    get_ticks,
    get_stream_status,
    get_market_session_info,
    get_streaming_continuity_analysis,
    discover_available_weeks,
)

logger = logging.getLogger("dashboard_server")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
INDEX_PATH = os.path.join(STATIC_DIR, "index.html")


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
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, PATCH, OPTIONS")
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
            reader = _get_lake_reader()
            if reader is None:
                self._send_json({"status": "CRITICAL", "healthy": False, "error": "tick lake unavailable"})
                return
            lake = reader.get_lake_health_report()
            try:
                stream = reader.get_stream_status()
            except Exception as exc:
                stream = {"status": "UNKNOWN", "error": str(exc)}
            self._send_json({
                "status": "HEALTHY" if lake.get("healthy") else "DEGRADED",
                "healthy": bool(lake.get("healthy")),
                "lake": lake,
                "stream": stream,
            })
            return

        # 3. API: Symbol Inventory
        if path == "/api/streaming/symbols":
            reg = _get_lake_registry()
            if reg is not None and reg.root.exists():
                symbols = _registry_symbols(reg)
                self._send_json({"symbols": symbols, "total": len(symbols), "database": "streaming"})
                return
            self._send_json({
                "error": "tick lake registry unavailable", "symbols": [], "total": 0,
            }, status=503)
            return

        if path == "/api/symbols":
            # The tick lake is the only store; a legacy 'source' is accepted and ignored.
            reg = _get_lake_registry()
            if reg is not None and reg.root.exists():
                symbols = _registry_symbols(reg)
                self._send_json({"symbols": symbols, "total": len(symbols), "database": "streaming"})
                return
            self._send_json({
                "error": "tick lake registry unavailable", "symbols": [], "total": 0,
            }, status=503)
            return

        # 4. API: Data Integrity & Health Audits
        if path == "/api/integrity":
            target_symbol = query.get("symbol", [None])[0]
            now_utc = datetime.now(timezone.utc)

            if target_symbol:
                symbols = [target_symbol]
            else:
                symbols = []
                registry = _get_lake_registry()
                if registry is not None:
                    try:
                        symbols = [entry.symbol for entry in registry.get_active_symbols()][:5]
                    except Exception as exc:
                        logger.warning(f"Integrity symbol lookup failed: {exc}")

            quiet_results = [
                detect_stream_quiet_intervals(sym, lookback_minutes=60, threshold_seconds=120)
                for sym in symbols
            ]

            self._send_json({
                "overall_passed": bool(quiet_results) and all(q.get("passed", False) for q in quiet_results),
                "timestamp": now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
                "symbols_audited": symbols,
                "quiet_intervals": quiet_results,
            })
            return

        # 5. API: OHLCV Candlestick Query (with native time_bucket)
        if path in ["/api/candles", "/api/streaming/candles"]:
            sym = query.get("symbol", ["SPY"])[0]
            tf = query.get("timeframe", query.get("tf", ["1m"]))[0]
            start = query.get("start", [None])[0]
            end = query.get("end", [None])[0]
            date_param = query.get("date", [None])[0]
            hours_param = query.get("hours", query.get("extended", ["extended"]))[0]

            try:
                limit = int(query.get("limit", [1000])[0])
            except ValueError:
                limit = 1000
            res = get_candles(sym, tf, start, end, limit, date=date_param, hours=hours_param)
            self._send_json(res)
            return

        # 6. API: Symbol Coverage & Health Summary
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
        # 11. API: Harvester Job Full Logs
        # 12. API: Available Streaming Weeks
        if path == "/api/streaming/continuity/weeks":
            weeks = discover_available_weeks()
            self._send_json({"database": "streaming", "weeks": weeks, "count": len(weeks)})
            return

        # 13. API: Streaming Data Continuity & Integrity Visualizer (Bird's Eye View)
        if path == "/api/streaming/continuity":
            days_param = query.get("days", ["5"])[0]
            try:
                days = int(days_param)
            except (ValueError, TypeError):
                days = 5
            symbol = query.get("symbol", ["all"])[0]
            if "extended" in query or "hours" in query:
                ext_param = query.get("extended", ["false"])[0]
                hours_param = query.get("hours", ["regular"])[0]
                include_extended = (str(ext_param).strip().lower() == "true" or str(hours_param).strip().lower() == "extended")
            else:
                include_extended = True
            week_start = query.get("week_start", query.get("week", [None]))[0]
            target_date = query.get("date", query.get("target_date", [None]))[0]
            week_offset_param = query.get("week_offset", [None])[0]
            week_offset = None
            if week_offset_param is not None:
                try:
                    week_offset = int(week_offset_param)
                except (ValueError, TypeError):
                    week_offset = None
            target_week = query.get("target_week", [None])[0]
            end_date = query.get("end_date", [None])[0]
            res = get_streaming_continuity_analysis(
                days=days,
                symbol=symbol,
                include_extended=include_extended,
                week_start=week_start,
                target_date=target_date,
                week_offset=week_offset,
                target_week=target_week,
                end_date=end_date,
            )
            self._send_json(res)
            return

        self._send_json({"error": "Not Found", "path": path}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}

        # 1. Add Symbol — the Parquet lake registry is the only store.
        if path in ["/api/symbols", "/api/streaming/symbols"]:
            raw_src = query.get("source", query.get("db", [None]))[0]
            if not raw_src and isinstance(payload, dict):
                raw_src = payload.get("source", payload.get("db"))
            if raw_src and str(raw_src).lower() not in ("streaming", "live"):
                self._send_json({
                    "error": f"Unknown source '{raw_src}': the tick lake is the only store"
                }, status=400)
                return

            disp = (payload.get("display_name") or payload.get("symbol") or "").strip().upper() if isinstance(payload, dict) else ""
            if not disp:
                self._send_json({"success": False, "error": "display_name is required"}, status=400)
                return

            reg = _get_lake_registry()
            if reg is not None and reg.root.exists():
                symbol = payload.get("symbol") or disp
                display_name = payload.get("display_name") or payload.get("name") or symbol
                capital_ticker = payload.get("capital_ticker") or payload.get("epic")
                databento_ticker = payload.get("databento_ticker")
                binance_ticker = payload.get("binance_ticker")
                asset_class = payload.get("asset_class")
                metadata = payload.get("metadata") or {}
                if asset_class and "asset_class" not in metadata:
                    metadata["asset_class"] = asset_class

                try:
                    entry = reg.add_symbol(
                        symbol=symbol,
                        display_name=display_name,
                        capital_ticker=capital_ticker,
                        databento_ticker=databento_ticker,
                        binance_ticker=binance_ticker,
                        metadata=metadata,
                    )
                    touch_stream_reload_signal(root=reg.root)
                    self._trigger_reload_signal()
                    self._send_json({
                        "status": "success",
                        "success": True,
                        "symbol": entry.to_dict(),
                        "message": f"Streaming symbol {entry.symbol} added and streamer signaled",
                        "database": "streaming",
                    }, status=201)
                    return
                except SymbolPendingPurgeError as e:
                    self._send_json({
                        "status": "conflict",
                        "error": f"Symbol {symbol} is currently pending purge: {str(e)}",
                        "message": str(e),
                    }, status=409)
                    return
                except SymbolAlreadyExistsError as e:
                    self._send_json({
                        "status": "conflict",
                        "error": f"Symbol {symbol} already exists: {str(e)}",
                        "message": str(e),
                    }, status=409)
                    return
                except InvalidSymbolError as e:
                    self._send_json({"status": "error", "error": str(e)}, status=400)
                    return
                except Exception as e:
                    self._send_json({"status": "error", "error": str(e)}, status=500)
                    return

            self._send_json({"success": False, "error": "tick lake registry unavailable"}, status=503)
            return

        # 2. Trigger Streamer Reload
        if path == "/api/streamer/reload":
            self._trigger_reload_signal()
            self._send_json({"success": True, "message": "Live reload signal triggered successfully"})
            return

        # 3. Trigger Harvest Job
        self._send_json({"error": "Not Found", "path": path}, status=404)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path.startswith("/api/streaming/symbols/"):
            raw_symbol = path.replace("/api/streaming/symbols/", "").strip()
            display_name = unquote(raw_symbol).upper()

            if not display_name:
                self._send_json({"success": False, "error": "Symbol display name is required"}, status=400)
                return

            reg = _get_lake_registry()
            if reg is not None and reg.root.exists():
                try:
                    entry = reg.remove_symbol(display_name)
                    touch_stream_reload_signal(root=reg.root)
                    self._trigger_reload_signal()
                    self._send_json({
                        "status": STATUS_PENDING_PURGE,
                        "symbol": entry.symbol,
                        "message": f"Symbol {entry.symbol} marked for purge and subscription fenced.",
                        "success": True,
                    }, status=200)
                    return
                except SymbolNotFoundError as e:
                    self._send_json({"status": "not_found", "error": str(e)}, status=404)
                    return
                except Exception as e:
                    self._send_json({"status": "error", "error": str(e)}, status=500)
                    return

            self._send_json({"success": False, "error": "tick lake registry unavailable"}, status=503)
            return

        if path.startswith("/api/symbols/"):
            raw_symbol = path.replace("/api/symbols/", "").strip()
            display_name = unquote(raw_symbol).upper()

            if not display_name:
                self._send_json({"success": False, "error": "Symbol display name is required"}, status=400)
                return

            source = query.get("source", query.get("db", [None]))[0]
            if source and str(source).lower() not in ("streaming", "live"):
                self._send_json({
                    "success": False,
                    "error": f"Unknown source '{source}': the tick lake is the only store",
                }, status=400)
                return

            reg = _get_lake_registry()
            if reg is not None and reg.root.exists():
                try:
                    entry = reg.remove_symbol(display_name)
                    touch_stream_reload_signal(root=reg.root)
                    self._trigger_reload_signal()
                    self._send_json({
                        "status": STATUS_PENDING_PURGE,
                        "symbol": entry.symbol,
                        "message": f"Symbol {entry.symbol} marked for purge and subscription fenced.",
                        "success": True,
                    }, status=200)
                    return
                except SymbolNotFoundError as e:
                    self._send_json({"status": "not_found", "error": str(e)}, status=404)
                    return
                except Exception as e:
                    self._send_json({"status": "error", "error": str(e)}, status=500)
                    return

            self._send_json({"success": False, "error": "tick lake registry unavailable"}, status=503)
            return
            self._send_json({"success": False, "error": "tick lake registry unavailable"}, status=503)
            return

        self._send_json({"error": "Not Found", "path": path}, status=404)

    def do_PATCH(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            payload = json.loads(body)
        except Exception:
            payload = {}

        if path.startswith("/api/streaming/symbols/"):
            raw_symbol = path.replace("/api/streaming/symbols/", "").strip()
            display_name = unquote(raw_symbol).upper()

            if not display_name:
                self._send_json({"success": False, "error": "Symbol display name is required"}, status=400)
                return

            active = payload.get("active")
            reg = _get_lake_registry()
            if reg is not None and reg.root.exists():
                try:
                    entry = reg.toggle_symbol(display_name, active=active)
                    touch_stream_reload_signal(root=reg.root)
                    self._trigger_reload_signal()
                    self._send_json({
                        "status": "success",
                        "symbol": entry.to_dict(),
                        "message": f"Symbol {entry.symbol} toggled to active={entry.active}",
                    }, status=200)
                    return
                except SymbolPendingPurgeError as e:
                    self._send_json({"status": "conflict", "error": str(e)}, status=409)
                    return
                except SymbolNotFoundError as e:
                    self._send_json({"status": "not_found", "error": str(e)}, status=404)
                    return
                except Exception as e:
                    self._send_json({"status": "error", "error": str(e)}, status=500)
                    return

            self._send_json({"status": "error", "error": "tick lake registry unavailable"}, status=503)
            return

        self._send_json({"error": "Not Found", "path": path}, status=404)

    def _trigger_reload_signal(self):
        """Touches the lake's reload signal for the running streamer to pick up."""
        try:
            reg = _get_lake_registry()
            if reg and reg.root:
                touch_stream_reload_signal(root=reg.root)
            else:
                logger.warning("Tick lake registry unavailable: reload signal not written")
        except Exception as e:
            logger.warning(f"Could not write reload signal: {e}")

    def log_message(self, format, *args):
        # Override to suppress default noisy console access log during testing
        return


def create_dashboard_server(host="127.0.0.1", port=8420):
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
        port = args.port or int(os.getenv("DASHBOARD_PORT") or os.getenv("PORT") or 8420)
        if args.host:
            host = args.host

    server = None
    target_ports = [port]
    if port == 8420:
        target_ports.extend([8421, 8422, 8425])

    for p in target_ports:
        try:
            server = create_dashboard_server(host=host, port=p)
            port = p
            break
        except OSError as e:
            # errno 48 is Unix EADDRINUSE; 10048 is Windows WSAEADDRINUSE
            is_addr_in_use = e.errno in (48, 10048) or getattr(e, "winerror", None) == 10048
            if is_addr_in_use and len(target_ports) > 1:
                logger.warning(f"Port {p} is in use, attempting next port...")
                continue
            raise

    print(f"Data Harvester Dashboard running on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard server...")
    finally:
        if server:
            server.server_close()


if __name__ == "__main__":
    run_dashboard_server()
