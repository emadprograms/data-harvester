"""
Unit and integration tests for dashboard static asset serving and security.
Validates serving of CSS stylesheets, JavaScript modules, root-relative routing,
MIME type headers, and directory traversal rejection.
"""
import os
import socket
import threading
import time
import pytest
import requests
from urllib.parse import quote

from src.dashboard.server import create_dashboard_server, STATIC_DIR


@pytest.fixture(scope="module")
def static_server():
    """Starts an ephemeral ThreadedHTTPServer on an open port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()

    server = create_dashboard_server(host="127.0.0.1", port=port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    time.sleep(0.15)

    base_url = f"http://127.0.0.1:{port}"
    yield base_url

    server.shutdown()
    server.server_close()


def test_serve_index_root(static_server):
    """GET / serves the index.html with links to modular static assets."""
    resp = requests.get(f"{static_server}/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("Content-Type", "")
    assert "/static/css/dashboard.css" in resp.text
    assert "/static/js/app.js" in resp.text
    assert "/static/js/chart.js" in resp.text
    assert "/static/js/tables.js" in resp.text


def test_serve_index_html(static_server):
    """GET /index.html serves the exact same HTML document."""
    resp = requests.get(f"{static_server}/index.html")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("Content-Type", "")
    assert "DATA HARVESTER" in resp.text


def test_serve_static_css(static_server):
    """GET /static/css/dashboard.css serves CSS with correct MIME type."""
    resp = requests.get(f"{static_server}/static/css/dashboard.css")
    assert resp.status_code == 200
    assert "text/css" in resp.headers.get("Content-Type", "")
    assert "pulse-subtle" in resp.text
    assert "live-pulse" in resp.text


@pytest.mark.parametrize("js_module,expected_symbol", [
    ("state.js", "showToast"),
    ("chart.js", "initChart"),
    ("tables.js", "renderInspectorTable"),
    ("telemetry.js", "fetchStreamTape"),
    ("harvester.js", "triggerHarvestRun"),
    ("app.js", "DOMContentLoaded"),
])
def test_serve_static_js_modules(static_server, js_module, expected_symbol):
    """GET /static/js/{module} serves valid JavaScript with application/javascript MIME type."""
    resp = requests.get(f"{static_server}/static/js/{js_module}")
    assert resp.status_code == 200
    assert "application/javascript" in resp.headers.get("Content-Type", "")
    assert expected_symbol in resp.text
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"


def test_serve_direct_asset_routes(static_server):
    """Direct root-relative asset routes /css/... and /js/... should be routed seamlessly."""
    resp_css = requests.get(f"{static_server}/css/dashboard.css")
    assert resp_css.status_code == 200
    assert "text/css" in resp_css.headers.get("Content-Type", "")

    resp_js = requests.get(f"{static_server}/js/app.js")
    assert resp_js.status_code == 200
    assert "application/javascript" in resp_js.headers.get("Content-Type", "")


def test_static_path_traversal_rejection(static_server):
    """Verify that path traversal attempts are rejected with 403 Forbidden."""
    # Direct traversal attempt to server.py
    resp = requests.get(f"{static_server}/static/../../src/dashboard/server.py")
    assert resp.status_code in (403, 404)
    if resp.status_code == 403:
        data = resp.json()
        assert "Path traversal detected" in data.get("error", "")

    # URL-encoded traversal attempt
    encoded_path = "%2e%2e%2f%2e%2e%2fsrc%2fdashboard%2fserver.py"
    resp_encoded = requests.get(f"{static_server}/static/{encoded_path}")
    assert resp_encoded.status_code in (403, 404)


def test_static_missing_file_404(static_server):
    """GET to a non-existent static file returns 404 Not Found."""
    resp = requests.get(f"{static_server}/static/js/nonexistent_file.js")
    assert resp.status_code == 404
    data = resp.json()
    assert data.get("error") == "Not Found"


def test_options_cors_preflight(static_server):
    """OPTIONS request returns 204 with CORS headers."""
    resp = requests.options(f"{static_server}/static/js/app.js")
    assert resp.status_code == 204
    assert resp.headers.get("Access-Control-Allow-Origin") == "*"
    assert "GET" in resp.headers.get("Access-Control-Allow-Methods", "")
