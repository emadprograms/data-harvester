"""NOTIF-01 — operational notifications reach Discord, best-effort only.

Every event an operator needs: session started, session stopped, **session failed
to start**, supervisor restart/crash, and drain or compaction failure. The hard
requirement is the negative one — a webhook failure must never block or crash
ingestion — so the failure paths get more tests than the happy path.
"""
from __future__ import annotations

import http.server
import json
import threading
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.utils import notifications
from src.utils.discord import _post_embed


@pytest.fixture
def posted(monkeypatch):
    """Capture what the transport would send, without a network call."""
    spy = MagicMock(return_value=MagicMock(status_code=204))
    monkeypatch.setattr("src.utils.discord.requests.post", spy)
    return spy


def _embed_payload(spy):
    kwargs = spy.call_args.kwargs
    if kwargs:
        return kwargs["json"]["embeds"][0]
    return spy.call_args.args[1]["embeds"][0]


# ------------------------------------------------------------------ the events


def test_every_operational_event_is_declared():
    """The five events the owner asked for, plus the drain/compaction failures."""
    assert set(notifications.EVENTS) >= {
        notifications.SESSION_STARTED,
        notifications.SESSION_STOPPED,
        notifications.SESSION_START_FAILED,
        notifications.SUPERVISOR_RESTART,
        notifications.DRAIN_FAILED,
        notifications.COMPACTION_FAILED,
    }
    for event, spec in notifications.EVENTS.items():
        assert spec["title"], event
        assert spec["emoji"], event


@pytest.mark.parametrize("event", sorted(notifications.EVENTS))
def test_each_event_posts_a_titled_embed(event, posted, monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    assert notifications.notify(event, detail="detail line") is True
    embed = _embed_payload(posted)
    assert notifications.EVENTS[event]["title"] in embed["title"]
    assert embed["description"] == "detail line"
    assert embed["color"] == notifications.EVENTS[event]["color"]


def test_unknown_event_is_refused_without_posting(posted, monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    assert notifications.notify("not_a_real_event") is False
    posted.assert_not_called()


def test_fields_are_included_when_supplied(posted, monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    notifications.notify(
        notifications.SESSION_STOPPED,
        detail="drained",
        fields={"Symbols": 19, "Ticks": 4200},
    )
    embed = _embed_payload(posted)
    names = [field["name"] for field in embed["fields"]]
    assert names == ["Symbols", "Ticks"]
    assert embed["fields"][1]["value"] == "4200"


# --------------------------------------------------------- best-effort promises


def test_skip_discord_disables_notifications(posted, monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    monkeypatch.setenv("SKIP_DISCORD", "true")
    assert notifications.notify(notifications.SESSION_STARTED) is False
    posted.assert_not_called()


def test_missing_webhook_is_a_silent_no_op(posted, monkeypatch):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SKIP_DISCORD", raising=False)
    assert notifications.notify(notifications.SESSION_STARTED) is False
    posted.assert_not_called()


def test_transport_error_returns_false_and_never_raises(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")

    def explode(*args, **kwargs):
        raise RuntimeError("connection reset by peer")

    monkeypatch.setattr("src.utils.discord.requests.post", explode)
    assert notifications.notify(notifications.DRAIN_FAILED, detail="boom") is False


def test_webhook_poster_failure_returns_false(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    monkeypatch.setattr("src.utils.discord.requests.post", MagicMock(return_value=None))
    assert notifications.notify(notifications.COMPACTION_FAILED) is False


def test_unreachable_webhook_returns_false_quickly(monkeypatch):
    """A dead endpoint must fail on the network's terms, not on a long timeout."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://127.0.0.1:9/closed")
    started = time.monotonic()
    assert notifications.notify(notifications.SESSION_STARTED) is False
    assert time.monotonic() - started < 5.0


def test_detached_dispatch_does_not_block_the_caller(monkeypatch):
    """The runner calls this on the event loop: it must return immediately."""
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    released = threading.Event()

    def slow_post(*args, **kwargs):
        released.wait(5.0)
        return MagicMock(status_code=204)

    monkeypatch.setattr("src.utils.discord.requests.post", slow_post)
    started = time.monotonic()
    notifications.notify_detached(notifications.SESSION_STARTED, detail="async")
    elapsed = time.monotonic() - started
    released.set()
    assert elapsed < 0.5, f"detached dispatch blocked for {elapsed:.2f}s"


def test_detached_dispatch_still_delivers(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")
    calls = []
    monkeypatch.setattr(
        "src.utils.discord.requests.post",
        lambda url, **kwargs: calls.append(kwargs) or MagicMock(status_code=204),
    )
    notifications.notify_detached(notifications.SESSION_STOPPED, detail="bye")
    deadline = time.monotonic() + 5.0
    while not calls and time.monotonic() < deadline:
        time.sleep(0.01)
    assert calls, "detached notification never reached the transport"


def test_detached_dispatch_swallows_a_broken_transport(monkeypatch):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "http://webhook.example/abc")

    def explode(*args, **kwargs):
        raise RuntimeError("no route to host")

    monkeypatch.setattr("src.utils.discord.requests.post", explode)
    notifications.notify_detached(notifications.SUPERVISOR_RESTART, detail="restart #3")
    time.sleep(0.1)  # if the worker thread raised, pytest would surface it as a warning


# ------------------------------------------------------------ real transport


class _CollectingHandler(http.server.BaseHTTPRequestHandler):
    received: list = []

    def do_POST(self):  # noqa: N802 (http.server API)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        type(self).received.append(json.loads(body))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):  # keep the test output clean
        return


def test_real_webhook_transport_posts_the_embed(monkeypatch):
    _CollectingHandler.received = []
    server = http.server.HTTPServer(("127.0.0.1", 0), _CollectingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/webhook"
        monkeypatch.setenv("DISCORD_WEBHOOK_URL", url)
        assert notifications.notify(
            notifications.SESSION_STARTED, detail="19 symbols", fields={"Window": "04:00-20:00 ET"}
        ) is True

        deadline = time.monotonic() + 5.0
        while not _CollectingHandler.received and time.monotonic() < deadline:
            time.sleep(0.02)
        assert _CollectingHandler.received, "the local webhook received nothing"
        embed = _CollectingHandler.received[0]["embeds"][0]
        assert "started" in embed["title"].lower()
        assert embed["description"] == "19 symbols"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def test_post_embed_treats_a_missing_url_as_a_no_op():
    assert _post_embed(None, {"title": "x"}) is False
