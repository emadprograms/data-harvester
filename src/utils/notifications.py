"""Operational notifications (NOTIF-01).

Builds the embeds for the events an operator must hear about and hands them to
the transport in `src/utils/discord.py`:

===========================  ==================================================
``session_started``          the streamer authenticated and subscribed
``session_stopped``          the streamer drained and stopped on purpose
``session_start_failed``     the streamer could not start (auth, lake, config)
``supervisor_restart``       the supervisor restarted a crashed child
``drain_failed``             accepted ticks could not be durably drained
``compaction_failed``        off-hours compaction failed
===========================  ==================================================

**Best-effort, by contract.** A webhook failure must never block or crash
ingestion, so:

- ``notify()`` is synchronous and returns a bool; it never raises.
- ``notify_detached()`` runs the transport on a daemon thread and returns
  immediately, which is what the event-loop-owning runner uses. Set
  ``SKIP_DISCORD=true`` to silence everything.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.credentials import get_discord_webhook_url
from src.utils.discord import _post_embed

logger = logging.getLogger("notifications")

SESSION_STARTED = "session_started"
SESSION_STOPPED = "session_stopped"
SESSION_START_FAILED = "session_start_failed"
SUPERVISOR_RESTART = "supervisor_restart"
DRAIN_FAILED = "drain_failed"
COMPACTION_FAILED = "compaction_failed"
INGESTION_STALLED = "ingestion_stalled"

EVENTS: Dict[str, Dict[str, Any]] = {
    SESSION_STARTED: {
        "emoji": "🟢",
        "title": "Ingestion session started",
        "color": 0x2ECC71,
    },
    SESSION_STOPPED: {
        "emoji": "⚪",
        "title": "Ingestion session stopped",
        "color": 0x95A5A6,
    },
    SESSION_START_FAILED: {
        "emoji": "🔴",
        "title": "Ingestion session failed to start",
        "color": 0xE74C3C,
    },
    SUPERVISOR_RESTART: {
        "emoji": "♻️",
        "title": "Supervisor restart",
        "color": 0xF1C40F,
    },
    DRAIN_FAILED: {
        "emoji": "🟠",
        "title": "Shutdown drain failed",
        "color": 0xE67E22,
    },
    COMPACTION_FAILED: {
        "emoji": "🟠",
        "title": "Off-hours compaction failed",
        "color": 0xE67E22,
    },
    INGESTION_STALLED: {
        "emoji": "🟠",
        "title": "Ingestion stalled",
        "color": 0xE67E22,
    },
}

_SKIP_TRUTHY = {"1", "true", "yes", "on"}


def notifications_disabled() -> bool:
    """True when ``SKIP_DISCORD`` asks for silence."""
    return os.getenv("SKIP_DISCORD", "").strip().lower() in _SKIP_TRUTHY


def build_embed(
    event: str,
    detail: Optional[str] = None,
    fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """The Discord embed for one event (raises ``KeyError`` for unknown events)."""
    spec = EVENTS[event]
    embed: Dict[str, Any] = {
        "title": f"{spec['emoji']} {spec['title']}",
        "description": detail or "",
        "color": spec["color"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if fields:
        embed["fields"] = [
            {"name": str(name), "value": str(value), "inline": True}
            for name, value in fields.items()
        ]
    return embed


def notify(
    event: str,
    detail: Optional[str] = None,
    *,
    fields: Optional[Dict[str, Any]] = None,
    webhook_url: Optional[str] = None,
    poster=None,
) -> bool:
    """Post one notification. Never raises; ``False`` means "not delivered"."""
    try:
        spec = EVENTS.get(event)
        if spec is None:
            logger.warning("Ignoring unknown notification event %r", event)
            return False
        if notifications_disabled():
            logger.debug("Discord notifications disabled (SKIP_DISCORD); dropping %s", event)
            return False
        url = webhook_url or get_discord_webhook_url()
        if not url:
            return False
        embed = build_embed(event, detail, fields)
        return bool((poster or _post_embed)(url, embed))
    except Exception as exc:  # noqa: BLE001 — best-effort by contract
        logger.warning("Discord notification %r failed: %s", event, exc)
        return False


def notify_detached(
    event: str,
    detail: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Fire-and-forget dispatch: returns immediately, never raises.

    Synchronous transport work happens on a daemon thread so a slow or dead
    webhook cannot stall the ingestion event loop or the supervisor loop.
    """
    if event not in EVENTS or notifications_disabled():
        return
    if not (kwargs.get("webhook_url") or get_discord_webhook_url()):
        return
    thread = threading.Thread(
        target=notify,
        args=(event, detail),
        kwargs=kwargs,
        name=f"notify-{event}",
        daemon=True,
    )
    thread.start()
