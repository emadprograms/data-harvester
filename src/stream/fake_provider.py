"""
Deterministic Fake Streaming Provider (Milestone 4.3 - Package D: DURB-03).

Provides:
  - Sequence ledger: records all generated ticks with monotonically increasing sequence IDs.
  - Controllable disconnect / reconnect triggers.
  - Replay-capable mode vs. non-replayable live feed mode.
  - Automatic integration with GapLedger for recording capture and disconnect gaps.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
from typing import Any, Callable, Dict, List, Optional

from src.stream.gap_ledger import GapLedger

logger = logging.getLogger("fake_provider")


class FakeProvider:
    """
    Deterministic streaming provider with an independent sequence ledger
    and controllable disconnect/reconnect triggers.
    """

    def __init__(
        self,
        epics: Optional[List[str]] = None,
        on_tick_callback: Optional[Callable[[Any], Any]] = None,
        ticks_per_sec: float = 50.0,
        provider_name: str = "FAKE_PROVIDER",
        gap_ledger: Optional[GapLedger] = None,
        replay_capable: bool = False,
    ):
        self.epics = list(epics) if epics else ["AAPL", "MSFT"]
        self.on_tick_callback = on_tick_callback
        self.ticks_per_sec = float(ticks_per_sec)
        self.provider_name = provider_name
        self.gap_ledger = gap_ledger
        self.replay_capable = replay_capable

        self.running = False
        self.connected = True
        self._task: Optional[asyncio.Task] = None
        self._seq_id = 0
        self.ledger: List[Dict[str, Any]] = []
        self._active_gap_id: Optional[str] = None
        self._unacked_during_disconnect: List[Dict[str, Any]] = []

    async def start(self) -> None:
        self.running = True
        self._task = asyncio.create_task(self._run_loop())
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    async def disconnect(self, reason: str = "DISCONNECT") -> Optional[str]:
        """Trigger disconnection; records gap in gap ledger."""
        if not self.connected:
            return self._active_gap_id
        self.connected = False
        logger.info("FakeProvider %s disconnected (reason=%s)", self.provider_name, reason)
        if self.gap_ledger:
            self._active_gap_id = self.gap_ledger.open_gap(
                provider=self.provider_name,
                symbol="all",
                reason=reason,
                status="LOSS_UNKNOWN",
            )
        return self._active_gap_id

    async def reconnect(self) -> None:
        """Trigger reconnection; closes active gap in gap ledger and optionally replays."""
        if self.connected:
            return
        self.connected = True
        logger.info("FakeProvider %s reconnected", self.provider_name)
        if self.gap_ledger and self._active_gap_id:
            self.gap_ledger.close_gap(self._active_gap_id)
            self._active_gap_id = None

        if self.replay_capable and self._unacked_during_disconnect:
            replaying = list(self._unacked_during_disconnect)
            self._unacked_during_disconnect.clear()
            logger.info("FakeProvider %s replaying %d missed ticks", self.provider_name, len(replaying))
            for tick in replaying:
                if self.on_tick_callback:
                    res = self.on_tick_callback(tick)
                    if asyncio.iscoroutine(res):
                        await res

    async def _run_loop(self) -> None:
        interval = 1.0 / self.ticks_per_sec if self.ticks_per_sec > 0 else 0.02
        while self.running:
            self._seq_id += 1
            epic = self.epics[(self._seq_id - 1) % len(self.epics)]
            now = datetime.now(timezone.utc)
            tick = {
                "seq_id": self._seq_id,
                "epic": epic,
                "symbol": epic,
                "price": 100.0 + (self._seq_id % 100) * 0.1,
                "timestamp": now,
                "bid": 99.95,
                "ask": 100.05,
                "volume": 1.0,
                "source": self.provider_name,
            }
            # Record in sequence ledger (independent oracle)
            self.ledger.append(tick)

            if self.connected:
                if self.on_tick_callback:
                    res = self.on_tick_callback(tick)
                    if asyncio.iscoroutine(res):
                        await res
            else:
                if self.replay_capable:
                    self._unacked_during_disconnect.append(tick)
                # If non-replayable, tick is dropped at the provider boundary without delivery

            await asyncio.sleep(interval)

    def stop(self) -> None:
        self.running = False
        if self._active_gap_id and self.gap_ledger:
            self.gap_ledger.close_gap(self._active_gap_id)
            self._active_gap_id = None
        if self._task and not self._task.done():
            self._task.cancel()

    async def update_subscriptions(self, epics: List[str]) -> bool:
        self.epics = list(epics)
        return True
