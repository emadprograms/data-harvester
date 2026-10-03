"""
Integration tests for runner registry reload and dynamic version polling (Milestone v4.0 - Phase 18 P3).
Validates cross-process version polling, signal file wakeup hints, empty registry
unsubscription (no hardcoded fallback tickers), and tick fencing for purged/inactive symbols.
"""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
import pytest

from src.storage.registry import (
    STATUS_ACTIVE,
    STATUS_INACTIVE,
    STATUS_PENDING_PURGE,
    SymbolRegistry,
    init_registry,
    touch_stream_reload_signal,
)
from src.stream.runner import StreamingEngine


def test_runner_cross_process_version_polling(tmp_path):
    """
    Verifies that StreamingEngine polls registry version every poll_interval.
    When registry.json is updated externally (bumping version), runner detects
    the change and executes reload_symbols within 0.3s without dropping connections.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)

        # Runner starts with fast poll_interval=0.1
        engine = StreamingEngine(lake_root=lake_root, flush_interval=1.0)
        # Configure registry poll interval if supported, otherwise test polling loop
        if hasattr(engine, "registry_poll_interval"):
            engine.registry_poll_interval = 0.1

        mock_streamer = MagicMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer
        engine.running = True

        watcher_task = asyncio.create_task(engine._symbol_watcher_worker())

        try:
            # Initial state: reload to establish baseline
            await engine.reload_symbols()
            mock_streamer.update_subscriptions.reset_mock()

            # External mutation: add NVDA to registry (version bumps 1 -> 2)
            reg.add_symbol("NVDA", display_name="NVIDIA", capital_ticker="NVDA")

            # Polling should detect version bump within 0.3s
            reloaded = False
            for _ in range(30):
                if mock_streamer.update_subscriptions.called:
                    reloaded = True
                    break
                await asyncio.sleep(0.01)

            assert reloaded, "Runner failed to detect external registry version bump within 0.3s"
            args, _ = mock_streamer.update_subscriptions.call_args
            assert "NVDA" in args[0]
        finally:
            engine.running = False
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

    asyncio.run(_run())


def test_runner_signal_file_wake_hint(tmp_path):
    """
    Verifies that touching .stream_reload.signal triggers an immediate reload
    without waiting for the full periodic polling interval (e.g. 60s).
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)

        # Runner starts with long periodic check (60.0s)
        engine = StreamingEngine(lake_root=lake_root, flush_interval=1.0)
        if hasattr(engine, "registry_poll_interval"):
            engine.registry_poll_interval = 60.0

        mock_streamer = MagicMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer
        engine.running = True

        watcher_task = asyncio.create_task(engine._symbol_watcher_worker())

        try:
            # Baseline reload
            await engine.reload_symbols()
            mock_streamer.update_subscriptions.reset_mock()

            # Add symbol to registry
            reg.add_symbol("AAPL", display_name="Apple", capital_ticker="AAPL")

            # Touch the reload signal file (immediate wakeup hint)
            touch_stream_reload_signal(root=lake_root)

            # Runner should wake up immediately and reload within 0.2s
            reloaded = False
            for _ in range(20):
                if mock_streamer.update_subscriptions.called:
                    reloaded = True
                    break
                await asyncio.sleep(0.01)

            assert reloaded, "Touching .stream_reload.signal did not trigger immediate subscription reload"
            args, _ = mock_streamer.update_subscriptions.call_args
            assert "AAPL" in args[0]
        finally:
            engine.running = False
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

    asyncio.run(_run())


def test_runner_empty_registry_unsubscribes_all(tmp_path):
    """
    Verifies that when the symbol registry is empty (symbols={}), the runner
    updates subscriptions to [] and clears active_streaming_symbols.
    CRITICAL: Does NOT fall back to legacy hardcoded 6 or 19 tickers!
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)
        assert reg.load().symbols == {}

        engine = StreamingEngine(lake_root=lake_root)
        mock_streamer = MagicMock()
        mock_streamer.update_subscriptions = AsyncMock(return_value=True)
        engine.capital_streamer = mock_streamer

        # Execute reload with clean empty registry
        await engine.reload_symbols()

        assert mock_streamer.update_subscriptions.called, "update_subscriptions was not called"
        args, _ = mock_streamer.update_subscriptions.call_args
        assert args[0] == [], f"Expected empty subscription list [], got {args[0]}"
        assert engine.active_streaming_symbols == set()

        # Specifically check that none of the legacy fallback tickers were subscribed
        legacy_fallbacks = {"AAPL", "NVDA", "TSLA", "AMD", "AMZN", "MSFT"}
        assert not (engine.active_streaming_symbols & legacy_fallbacks), (
            f"Fell back to hardcoded tickers: {engine.active_streaming_symbols}"
        )

    asyncio.run(_run())


def test_runner_subscription_fence_drops_purged_symbol_ticks(tmp_path):
    """
    Verifies subscription fencing:
    Ticks arriving for symbols in PENDING_PURGE, INACTIVE, or not registered
    are dropped immediately outside the write queue, preventing writes to
    purged partitions.
    """
    async def _run():
        lake_root = tmp_path / "lake"
        init_registry(lake_root)
        reg = SymbolRegistry(lake_root)

        # Active symbol
        reg.add_symbol("AAPL", capital_ticker="AAPL")
        # Inactive symbol
        reg.add_symbol("TSLA", capital_ticker="TSLA")
        reg.toggle_symbol("TSLA", active=False)
        # Pending purge symbol
        reg.add_symbol("AMD", capital_ticker="AMD")
        reg.remove_symbol("AMD")

        engine = StreamingEngine(lake_root=lake_root)
        await engine.reload_symbols()

        # Check fence sets
        assert "AAPL" in engine.active_streaming_symbols
        assert "TSLA" not in engine.active_streaming_symbols
        assert "AMD" not in engine.active_streaming_symbols

        now = datetime.now(timezone.utc)

        # 1. Active tick for AAPL -> should be accepted into write_queue
        await engine._handle_capital_tick({
            "epic": "AAPL", "price": 150.0, "timestamp": now, "bid": 149.9, "ask": 150.1
        })

        # 2. Inactive tick for TSLA -> must be dropped
        await engine._handle_capital_tick({
            "epic": "TSLA", "price": 200.0, "timestamp": now, "bid": 199.9, "ask": 200.1
        })

        # 3. Pending purge tick for AMD -> must be dropped
        await engine._handle_capital_tick({
            "epic": "AMD", "price": 100.0, "timestamp": now, "bid": 99.9, "ask": 100.1
        })

        # 4. Unregistered tick for GOOGL -> must be dropped
        await engine._handle_capital_tick({
            "epic": "GOOGL", "price": 120.0, "timestamp": now, "bid": 119.9, "ask": 120.1
        })

        # Write queue should contain EXACTLY 1 item, which is AAPL
        assert engine.write_queue.qsize() == 1, (
            f"Expected write_queue size 1, found {engine.write_queue.qsize()}"
        )
        tick = engine.write_queue.get_nowait()
        assert tick[1] == "AAPL", f"Expected queued tick for AAPL, got {tick[1]}"

    asyncio.run(_run())
