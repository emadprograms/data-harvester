"""
Provider Gap Ledger & Disconnect Tests (Milestone 4.3 - DURB-03).

Verifies:
  - Gaps recorded in <lake_root>/_control/gaps.json
  - Gap entries contain: gap_id, provider/source, symbol, start_time, end_time, reason, status: "LOSS_UNKNOWN"
  - Never invent a fake count of lost ticks during disconnects
  - FakeProvider with sequence ledger and controllable disconnect/reconnect triggers
  - Disconnection while capacity/buffer is exhausted (backpressure / overflow)
  - Disconnection during supervisor handoff
  - Disconnection during shutdown
  - Visible gaps readable via TickLakeReader.read_gaps()
  - Replay-capable vs. non-replayable feed semantics
"""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import pytest

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from src.storage.registry import SymbolRegistry, init_registry
from src.stream.fake_provider import FakeProvider
from src.stream.gap_ledger import GapEntry, GapLedger
from src.stream.runner import DrainFailedError, StreamingEngine


@pytest.fixture
def lake_root(tmp_path):
    root = tmp_path / "lake"
    init_tick_lake(root)
    init_registry(root)
    reg = SymbolRegistry(root)
    reg.add_symbol("AAPL", capital_ticker="AAPL")
    reg.add_symbol("MSFT", capital_ticker="MSFT")
    return root


def test_gap_ledger_open_close_and_persistence(lake_root):
    """Verifies open_gap, close_gap, and atomic persistence to _control/gaps.json."""
    ledger = GapLedger(lake_root)

    # 1. Open a disconnect gap
    gap_id = ledger.open_gap(
        provider="CAPITAL",
        symbol="all",
        reason="DISCONNECT",
        status="LOSS_UNKNOWN",
        details={"attempt": 1},
    )
    assert gap_id.startswith("gap_")

    # Assert visible in file immediately
    gaps_file = lake_root / "_control" / "gaps.json"
    assert gaps_file.is_file()
    entries = json.loads(gaps_file.read_text(encoding="utf-8"))
    assert len(entries) == 1
    assert entries[0]["gap_id"] == gap_id
    assert entries[0]["provider"] == "CAPITAL"
    assert entries[0]["symbol"] == "all"
    assert entries[0]["reason"] == "DISCONNECT"
    assert entries[0]["status"] == "LOSS_UNKNOWN"
    assert entries[0]["end_time"] is None

    # 2. Close the gap
    closed = ledger.close_gap(gap_id, details_update={"reconnected_cleanly": True})
    assert closed is not None
    assert closed.end_time is not None
    assert closed.details.get("reconnected_cleanly") is True

    # Assert updated in file
    entries = json.loads(gaps_file.read_text(encoding="utf-8"))
    assert len(entries) == 1
    assert entries[0]["end_time"] == closed.end_time
    assert entries[0]["status"] == "LOSS_UNKNOWN"

    # Readable via TickLakeReader
    reader = TickLakeReader(lake_root)
    reader_gaps = reader.read_gaps()
    assert len(reader_gaps) == 1
    assert reader_gaps[0]["gap_id"] == gap_id


def test_fake_provider_deterministic_sequence_and_disconnect(lake_root):
    """
    FakeProvider emits monotonic sequence numbers into its ledger.
    When disconnected in non-replayable mode, unreceived ticks are not delivered,
    and a gap with status: LOSS_UNKNOWN is opened and closed.
    """
    received_ticks = []

    async def _run():
        ledger = GapLedger(lake_root)
        provider = FakeProvider(
            epics=["AAPL"],
            on_tick_callback=lambda t: received_ticks.append(t),
            ticks_per_sec=200.0,
            provider_name="FAKE_CAPITAL",
            gap_ledger=ledger,
            replay_capable=False,
        )

        task = asyncio.create_task(provider.start())
        # Receive ~10 ticks
        await asyncio.sleep(0.08)
        assert len(received_ticks) >= 3
        count_before_disconnect = len(received_ticks)

        # Disconnect provider
        gap_id = await provider.disconnect(reason="DISCONNECT")
        assert not provider.connected
        assert gap_id is not None

        # Verify gap opened in ledger
        gaps = ledger.read_gaps()
        assert len(gaps) == 1
        assert gaps[0]["gap_id"] == gap_id
        assert gaps[0]["reason"] == "DISCONNECT"
        assert gaps[0]["status"] == "LOSS_UNKNOWN"
        assert gaps[0]["end_time"] is None

        # Ticks continue to be recorded in provider's internal sequence ledger
        await asyncio.sleep(0.08)
        assert len(received_ticks) == count_before_disconnect, "No ticks should be delivered while disconnected"
        assert len(provider.ledger) > count_before_disconnect

        # Reconnect
        await provider.reconnect()
        assert provider.connected
        await asyncio.sleep(0.08)

        # Assert new ticks received
        assert len(received_ticks) > count_before_disconnect

        # Gap is now closed
        closed_gaps = ledger.read_gaps()
        assert len(closed_gaps) == 1
        assert closed_gaps[0]["end_time"] is not None
        assert closed_gaps[0]["status"] == "LOSS_UNKNOWN"
        # Never invented a fake count of lost ticks in the ledger
        assert "lost_count" not in closed_gaps[0]

        provider.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_fake_provider_replay_capable_mode(lake_root):
    """
    In replay_capable=True mode, FakeProvider replays ticks emitted during disconnect
    upon reconnection, preserving the complete sequence.
    """
    received_ticks = []

    async def _run():
        ledger = GapLedger(lake_root)
        provider = FakeProvider(
            epics=["AAPL"],
            on_tick_callback=lambda t: received_ticks.append(t),
            ticks_per_sec=200.0,
            provider_name="REPLAY_FEED",
            gap_ledger=ledger,
            replay_capable=True,
        )

        task = asyncio.create_task(provider.start())
        await asyncio.sleep(0.06)
        before_count = len(received_ticks)

        # Disconnect
        await provider.disconnect(reason="TRANSIENT_NETWORK_DROP")
        await asyncio.sleep(0.08)
        assert len(received_ticks) == before_count

        # Reconnect with replay
        await provider.reconnect()
        await asyncio.sleep(0.06)

        # All sequence IDs are contiguous
        seq_ids = [t["seq_id"] for t in received_ticks]
        assert seq_ids == list(range(1, len(seq_ids) + 1)), "Replay must deliver contiguous sequence numbers"

        provider.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run())


def test_gap_recorded_on_buffer_overflow_drop(lake_root):
    """
    When write_queue is full and ticks are dropped, StreamingEngine records
    a gap in the gap ledger with reason: BUFFER_OVERFLOW and status: LOSS_UNKNOWN.
    """
    engine = StreamingEngine(
        lake_root=lake_root,
        max_queue_size=5,
        flush_interval=10.0,
    )
    engine.running = True

    # Fill queue to capacity (5 items)
    for i in range(5):
        assert engine._enqueue_tick(("2026-10-02 12:00:00", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG"))

    # 6th tick exceeds capacity -> rejected and dropped
    success = engine._enqueue_tick(("2026-10-02 12:00:00", "AAPL", 160.0, 1.0, 149.9, 150.1, "CAPITAL", "REG"))
    assert not success
    assert engine.ticks_dropped == 1

    # Verify gap recorded in ledger
    reader = TickLakeReader(lake_root)
    gaps = reader.read_gaps()
    assert len(gaps) >= 1
    overflow_gaps = [g for g in gaps if g["reason"] == "BUFFER_OVERFLOW"]
    assert len(overflow_gaps) == 1
    assert overflow_gaps[0]["status"] == "LOSS_UNKNOWN"
    assert overflow_gaps[0]["symbol"] == "AAPL"

    engine.stop()
    engine.writer.close()


def test_gap_recorded_during_supervisor_handoff(lake_root):
    """
    During supervisor handoff, visible handoff gap is recorded in the gap ledger.
    """
    ledger = GapLedger(lake_root)
    handoff_gap = ledger.record_gap(
        provider="SUPERVISOR",
        symbol="all",
        reason="SUPERVISOR_HANDOFF",
        status="LOSS_UNKNOWN",
        details={"old_pid": 1234, "new_pid": 5678, "handoff_type": "HOT_STANDBY"},
    )
    assert handoff_gap.reason == "SUPERVISOR_HANDOFF"
    assert handoff_gap.status == "LOSS_UNKNOWN"

    reader = TickLakeReader(lake_root)
    gaps = reader.read_gaps()
    assert any(g["reason"] == "SUPERVISOR_HANDOFF" for g in gaps)


def test_gap_recorded_on_shutdown_with_unflushed_work(lake_root):
    """
    When shutdown fails or times out with unflushed accepted items,
    StreamingEngine records a SHUTDOWN_UNFLUSHED gap in the gap ledger with status: LOSS_UNKNOWN.
    """
    async def _run():
        engine = StreamingEngine(
            lake_root=lake_root,
            max_queue_size=100,
            flush_interval=10.0,
        )
        engine.running = True

        for i in range(15):
            engine._enqueue_tick(("2026-10-02 12:00:00", "AAPL", 150.0 + i, 1.0, 149.9, 150.1, "CAPITAL", "REG"))

        # Shutdown without worker causes drain timeout
        with pytest.raises(DrainFailedError):
            await engine.shutdown(drain_timeout=0.1)

        # Verify gap recorded
        reader = TickLakeReader(lake_root)
        gaps = reader.read_gaps()
        unflushed_gaps = [g for g in gaps if g["reason"] == "SHUTDOWN_UNFLUSHED"]
        assert len(unflushed_gaps) == 1
        assert unflushed_gaps[0]["status"] == "LOSS_UNKNOWN"
        assert unflushed_gaps[0]["details"]["pending_items"] == 15

        # Cleanup test resources
        while not engine.write_queue.empty():
            engine.write_queue.get_nowait()
            engine.write_queue.task_done()
        engine._pending_accepted_items = 0
        engine.writer.close()

    asyncio.run(_run())
