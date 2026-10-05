"""T1 regression specifications for streaming admission, durability, registry and config."""
import asyncio
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import threading

import duckdb
import pytest

import src.stream.runner as runner_module
import tools.service_supervisor as supervisor_module
from src.storage.config import init_tick_lake
from src.storage.registry import RegistryError, SymbolRegistry, init_registry
from src.storage.schema import QuoteTick
from src.stream.runner import StreamingEngine
from tests.support.lake_assertions import read_final_rows


TS = datetime(2026, 10, 2, 14, 30, tzinfo=timezone.utc)


def _capital_tick(price: float, epic: str = "AAPL"):
    return {"epic": epic, "price": price, "timestamp": TS, "bid": price - 0.01, "ask": price + 0.01}


async def _cleanup_engine(engine, worker_task=None):
    engine.stop()
    if worker_task is not None and not worker_task.done():
        worker_task.cancel()
        try:
            await worker_task
        except (asyncio.CancelledError, Exception):
            pass
    try:
        await engine.shutdown(drain_timeout=0.1)
    except Exception:
        # On a deliberately failing-storage test, preserve the observed failure;
        # temporary lake/process cleanup is handled by pytest's tmp_path fixture.
        pass


def test_real_callback_waits_for_queue_capacity_without_loss(tmp_path):
    async def run():
        async def callback(price):
            await engine._handle_capital_tick(_capital_tick(price))
        lake_root = tmp_path / "callback-lake"
        engine = StreamingEngine(lake_root=lake_root, max_queue_size=1, flush_interval=0.01)
        engine.running = True
        entered_publish = asyncio.Event()
        release_publish = asyncio.Event()
        original_publish = engine.writer.publish_batch_async
        first = True

        async def gated_publish(batch):
            nonlocal first
            if first:
                first = False
                entered_publish.set()
                await release_publish.wait()
            return await original_publish(batch)

        engine.writer.publish_batch_async = gated_publish
        worker_task = asyncio.create_task(engine._lake_writer_worker())
        try:
            await callback(100.0)
            await asyncio.wait_for(entered_publish.wait(), timeout=2.0)
            await callback(101.0)

            third = asyncio.create_task(callback(102.0))
            loop_progress = asyncio.Event()
            asyncio.get_running_loop().call_soon(loop_progress.set)
            await asyncio.wait_for(loop_progress.wait(), timeout=1.0)
            assert not third.done(), "valid callback returned successfully instead of awaiting queue capacity"
            assert engine.ticks_dropped == 0

            release_publish.set()
            await asyncio.wait_for(third, timeout=2.0)
            engine.stop()
            await asyncio.wait_for(engine.write_queue.join(), timeout=5.0)
            await asyncio.wait_for(worker_task, timeout=2.0)
            await engine.shutdown(drain_timeout=1.0)

            rows = read_final_rows(lake_root)
            assert sorted(row["price"] for row in rows) == [100.0, 101.0, 102.0]
            assert len({row["ingest_id"] for row in rows}) == 3
            assert engine.ticks_dropped == 0
            assert engine.ticks_committed == 3
        finally:
            release_publish.set()
            await _cleanup_engine(engine, worker_task)

    asyncio.run(run())


def test_callback_waiting_for_capacity_can_be_cancelled_without_false_drop(tmp_path):
    async def run():
        engine = StreamingEngine(lake_root=tmp_path / "cancel-callback-lake", max_queue_size=1)
        try:
            await engine._handle_capital_tick(_capital_tick(100.0))
            waiting = asyncio.create_task(engine._handle_capital_tick(_capital_tick(101.0)))
            loop_progress = asyncio.Event()
            asyncio.get_running_loop().call_soon(loop_progress.set)
            await asyncio.wait_for(loop_progress.wait(), timeout=1.0)
            assert not waiting.done(), "callback should be waiting for capacity before cancellation"
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            assert engine.write_queue.qsize() == 1
            assert engine.ticks_dropped == 0
            item = engine.write_queue.get_nowait()
            engine.write_queue.task_done()
            assert item[2] == 100.0
        finally:
            await _cleanup_engine(engine)

    asyncio.run(run())


def test_exhausted_storage_retries_retain_batch_until_recovery(tmp_path):
    async def run():
        lake_root = tmp_path / "retry-lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.01)
        engine.writer.retry_attempts = 3
        engine.writer.retry_backoff_base = 0.001
        engine.running = True
        attempts_exhausted = threading.Event()
        attempt_count = 0
        original_publish = engine.writer.publisher.publish_batch

        def fail_then_allow(*args, **kwargs):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count <= 3:
                if attempt_count == 3:
                    attempts_exhausted.set()
                raise OSError("temporary storage outage")
            return original_publish(*args, **kwargs)

        engine.writer.publisher.publish_batch = fail_then_allow
        worker_task = asyncio.create_task(engine._lake_writer_worker())
        try:
            await engine._handle_capital_tick(_capital_tick(110.0))
            assert await asyncio.to_thread(attempts_exhausted.wait, 3.0)
            await asyncio.wait_for(engine.storage_error_event.wait(), timeout=2.0)

            assert engine.write_queue.unfinished_tasks == 1
            assert engine.ticks_dropped == 0
            assert engine.ticks_committed == 0
            assert engine.pending_accepted_ticks == 1

            engine.writer.publisher.publish_batch = original_publish
            engine.resume_storage()
            await asyncio.wait_for(engine.write_queue.join(), timeout=5.0)
            engine.stop()
            await asyncio.wait_for(worker_task, timeout=2.0)
            await engine.shutdown(drain_timeout=1.0)

            rows = read_final_rows(lake_root)
            assert len(rows) == 1
            assert rows[0]["price"] == 110.0
            assert engine.ticks_committed == 1
            assert engine.ticks_dropped == 0
        finally:
            engine.writer.publisher.publish_batch = original_publish
            engine.resume_storage()
            await _cleanup_engine(engine, worker_task)

    asyncio.run(run())


def test_shutdown_reports_unsaved_accepted_batch_as_failed_drain(tmp_path):
    async def run():
        lake_root = tmp_path / "shutdown-failure-lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.01)
        engine.writer.retry_attempts = 1
        engine.running = True

        def fail_publish(*args, **kwargs):
            raise OSError("disk unavailable")

        original_publish = engine.writer.publisher.publish_batch
        engine.writer.publisher.publish_batch = fail_publish
        worker_task = asyncio.create_task(engine._lake_writer_worker())
        try:
            await engine._handle_capital_tick(_capital_tick(120.0))
            await asyncio.wait_for(engine.storage_error_event.wait(), timeout=3.0)
            with pytest.raises(runner_module.DrainFailedError):
                await engine.shutdown(drain_timeout=0.05)

            assert engine.drain_succeeded is False
            assert engine.pending_accepted_ticks == 1
            assert engine.write_queue.unfinished_tasks == 1
            assert engine.writer.status != "STOPPED"
            status = json.loads((lake_root / "_control" / "writer_status.json").read_text())
            assert status["status"] in {"DEGRADED", "DRAIN_FAILED"}
            assert status["queue_depth"] >= 1
        finally:
            engine.writer.publisher.publish_batch = original_publish
            engine.resume_storage()
            engine.running = True
            try:
                await asyncio.wait_for(engine.write_queue.join(), timeout=5.0)
            except Exception:
                pass
            await _cleanup_engine(engine, worker_task)

    asyncio.run(run())


def test_runner_committed_count_uses_verified_receipt_rows(tmp_path):
    async def run():
        lake_root = tmp_path / "receipt-count-lake"
        engine = StreamingEngine(lake_root=lake_root, flush_interval=0.01)
        engine.running = True
        worker_task = asyncio.create_task(engine._lake_writer_worker())
        try:
            await engine._handle_capital_tick(_capital_tick(130.0))
            # This malformed item reaches the accepted queue but is quarantined by the writer.
            await engine._handle_capital_tick({"epic": "AAPL", "price": float("nan"), "timestamp": TS})
            engine.stop()
            await asyncio.wait_for(engine.write_queue.join(), timeout=5.0)
            await asyncio.wait_for(worker_task, timeout=2.0)
            await engine.shutdown(drain_timeout=1.0)

            rows = read_final_rows(lake_root)
            assert len(rows) == 1
            assert engine.ticks_committed == 1
            assert engine.writer.metrics.total_quarantined == 1
        finally:
            await _cleanup_engine(engine, worker_task)

    asyncio.run(run())


@pytest.mark.parametrize("bad_value", [0, -1, float("nan"), float("inf")])
def test_invalid_stream_flush_settings_are_rejected(tmp_path, monkeypatch, bad_value):
    monkeypatch.setenv("STREAM_FLUSH_INTERVAL_SECONDS", str(bad_value))
    with pytest.raises(ValueError):
        StreamingEngine(lake_root=tmp_path / "bad-config-lake")


@pytest.mark.parametrize("bad_value", [0, -1])
def test_invalid_stream_batch_settings_are_rejected(tmp_path, monkeypatch, bad_value):
    monkeypatch.setenv("STREAM_MAX_BATCH_ROWS", str(bad_value))
    with pytest.raises(ValueError):
        StreamingEngine(lake_root=tmp_path / "bad-batch-lake")


def test_runner_defaults_and_environment_overrides_reach_real_writer(tmp_path, monkeypatch):
    monkeypatch.delenv("STREAM_FLUSH_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("STREAM_MAX_BATCH_ROWS", raising=False)
    engine = StreamingEngine(lake_root=tmp_path / "defaults-lake")
    try:
        assert engine.flush_interval == 5.0
        assert engine.max_batch_rows == 5000
        assert engine.writer.flush_interval_seconds == 5.0
        assert engine.writer.max_batch_rows == 5000
    finally:
        engine.stop()
        engine.writer.close()

    monkeypatch.setenv("STREAM_FLUSH_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("STREAM_MAX_BATCH_ROWS", "321")
    engine = StreamingEngine(lake_root=tmp_path / "overrides-lake")
    try:
        assert engine.flush_interval == 3.5
        assert engine.max_batch_rows == 321
        assert engine.writer.flush_interval_seconds == 3.5
        assert engine.writer.max_batch_rows == 321
    finally:
        engine.stop()
        engine.writer.close()


def test_runner_cli_overrides_env_and_reaches_engine(tmp_path, monkeypatch):
    captured = {}

    class CaptureEngine:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            self.running = False

        def stop(self):
            pass

    monkeypatch.setattr(runner_module, "StreamingEngine", CaptureEngine)
    monkeypatch.setattr(runner_module.sys, "argv", [
        "runner", "--lake-root", str(tmp_path / "cli-lake"),
        "--flush-interval", "4.25", "--max-batch-rows", "2345",
    ])
    monkeypatch.setenv("STREAM_FLUSH_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("STREAM_MAX_BATCH_ROWS", "321")

    original_run = runner_module.asyncio.run
    monkeypatch.setattr(runner_module.asyncio, "run", lambda coroutine: coroutine.close())
    try:
        runner_module.main()
    finally:
        monkeypatch.setattr(runner_module.asyncio, "run", original_run)

    assert captured["flush_interval"] == 4.25
    assert captured["max_batch_rows"] == 2345


def test_configured_batch_row_trigger_flushes_before_timer(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setenv("STREAM_MAX_BATCH_ROWS", "2")
        engine = StreamingEngine(lake_root=tmp_path / "row-trigger-lake", flush_interval=60.0)
        published = asyncio.Event()
        original_publish = engine.writer.publish_batch_async

        async def observe_publish(batch):
            published.set()
            return await original_publish(batch)

        engine.writer.publish_batch_async = observe_publish
        engine.running = True
        worker_task = asyncio.create_task(engine._lake_writer_worker())
        try:
            await engine._handle_capital_tick(_capital_tick(140.0))
            await engine._handle_capital_tick(_capital_tick(141.0))
            await asyncio.wait_for(published.wait(), timeout=1.0)
            assert engine.max_batch_rows == 2
            assert engine.writer.max_batch_rows == 2
        finally:
            await _cleanup_engine(engine, worker_task)

    asyncio.run(run())


def test_supervisor_passes_effective_stream_settings_to_child(tmp_path, monkeypatch):
    captured = {}

    class FakeProcess:
        pid = 765432
        returncode = None

        def poll(self):
            return self.returncode

        def terminate(self):
            self.returncode = 0

        def wait(self, timeout=None):
            self.returncode = 0
            return self.returncode

        def kill(self):
            self.returncode = -9

    def fake_popen(command, **kwargs):
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return FakeProcess()

    monkeypatch.setattr(supervisor_module.subprocess, "Popen", fake_popen)
    supervisor = supervisor_module.ProcessSupervisor(
        name="audit-streamer",
        module="src.stream.runner",
        module_args=["--flush-interval", "5", "--max-batch-rows", "5000"],
        extra_env={"STREAM_FLUSH_INTERVAL_SECONDS": "5", "STREAM_MAX_BATCH_ROWS": "5000"},
        log_dir=tmp_path / "logs",
        handle_signals=False,
    )
    supervisor._start_child()
    try:
        assert captured["command"][-4:] == ["--flush-interval", "5", "--max-batch-rows", "5000"]
        assert captured["env"]["STREAM_FLUSH_INTERVAL_SECONDS"] == "5"
        assert captured["env"]["STREAM_MAX_BATCH_ROWS"] == "5000"
    finally:
        supervisor._stop_child(timeout=0.1)


@pytest.mark.parametrize("inventory_state", ["fresh_empty", "empty", "all_inactive"])
def test_empty_registry_start_does_not_subscribe_or_authenticate(tmp_path, monkeypatch, inventory_state):
    async def run():
        lake_root = tmp_path / f"{inventory_state}-registry-lake"
        if inventory_state != "fresh_empty":
            init_tick_lake(lake_root)
            init_registry(lake_root)
        if inventory_state == "all_inactive":
            registry = SymbolRegistry(root=lake_root)
            registry.add_symbol("AAPL", capital_ticker="AAPL")
            registry.toggle_symbol("AAPL", active=False)
        created = asyncio.Event()
        provider_ref = {}

        class FakeProvider:
            def __init__(self, epics, on_tick_callback, **kwargs):
                self.epics = list(epics)
                self.on_tick_callback = on_tick_callback
                self.options = kwargs
                self.stop_event = asyncio.Event()
                provider_ref["instance"] = self
                created.set()

            async def start(self):
                await self.stop_event.wait()

            def stop(self):
                self.stop_event.set()

            async def update_subscriptions(self, epics):
                self.epics = list(epics)
                return True

        monkeypatch.setattr(runner_module, "MockStreamer", FakeProvider)
        engine = StreamingEngine(lake_root=lake_root, mock_mode=True)
        task = asyncio.create_task(engine.start())
        try:
            await asyncio.wait_for(created.wait(), timeout=2.0)
            provider = provider_ref["instance"]
            assert provider.epics == []
            assert engine.active_streaming_symbols == set()
            assert engine.registry.get_active_symbols() == []
            engine.stop()
            await asyncio.wait_for(task, timeout=3.0)
        finally:
            if not task.done():
                engine.stop()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            if engine.writer and engine.writer.status != "STOPPED":
                await engine.writer.close_async()

    asyncio.run(run())


@pytest.mark.parametrize("registry_state", ["missing", "corrupt"])
def test_established_lake_registry_errors_fail_closed_before_provider_start(tmp_path, monkeypatch, registry_state):
    async def run():
        lake_root = tmp_path / f"established-{registry_state}-lake"
        init_tick_lake(lake_root)
        registry_path = lake_root / "_control" / "registry.json"
        if registry_state == "corrupt":
            registry_path.parent.mkdir(parents=True, exist_ok=True)
            registry_path.write_text("{not valid json", encoding="utf-8")

        class ForbiddenProvider:
            def __init__(self, *args, **kwargs):
                raise AssertionError("provider startup/authentication must not occur")

        monkeypatch.setattr(runner_module, "MockStreamer", ForbiddenProvider)
        engine = StreamingEngine(lake_root=lake_root, mock_mode=True)
        try:
            with pytest.raises(RegistryError):
                await engine.start()
        finally:
            if engine.writer and engine.writer.status != "STOPPED":
                await engine.writer.close_async()

    asyncio.run(run())
