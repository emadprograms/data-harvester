"""
24/7 Capital.com Exclusive Streaming Runner.
Connects to Capital.com WebSocket, receives real-time tick quotes,
and flushes them into the partitioned Parquet Tick Lake.
Supports dynamic subscription reload without dropping the WebSocket connection.
"""
import asyncio
import logging
import math
from pathlib import Path
import signal
import sys
import time
from datetime import datetime, timezone
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from src.storage.registry import RegistryError
from src.stream.capital_stream import CapitalStreamer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("stream_runner")


class DrainFailedError(RuntimeError):
    """Raised when accepted stream work cannot be durably drained before shutdown."""


DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS = 5.0
DEFAULT_STREAM_MAX_BATCH_ROWS = 5000

# Process exit codes the supervisor distinguishes (SCHED-02/03).
EXIT_START_FAILED = 1
EXIT_OUTSIDE_WINDOW = 3
EXIT_DRAIN_FAILED = 4


def check_ingestion_window(now=None, mock_mode: bool = False):
    """(may_start, message) for the weekday 04:00-20:00 ET ingestion window.

    ``mock_mode`` never authenticates or subscribes to a provider, so it is exempt
    from the gate; the honest window state is still returned for the log line.
    """
    current = now if now is not None else now_et()
    message = describe_window(current)
    if mock_mode or is_eligible(current):
        return True, message
    return False, message


def _is_fresh_lake_path(path: Union[str, Path]) -> bool:
    """A lake can bootstrap an empty registry only before any lake artifact exists."""
    root = Path(path).resolve()
    if not root.exists():
        return True
    return root.is_dir() and not any(root.iterdir())


def _resolve_lake_root(explicit=None) -> Path:
    """Resolve the Parquet tick lake root. v5.0 has no disk-database backend."""
    from src.storage.config import StorageConfigError, resolve_tick_lake_root

    try:
        return Path(resolve_tick_lake_root(explicit)).resolve()
    except StorageConfigError as exc:
        raise ValueError(
            "A Parquet tick lake root is required: pass lake_root= or set "
            "TICK_LAKE_ROOT/DATA_DIR. v5.0 has no disk-database backend."
        ) from exc


def _positive_float(value: Any, env_name: str, default: float) -> float:
    raw = value if value is not None else os.environ.get(env_name, default)
    try:
        resolved = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{env_name} must be a finite positive number, got {raw!r}") from exc
    if not math.isfinite(resolved) or resolved <= 0:
        raise ValueError(f"{env_name} must be a finite positive number, got {raw!r}")
    return resolved


def _positive_int(value: Any, env_name: str, default: int) -> int:
    raw = value if value is not None else os.environ.get(env_name, default)
    try:
        numeric = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{env_name} must be a positive integer, got {raw!r}") from exc
    if not math.isfinite(numeric) or numeric <= 0 or not numeric.is_integer():
        raise ValueError(f"{env_name} must be a positive integer, got {raw!r}")
    return int(numeric)


from src.utils.notifications import (
    DRAIN_FAILED,
    SESSION_STARTED,
    SESSION_START_FAILED,
    SESSION_STOPPED,
    notify_detached,
)
from src.utils.session_window import describe as describe_window
from src.utils.session_window import is_eligible, now_et

from src.stream.fake_provider import FakeProvider


class MockStreamer(FakeProvider):
    """Mock streamer generating synthetic ticks for offline/test execution without real network/credentials."""
    def __init__(
        self,
        epics: List[str],
        on_tick_callback,
        ticks_per_sec: float = 50.0,
        gap_ledger=None,
        replay_capable: bool = False,
    ):
        super().__init__(
            epics=epics,
            on_tick_callback=on_tick_callback,
            ticks_per_sec=ticks_per_sec,
            provider_name="MOCK_CAPITAL",
            gap_ledger=gap_ledger,
            replay_capable=replay_capable,
        )


class BoundedWriteQueue(asyncio.Queue):
    """Asyncio Queue with an unfinished_tasks property for honest task_done tracking."""
    @property
    def unfinished_tasks(self) -> int:
        return self._unfinished_tasks


class StreamingEngine:
    """Master streaming orchestrator saving pure tick-by-tick data into the partitioned Parquet tick lake."""
    def __init__(
        self,
        flush_interval=None,
        lake_root=None,
        max_queue_size=10000,
        max_batch_rows=None,
        writer_id="writer_1",
        registry_poll_interval=1.0,
        registry_debounce_interval=0.05,
        mock_mode: bool = False,
        drain_delay: float = 0.0,
        mock_ticks_per_sec: float = 50.0,
    ):
        self.lake_root = lake_root
        self.flush_interval = _positive_float(
            flush_interval, "STREAM_FLUSH_INTERVAL_SECONDS", DEFAULT_STREAM_FLUSH_INTERVAL_SECONDS
        )
        self.max_batch_rows = _positive_int(
            max_batch_rows, "STREAM_MAX_BATCH_ROWS", DEFAULT_STREAM_MAX_BATCH_ROWS
        )
        self.max_queue_size = _positive_int(max_queue_size, "STREAM_MAX_QUEUE_SIZE", 10000)
        self.mock_mode = mock_mode
        self.drain_delay = float(drain_delay)
        self.mock_ticks_per_sec = float(mock_ticks_per_sec)
        self.running = False
        self.write_queue = BoundedWriteQueue(maxsize=self.max_queue_size)
        self.reload_event = asyncio.Event()
        self.db_conn = None
        self.total_ticks_saved = 0
        self.total_ticks_persisted = 0
        self.ticks_received = 0
        self.ticks_enqueued = 0
        self.ticks_dropped = 0
        self._pending_accepted_items = 0
        # SCHED-04: the window owns admission. Once it closes, no new tick is
        # accepted; everything already accepted still drains exactly once.
        self.admission_open = True
        # SYMB-01: the lake registry is the only symbol authority. Stream
        # identifiers that are not in it are rejected at the callback boundary.
        self.ticks_rejected_out_of_scope = 0
        self._rejected_symbols_logged = set()
        self.storage_error_event = asyncio.Event()
        self._storage_resume_event = asyncio.Event()
        self._storage_resume_event.set()
        self._storage_error: Optional[BaseException] = None
        self.drain_succeeded = True
        self._retained_worker_buffer: List[Any] = []
        self.writer = None
        self.lake_writer = None
        self.registry = None
        self.registry_poll_interval = float(registry_poll_interval)
        self.registry_debounce_interval: float = float(registry_debounce_interval)
        self._is_shutdown = False

        self.lake_root = _resolve_lake_root(self.lake_root)
        fresh_lake = _is_fresh_lake_path(self.lake_root)
        from src.storage.parquet_writer import TickLakeWriter
        self.writer = TickLakeWriter(
            root=self.lake_root,
            writer_id=writer_id,
            flush_interval_seconds=self.flush_interval,
            max_queue_size=self.max_queue_size,
            max_batch_rows=self.max_batch_rows,
        )
        self.lake_writer = self.writer
        try:
            from src.storage.registry import SymbolRegistry, init_registry
            self.registry = SymbolRegistry(root=self.lake_root)
            if fresh_lake and not self.registry.path.exists():
                init_registry(self.lake_root)
        except Exception:
            self.writer.close()
            self.writer = None
            self.lake_writer = None
            raise

        self.capital_streamer = None
        self.active_streaming_symbols = set()
        self.epic_to_display = {}
        self._subscriptions_initialized = False

        self.gap_ledger = None
        self._active_overflow_gap_id: Optional[str] = None
        from src.stream.gap_ledger import GapLedger
        self.gap_ledger = GapLedger(self.lake_root)

    def _record_drop(self, count: int = 1, symbol: str = "all", reason: str = "BUFFER_OVERFLOW") -> None:
        """Increments drop counter and synchronizes drop count to writer metrics immediately."""
        self.ticks_dropped += count
        if self.writer is not None and hasattr(self.writer, "_metrics"):
            self.writer._metrics.total_dropped = self.ticks_dropped
        if self.gap_ledger is not None:
            if self._active_overflow_gap_id is None:
                try:
                    now = datetime.now(timezone.utc)
                    self._active_overflow_gap_id = self.gap_ledger.open_gap(
                        provider="STREAMING_ENGINE",
                        symbol=symbol,
                        reason=reason,
                        start_time=now,
                        status="LOSS_UNKNOWN",
                        details={"dropped_count": count},
                    )
                except Exception:
                    pass

    @property
    def ticks_committed(self) -> int:
        return self.total_ticks_persisted

    @property
    def pending_accepted_ticks(self) -> int:
        """Queue items still owned by the writer, whether queued or in-flight."""
        return self._pending_accepted_items

    def _registry_identifiers(self) -> Dict[str, str]:
        """{stream identifier -> canonical display symbol} for active registry entries."""
        if self.registry is None:
            return {}
        try:
            entries = self.registry.get_active_symbols()
        except Exception:
            return {}
        mapping: Dict[str, str] = {}
        for entry in entries:
            mapping[str(entry.symbol).strip().upper()] = entry.symbol
            if entry.capital_ticker:
                mapping[str(entry.capital_ticker).strip().upper()] = entry.symbol
        return mapping

    def _resolve_stream_symbol(self, raw_symbol):
        """Canonical display symbol for a stream identifier, or None when unauthorized.

        Once the subscription set is known (after `start()`/`reload_symbols()`),
        that set is the authority. Before then, the registry itself stands in for
        it — a lake with no active symbols has no authority and accepts as before.
        """
        key = str(raw_symbol or "").strip().upper()
        if not key:
            return None
        if self._subscriptions_initialized or self.active_streaming_symbols:
            if key not in self.active_streaming_symbols:
                return None
            return self.epic_to_display.get(key, key)
        authorized = self._registry_identifiers()
        if authorized:
            return authorized.get(key)
        return self.epic_to_display.get(key, key)

    def _reject_out_of_scope(self, raw_symbol) -> None:
        """Count and log (once per identifier) an unsolicited stream symbol."""
        self.ticks_rejected_out_of_scope += 1
        key = str(raw_symbol or "").strip().upper()
        if key not in self._rejected_symbols_logged:
            self._rejected_symbols_logged.add(key)
            logger.warning(
                "Rejected out-of-scope symbol %r: the lake registry is the only symbol authority",
                key,
            )

    def close_admission(self, reason: str = "window closed") -> None:
        """Stop accepting new ticks (window closed); already-accepted work still drains."""
        if self.admission_open:
            self.admission_open = False
            logger.info("Tick admission closed: %s", reason)

    def _enqueue_tick(self, tick_tuple) -> bool:
        """Best-effort synchronous admission; returns False and counts a pre-admission drop on full."""
        self.ticks_received += 1
        if not self.admission_open:
            sym = tick_tuple[1] if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1 else "all"
            self._record_drop(1, symbol=str(sym), reason="WINDOW_CLOSED")
            return False
        try:
            self.write_queue.put_nowait(tick_tuple)
        except asyncio.QueueFull:
            sym = tick_tuple[1] if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1 else "all"
            self._record_drop(1, symbol=str(sym), reason="BUFFER_OVERFLOW")
            logger.warning("write_queue full (%d), synchronously rejected tick: %s", self.write_queue.maxsize, sym)
            return False
        self.ticks_enqueued += 1
        self._pending_accepted_items += 1
        return True

    async def _enqueue_tick_async(self, tick_tuple) -> None:
        """Lossless async admission that waits for bounded queue capacity."""
        self.ticks_received += 1
        if not self.admission_open:
            sym = tick_tuple[1] if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1 else "all"
            self._record_drop(1, symbol=str(sym), reason="WINDOW_CLOSED")
            return
        await self.write_queue.put(tick_tuple)
        self.ticks_enqueued += 1
        self._pending_accepted_items += 1

    def _pause_storage(self, error: BaseException) -> None:
        self._storage_error = error
        self.storage_error_event.set()
        self._storage_resume_event.clear()
        if self.writer is not None:
            try:
                self.writer.set_operational_status("DEGRADED", queue_depth=self.pending_accepted_ticks)
            except Exception:
                pass

    def resume_storage(self) -> None:
        """Resume retries for a retained in-flight batch after storage recovery."""
        self._storage_error = None
        self.storage_error_event.clear()
        if self.writer is not None:
            try:
                self.writer.set_operational_status("RUNNING", queue_depth=self.pending_accepted_ticks)
            except Exception:
                pass
        self._storage_resume_event.set()

    def _enqueue_bar(self, bar_tuple) -> bool:
        """Backward-compatible best-effort synchronous bar admission."""
        return self._enqueue_tick(bar_tuple)

    async def _handle_capital_tick(self, tick):
        """Feeds a tick from Capital.com directly into the write queue, filtering out excluded/purged assets."""
        if isinstance(tick, dict):
            raw_epic = tick.get("epic", "") or tick.get("symbol", "")
            symbol = self._resolve_stream_symbol(raw_epic)
            if symbol is None:
                self._reject_out_of_scope(raw_epic)
                return
            price = float(tick.get("price", 0.0))
            ts = tick.get("timestamp")
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(ts, datetime) else str(ts)
            bid = tick.get("bid")
            ask = tick.get("ask")
            vol = float(tick.get("volume", 1.0))
            src = tick.get("source", "CAPITAL")
            sess = tick.get("session", "REG")
            ingest_id = tick.get("ingest_id")
            if ingest_id:
                tick_tuple = (ts_str, symbol, price, vol, bid, ask, src, sess, str(ingest_id))
            else:
                tick_tuple = (ts_str, symbol, price, vol, bid, ask, src, sess)
        elif hasattr(tick, "symbol") and hasattr(tick, "timestamp"):
            sym = getattr(tick, "symbol")
            if self._resolve_stream_symbol(sym) is None:
                self._reject_out_of_scope(sym)
                return
            tick_tuple = tick
        else:
            tick_tuple = tick
            if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1:
                sym = tick_tuple[1]
                if self._resolve_stream_symbol(sym) is None:
                    self._reject_out_of_scope(sym)
                    return
        await self._enqueue_tick_async(tick_tuple)

    async def _lake_writer_worker(self):
        """Persist accepted queue items; retain ownership until a receipt confirms commit."""
        buffer = list(self._retained_worker_buffer)
        self._retained_worker_buffer.clear()
        last_flush = time.monotonic()
        cancelled = False

        try:
            while self.running or not self.write_queue.empty() or buffer:
                # A failed batch remains at the head of this worker-owned buffer. New
                # callbacks may enqueue only until the bounded queue fills.
                await self._storage_resume_event.wait()

                if len(buffer) < self.max_batch_rows:
                    wait_time = 0.2
                    if buffer:
                        remaining = self.flush_interval - (time.monotonic() - last_flush)
                        wait_time = min(wait_time, max(0.001, remaining))
                    try:
                        tick = await asyncio.wait_for(self.write_queue.get(), timeout=wait_time)
                        buffer.append(tick)
                        while len(buffer) < self.max_batch_rows:
                            try:
                                buffer.append(self.write_queue.get_nowait())
                            except asyncio.QueueEmpty:
                                break
                    except asyncio.TimeoutError:
                        pass

                now = time.monotonic()
                should_flush = bool(buffer) and (
                    len(buffer) >= self.max_batch_rows
                    or now - last_flush >= self.flush_interval
                    or not self.running
                )
                if not should_flush:
                    continue

                batch = list(buffer)
                publish_task = asyncio.create_task(self.writer.publish_batch_async(batch))
                try:
                    try:
                        receipt = await asyncio.shield(publish_task)
                    except asyncio.CancelledError:
                        cancelled = True
                        receipt = await publish_task
                except Exception as exc:
                    logger.error("Lake writer worker retained %d accepted items after storage failure: %s", len(batch), exc)
                    self._pause_storage(exc)
                    if cancelled:
                        self._retained_worker_buffer = list(buffer)
                        buffer.clear()
                        raise asyncio.CancelledError()
                    continue

                batch_len = len(buffer)
                buffer.clear()
                self._pending_accepted_items = max(0, self._pending_accepted_items - batch_len)
                committed_rows = int(receipt.row_count)
                self.total_ticks_persisted += committed_rows
                self.total_ticks_saved += committed_rows
                for _ in range(batch_len):
                    self.write_queue.task_done()
                if self._active_overflow_gap_id is not None and self.write_queue.empty() and self.gap_ledger is not None:
                    try:
                        self.gap_ledger.close_gap(
                            self._active_overflow_gap_id,
                            end_time=datetime.now(timezone.utc),
                            details_update={"final_dropped_count": self.ticks_dropped},
                        )
                    except Exception:
                        pass
                    self._active_overflow_gap_id = None
                self._storage_error = None
                self.storage_error_event.clear()
                last_flush = time.monotonic()
                if cancelled:
                    break
        except asyncio.CancelledError:
            if buffer:
                self._retained_worker_buffer = list(buffer)
            raise

        if cancelled:
            raise asyncio.CancelledError()

    def trigger_reload(self):
        """Signals the background watcher to reload symbol subscriptions immediately."""
        self.reload_event.set()

    async def reload_symbols(self, symbols_override=None):
        """Re-reads streaming_symbol_map and updates live Capital.com subscriptions on the fly."""
        self._subscriptions_initialized = True
        if symbols_override is not None:
            capital_symbols = list(symbols_override)
            self.active_streaming_symbols = set(capital_symbols)
            self.epic_to_display = {s: s for s in capital_symbols}
        elif self.registry is not None:
            try:
                active_entries = self.registry.get_active_symbols()
            except RegistryError as e:
                logger.warning(f"Failed to load active symbols from registry: {e}")
                return False
            capital_symbols = []
            self.active_streaming_symbols = set()
            self.epic_to_display = {}
            for entry in active_entries:
                sym = entry.symbol
                self.active_streaming_symbols.add(sym)
                c_ticker = entry.capital_ticker or sym
                self.active_streaming_symbols.add(c_ticker)
                capital_symbols.append(c_ticker)
                self.epic_to_display[c_ticker] = sym
                self.epic_to_display[sym] = sym
        logger.info(f"🔄 Reloading active Capital.com streaming symbols ({len(capital_symbols)}): {capital_symbols}")
        if self.capital_streamer:
            success = await self.capital_streamer.update_subscriptions(capital_symbols)
            return success
        return False

    async def _symbol_watcher_worker(self, poll_interval=None):
        """Background worker that polls registry version and watches for reload signals."""
        last_version = None
        if self.registry:
            try:
                last_version = self.registry.version
            except Exception:
                pass

        signal_paths = []
        if self.lake_root:
            p = Path(self.lake_root)
            signal_paths.append(p / ".stream_reload.signal")
            signal_paths.append(p / "_control" / ".stream_reload.signal")

        while self.running:
            try:
                interval = poll_interval or getattr(self, "registry_poll_interval", 1.0)
                slice_time = min(0.01, interval)
                elapsed = 0.0

                while elapsed < interval and self.running:
                    # 1 & 2. Check explicit asyncio reload_event or signal file hint
                    signal_detected = self.reload_event.is_set()
                    if not signal_detected:
                        for sp in signal_paths:
                            if sp.exists():
                                signal_detected = True
                                break

                    if signal_detected:
                        # Coalesce a burst until the signal files have been quiet
                        # for the configured debounce window. A fixed sleep can
                        # consume only the first edge and then reload repeatedly
                        # while a sustained registry-update storm is still writing.
                        debounce = max(0.0, float(getattr(self, "registry_debounce_interval", 0.05)))
                        if debounce > 0:
                            def signal_state():
                                state = []
                                for signal_path in signal_paths:
                                    try:
                                        stat = signal_path.stat()
                                        state.append((str(signal_path), stat.st_mtime_ns, stat.st_size))
                                    except FileNotFoundError:
                                        state.append((str(signal_path), None, None))
                                    except OSError:
                                        state.append((str(signal_path), "unavailable", None))
                                return tuple(state)

                            started_at = time.monotonic()
                            last_change = started_at
                            previous_state = signal_state()
                            max_debounce = max(debounce * 4.0, 0.25)
                            while self.running:
                                elapsed = time.monotonic() - started_at
                                if elapsed >= max_debounce:
                                    break
                                await asyncio.sleep(min(0.01, max_debounce - elapsed))
                                current_state = signal_state()
                                now = time.monotonic()
                                if current_state != previous_state:
                                    previous_state = current_state
                                    last_change = now
                                elif now - last_change >= debounce:
                                    break

                        # Clean up signal files and event
                        for sp in signal_paths:
                            try:
                                if sp.exists():
                                    sp.unlink()
                            except Exception:
                                pass
                        self.reload_event.clear()

                        logger.info("⚡ Live reload triggered via signal hint or reload_event.")
                        await self.reload_symbols()
                        if self.registry:
                            try:
                                last_version = self.registry.version
                            except Exception:
                                pass
                        break

                    await asyncio.sleep(slice_time)
                    elapsed += slice_time

                # 3. Periodic registry version polling
                if self.running and self.registry:
                    try:
                        curr_version = self.registry.version
                        if last_version is not None and curr_version > last_version:
                            logger.info(f"🔄 Detected registry version bump ({last_version} -> {curr_version}). Reloading...")
                            last_version = curr_version
                            await self.reload_symbols()
                        elif last_version is None:
                            last_version = curr_version
                    except Exception as e:
                        logger.debug(f"Error checking registry version: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Symbol watcher error: {e}")
                await asyncio.sleep(0.1)

    async def start(self):
        self.running = True
        # Discover symbols from the registry - the single symbol authority.
        self._subscriptions_initialized = True
        capital_symbols = []
        self.active_streaming_symbols = set()
        self.epic_to_display = {}

        if self.registry is not None:
            try:
                active_entries = self.registry.get_active_symbols()
            except RegistryError:
                self.running = False
                raise
            for entry in active_entries:
                sym = entry.symbol
                self.active_streaming_symbols.add(sym)
                c_ticker = entry.capital_ticker or sym
                self.active_streaming_symbols.add(c_ticker)
                capital_symbols.append(c_ticker)
                self.epic_to_display[c_ticker] = sym
                self.epic_to_display[sym] = sym
        logger.info(f"🎯 Target Capital.com symbols for live quotes ({len(capital_symbols)}): {capital_symbols[:6]}...")

        # Initialize Capital.com streamer (or MockStreamer in mock_mode)
        if self.mock_mode:
            self.capital_streamer = MockStreamer(
                epics=capital_symbols,
                on_tick_callback=self._handle_capital_tick,
                ticks_per_sec=self.mock_ticks_per_sec,
                gap_ledger=self.gap_ledger,
            )
        else:
            self.capital_streamer = CapitalStreamer(
                epics=capital_symbols,
                on_tick_callback=self._handle_capital_tick
            )

        notify_detached(
            SESSION_STARTED,
            detail=(
                f"{len(capital_symbols)} subscribed symbols; "
                f"{'MOCK provider' if self.mock_mode else 'Capital.com live'}; "
                f"{describe_window(now_et())}"
            ),
            fields={
                "Symbols": len(capital_symbols),
                "Mode": "MOCK" if self.mock_mode else "LIVE",
                "Window": "weekdays 04:00-20:00 ET",
            },
        )

        writer_worker_task = asyncio.create_task(self._lake_writer_worker())

        tasks = [
            writer_worker_task,
            asyncio.create_task(self.capital_streamer.start()),
            asyncio.create_task(self._symbol_watcher_worker()),
        ]

        logger.info("🚀 24/7 Capital.com Tick Streaming Engine started successfully.")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Streaming Engine stopping...")
        finally:
            await self.shutdown()

    def stop(self):
        logger.info("Stopping Streaming Engine...")
        self.running = False
        if self.capital_streamer:
            self.capital_streamer.stop()
        if self._active_overflow_gap_id is not None and self.gap_ledger is not None:
            try:
                self.gap_ledger.close_gap(
                    self._active_overflow_gap_id,
                    end_time=datetime.now(timezone.utc),
                    details_update={"final_dropped_count": self.ticks_dropped},
                )
            except Exception:
                pass
            self._active_overflow_gap_id = None

    async def shutdown(self, drain_timeout: float = 10.0):
        """Stop providers and report failure rather than acknowledge unsaved work."""
        if getattr(self, "_is_shutdown", False):
            return
        self.running = False
        self._is_shutdown = True
        if self.drain_delay > 0:
            await asyncio.sleep(self.drain_delay)

        if self.capital_streamer:
            self.capital_streamer.stop()

        if self._active_overflow_gap_id is not None and self.gap_ledger is not None:
            try:
                self.gap_ledger.close_gap(
                    self._active_overflow_gap_id,
                    end_time=datetime.now(timezone.utc),
                    details_update={"final_dropped_count": self.ticks_dropped},
                )
            except Exception:
                pass
            self._active_overflow_gap_id = None

        try:
            await asyncio.wait_for(self.write_queue.join(), timeout=drain_timeout)
        except asyncio.TimeoutError as exc:
            self.drain_succeeded = False
            message = (
                f"Streaming drain timed out with {self.pending_accepted_ticks} accepted items pending "
                f"({getattr(self.write_queue, 'unfinished_tasks', 0)} unfinished queue tasks)"
            )
            if self.gap_ledger is not None:
                try:
                    now = datetime.now(timezone.utc)
                    self.gap_ledger.record_gap(
                        provider="STREAMING_ENGINE",
                        symbol="all",
                        reason="SHUTDOWN_UNFLUSHED",
                        start_time=now,
                        end_time=now,
                        status="LOSS_UNKNOWN",
                        details={"pending_items": self.pending_accepted_ticks},
                    )
                except Exception:
                    pass
            if self.writer is not None:
                try:
                    self.writer.set_operational_status("DRAIN_FAILED", queue_depth=self.pending_accepted_ticks)
                except Exception:
                    pass
            notify_detached(DRAIN_FAILED, detail=message)
            raise DrainFailedError(message) from exc

        if self.writer is not None:
            try:
                await self.writer.flush_async()
                await self.writer.close_async()
            except Exception as exc:
                self.drain_succeeded = False
                if self.gap_ledger is not None:
                    try:
                        now = datetime.now(timezone.utc)
                        self.gap_ledger.record_gap(
                            provider="STREAMING_ENGINE",
                            symbol="all",
                            reason="SHUTDOWN_UNFLUSHED",
                            start_time=now,
                            end_time=now,
                            status="LOSS_UNKNOWN",
                            details={"pending_items": self.pending_accepted_ticks},
                        )
                    except Exception:
                        pass
                try:
                    self.writer.set_operational_status("DRAIN_FAILED", queue_depth=self.pending_accepted_ticks)
                except Exception:
                    pass
                notify_detached(
                    DRAIN_FAILED,
                    detail=f"Streaming writer failed during final drain: {exc}",
                )
                raise DrainFailedError(f"Streaming writer failed during final drain: {exc}") from exc

        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None
        self.drain_succeeded = True
        notify_detached(
            SESSION_STOPPED,
            detail="graceful drain complete; every accepted tick is durable in the lake",
            fields={"Dropped": self.ticks_dropped},
        )


async def window_watchdog(stop_event, clock=None, poll_seconds: float = 5.0) -> None:
    """Ask the runner to stop when the ingestion window closes (SCHED-04).

    Admission stops the moment the window ends; the normal shutdown path then
    drains every accepted tick exactly once.
    """
    clock = clock or now_et
    while True:
        if not is_eligible(clock()):
            logger.info(
                "Ingestion window closed (%s); stopping admission and draining.",
                describe_window(clock()),
            )
            stop_event.set()
            return
        await asyncio.sleep(poll_seconds)


def _run_streamer(engine, lake_root=None, clock=None, watchdog_poll_seconds: float = 5.0,
                  watch_window: bool = True) -> int:
    """Run the engine until a signal, mapping the outcome to an exit code.

    Exit codes (imported by the supervisor, SCHED-02/04):
    ``0`` clean drain, ``EXIT_DRAIN_FAILED`` accepted ticks were lost,
    ``EXIT_START_FAILED`` the engine never reached ingestion.
    """

    async def _async_main():
        stop_event = asyncio.Event()

        def _shutdown_signal_handler(sig_num=None, frame=None):
            sig_name = signal.Signals(sig_num).name if sig_num is not None else "EXIT"
            logger.info(f"Signal {sig_name} caught by _shutdown_signal_handler, initiating graceful shutdown...")
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: _shutdown_signal_handler(s))
            except NotImplementedError:
                signal.signal(sig, lambda s, f: _shutdown_signal_handler(s, f))

        engine_task = asyncio.create_task(engine.start())
        stop_task = asyncio.create_task(stop_event.wait())

        workers = [engine_task, stop_task]
        # Mock runs intentionally bypass the window (they never authenticate or
        # subscribe), so the watchdog only governs live runs.
        live_window = watch_window and not getattr(engine, "mock_mode", False)
        if live_window:
            workers.append(
                asyncio.create_task(
                    window_watchdog(stop_event, clock=clock, poll_seconds=watchdog_poll_seconds)
                )
            )

        done, pending = await asyncio.wait(
            workers,
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_event.is_set():
            engine.stop()
            # StreamingEngine.start() owns the single shutdown/drain path in its
            # finally block. Await it instead of closing the same writer here;
            # double shutdown can race the executor and report a false drain failure.
            if engine_task in pending:
                try:
                    await engine_task
                except Exception as exc:
                    logger.error("Streaming engine failed while draining on shutdown: %s", exc)
                    raise
                pending.discard(engine_task)

        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        if engine_task in done and not stop_event.is_set():
            # The engine finished on its own: surface whatever ended it.
            await engine_task

    try:
        asyncio.run(_async_main())
    except DrainFailedError as exc:
        notify_detached(DRAIN_FAILED, detail=str(exc))
        logger.error("Streaming engine drained unsaved work: %s", exc)
        return EXIT_DRAIN_FAILED
    except KeyboardInterrupt:
        logger.info("Streaming Engine process stopped.")
        return 0
    except Exception as exc:
        notify_detached(
            SESSION_START_FAILED,
            detail=f"{type(exc).__name__}: {exc}",
        )
        logger.exception("Streaming engine failed to start")
        return EXIT_START_FAILED
    return 0


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Capital.com Tick Streaming Engine (weekdays 04:00-20:00 ET)")
    parser.add_argument("--lake-root", type=str, default=None, help="Path to tick lake root")
    parser.add_argument("--writer-id", type=str, default="writer_1", help="Writer ID for LakePublisher")
    parser.add_argument("--flush-interval", type=float, default=None, help="Batch flush interval in seconds")
    parser.add_argument("--max-batch-rows", type=int, default=None, help="Maximum rows per publication batch")
    parser.add_argument("--max-queue-size", type=int, default=10000, help="Max bounded queue size")
    parser.add_argument("--mock", action="store_true", help="Run with MockStreamer generating synthetic ticks (never subscribes; exempt from the window gate)")
    parser.add_argument("--fail-immediately", action="store_true", help="Exit immediately with returncode 1 (for supervisor flapping chaos)")
    parser.add_argument("--drain-delay", type=float, default=0.0, help="Artificial delay during shutdown drain in seconds")
    parser.add_argument("--ticks-per-sec", type=float, default=50.0, help="Tick production rate in mock mode")

    args, _ = parser.parse_known_args()

    if args.fail_immediately:
        sys.exit(EXIT_START_FAILED)

    allowed, window_message = check_ingestion_window(mock_mode=args.mock)
    if not allowed:
        logger.error("Refusing to start outside the ingestion window (weekdays 04:00-20:00 ET): %s", window_message)
        print(
            "Refusing to start: outside the ingestion window (weekdays 04:00-20:00 ET). "
            f"{window_message}",
            file=sys.stderr,
        )
        sys.exit(EXIT_OUTSIDE_WINDOW)

    lake_root = Path(args.lake_root).resolve() if args.lake_root else None
    engine = StreamingEngine(
        lake_root=lake_root,
        writer_id=args.writer_id,
        flush_interval=args.flush_interval,
        max_batch_rows=args.max_batch_rows,
        max_queue_size=args.max_queue_size,
        mock_mode=args.mock,
        drain_delay=args.drain_delay,
        mock_ticks_per_sec=args.ticks_per_sec,
    )

    sys.exit(_run_streamer(engine, lake_root=lake_root))


if __name__ == "__main__":
    main()
