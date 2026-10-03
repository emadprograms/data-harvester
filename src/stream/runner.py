"""
24/7 Capital.com Exclusive Streaming Runner.
Connects to Capital.com WebSocket, receives real-time tick quotes,
and flushes them into dedicated streaming.duckdb in batched transactions
or partitioned Parquet Tick Lake.
Supports dynamic subscription reload without dropping the WebSocket connection.
"""
import asyncio
import logging
from pathlib import Path
import signal
import sys
import time
from datetime import datetime, timezone
import os
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from src.database.connection import get_streaming_db_connection, DEFAULT_STREAMING_DB_PATH
from src.database.schema import init_streaming_db
from src.database.operations import (
    save_ticks_to_storage,
    get_streaming_database_symbols_from_db,
    get_streaming_symbol_map_from_db,
)
from src.storage.registry import RegistryError
from src.stream.binance_stream import BinanceStreamer
from src.stream.capital_stream import CapitalStreamer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("stream_runner")


class MockStreamer:
    """Mock streamer generating synthetic ticks for offline/test execution without real network/credentials."""
    def __init__(self, epics: List[str], on_tick_callback, ticks_per_sec: float = 50.0):
        self.epics = list(epics) if epics else ["AAPL", "NVDA", "MSFT"]
        self.on_tick_callback = on_tick_callback
        self.ticks_per_sec = float(ticks_per_sec)
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self.running = True
        self._task = asyncio.create_task(self._run_loop())
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    async def _run_loop(self):
        idx = 0
        interval = 1.0 / self.ticks_per_sec if self.ticks_per_sec > 0 else 0.02
        while self.running:
            if self.epics:
                epic = self.epics[idx % len(self.epics)]
                now = datetime.now(timezone.utc)
                tick = {
                    "epic": epic,
                    "price": 100.0 + (idx % 100) * 0.1,
                    "timestamp": now,
                    "bid": 99.95,
                    "ask": 100.05,
                    "volume": 1.0,
                }
                res = self.on_tick_callback(tick)
                if asyncio.iscoroutine(res):
                    await res
                idx += 1
            await asyncio.sleep(interval)

    def stop(self):
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def update_subscriptions(self, epics: List[str]) -> bool:
        self.epics = list(epics)
        return True


class BoundedWriteQueue(asyncio.Queue):
    """Asyncio Queue with an unfinished_tasks property for honest task_done tracking."""
    @property
    def unfinished_tasks(self) -> int:
        return self._unfinished_tasks


class StreamingEngine:
    """Master streaming orchestrator saving pure tick-by-tick data into partitioned Parquet tick lake or streaming.duckdb."""
    def __init__(
        self,
        db_path=None,
        flush_interval=2.0,
        enable_binance=False,
        lake_root=None,
        max_queue_size=10000,
        writer_id="writer_1",
        registry_poll_interval=1.0,
        registry_debounce_interval=0.05,
        mock_mode: bool = False,
        drain_delay: float = 0.0,
        mock_ticks_per_sec: float = 50.0,
    ):
        self.lake_root = lake_root
        self.max_queue_size = max_queue_size
        self.flush_interval = flush_interval
        self.enable_binance = enable_binance
        self.mock_mode = mock_mode
        self.drain_delay = float(drain_delay)
        self.mock_ticks_per_sec = float(mock_ticks_per_sec)
        self.running = False
        self.write_queue = BoundedWriteQueue(maxsize=max_queue_size)
        self.reload_event = asyncio.Event()
        self.db_conn = None
        self.total_ticks_saved = 0
        self.total_ticks_persisted = 0
        self.ticks_received = 0
        self.ticks_enqueued = 0
        self.ticks_dropped = 0
        self.writer = None
        self.lake_writer = None
        self.registry = None
        self.registry_poll_interval = float(registry_poll_interval)
        self.registry_debounce_interval: float = float(registry_debounce_interval)

        if self.lake_root is not None:
            from src.storage.parquet_writer import TickLakeWriter
            self.writer = TickLakeWriter(
                root=self.lake_root,
                writer_id=writer_id,
                flush_interval_seconds=flush_interval,
                max_queue_size=max_queue_size,
            )
            self.lake_writer = self.writer
            self.db_path = None
            try:
                from src.storage.registry import SymbolRegistry
                self.registry = SymbolRegistry(root=self.lake_root)
            except Exception:
                pass
        elif db_path is None and (os.environ.get("TICK_LAKE_ROOT") or os.environ.get("DATA_DIR")):
            try:
                from src.storage.config import resolve_tick_lake_root
                resolved_root = resolve_tick_lake_root()
                self.lake_root = resolved_root
                from src.storage.parquet_writer import TickLakeWriter
                self.writer = TickLakeWriter(
                    root=self.lake_root,
                    writer_id=writer_id,
                    flush_interval_seconds=flush_interval,
                    max_queue_size=max_queue_size,
                )
                self.lake_writer = self.writer
                self.db_path = None
                from src.storage.registry import SymbolRegistry
                self.registry = SymbolRegistry(root=self.lake_root)
            except Exception:
                self.db_path = DEFAULT_STREAMING_DB_PATH
        else:
            self.db_path = db_path or DEFAULT_STREAMING_DB_PATH

        self.binance_streamer = None
        self.capital_streamer = None
        self.active_streaming_symbols = set()
        self.epic_to_display = {}
        self._subscriptions_initialized = False

    def _record_drop(self, count: int = 1) -> None:
        """Increments drop counter and synchronizes drop count to writer metrics immediately."""
        self.ticks_dropped += count
        if self.writer is not None and hasattr(self.writer, "_metrics"):
            self.writer._metrics.total_dropped = self.ticks_dropped

    @property
    def ticks_committed(self) -> int:
        return self.total_ticks_persisted

    def _enqueue_tick(self, tick_tuple):
        """Pushes an individual tick into the async write queue."""
        self.ticks_received += 1
        try:
            self.write_queue.put_nowait(tick_tuple)
            self.ticks_enqueued += 1
        except asyncio.QueueFull:
            self._record_drop(1)
            sym = tick_tuple[1] if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1 else str(tick_tuple)
            logger.warning("write_queue full (%d), dropping tick: %s", self.write_queue.maxsize, sym)

    def _enqueue_bar(self, bar_tuple):
        """Backward-compatible bar enqueueing."""
        self._enqueue_tick(bar_tuple)

    async def _handle_binance_tick(self, tick_tuple):
        """Feeds a raw trade tick from Binance directly into the write queue with fencing."""
        if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1:
            sym = tick_tuple[1]
            if self._subscriptions_initialized or self.active_streaming_symbols:
                if sym not in self.active_streaming_symbols:
                    return
        self._enqueue_tick(tick_tuple)

    async def _handle_binance_bar(self, bar_tuple, is_closed=True):
        """Backward-compatible handler for Binance bars."""
        if is_closed:
            self._enqueue_tick(bar_tuple)

    async def _handle_capital_tick(self, tick):
        """Feeds a tick from Capital.com directly into the write queue, filtering out excluded/purged assets."""
        if isinstance(tick, dict):
            raw_epic = tick.get("epic", "")
            # Subscription fencing: drop if symbol is not active
            if self._subscriptions_initialized or self.active_streaming_symbols:
                if raw_epic not in self.active_streaming_symbols:
                    return

            symbol = self.epic_to_display.get(raw_epic, raw_epic)
            price = float(tick.get("price", 0.0))
            ts = tick.get("timestamp")
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(ts, datetime) else str(ts)
            bid = tick.get("bid")
            ask = tick.get("ask")
            tick_tuple = (ts_str, symbol, price, 1.0, bid, ask, "CAPITAL", "REG")
        else:
            tick_tuple = tick
            if isinstance(tick_tuple, (list, tuple)) and len(tick_tuple) > 1:
                sym = tick_tuple[1]
                if self._subscriptions_initialized or self.active_streaming_symbols:
                    if sym not in self.active_streaming_symbols:
                        return
        self._enqueue_tick(tick_tuple)

    async def _duckdb_writer_worker(self):
        """Worker that drains the write queue and batches raw tick writes into streaming.duckdb."""
        buffer = []
        last_flush = time.time()

        while self.running or not self.write_queue.empty():
            try:
                wait_time = min(0.2, self.flush_interval) if buffer else 0.5
                try:
                    tick = await asyncio.wait_for(self.write_queue.get(), timeout=wait_time)
                    # Normalize if 9-element bar tuple from legacy callers: (ts, sym, o, h, l, c, v, sess, src)
                    if len(tick) == 9:
                        tick = (tick[0], tick[1], tick[5], tick[6], None, None, tick[8], tick[7])
                    buffer.append(tick)
                    self.write_queue.task_done()
                except asyncio.TimeoutError:
                    pass

                now = time.time()
                should_flush = (
                    len(buffer) >= 100 or
                    (buffer and (now - last_flush) >= self.flush_interval) or
                    (not self.running and buffer)
                )

                if should_flush:
                    count = len(buffer)
                    save_ticks_to_storage(self.db_conn, buffer, label="DuckDB-Ticks")
                    self.total_ticks_saved += count
                    logger.info(f"💾 Committed {count} ticks to streaming.duckdb (Total session: {self.total_ticks_saved})")
                    buffer.clear()
                    last_flush = now

            except Exception as e:
                logger.error(f"DuckDB writer worker error: {e}")
                await asyncio.sleep(0.5)

        # Flush any remaining buffer on shutdown
        if buffer:
            try:
                count = len(buffer)
                save_ticks_to_storage(self.db_conn, buffer, label="DuckDB-Ticks")
                self.total_ticks_saved += count
                logger.info(f"💾 Final flush: committed {count} ticks to streaming.duckdb.")
                buffer.clear()
            except Exception as e:
                logger.error(f"Error during final buffer flush: {e}")

    async def _lake_writer_worker(self):
        """Worker that drains the write queue and batches raw tick writes into TickLakeWriter."""
        buffer = []
        last_flush = time.monotonic()
        cancelled = False

        while (self.running or not self.write_queue.empty()) and not cancelled:
            try:
                wait_time = min(0.05, self.flush_interval) if buffer else (0.05 if not self.running else min(0.2, self.flush_interval))
                try:
                    tick = await asyncio.wait_for(self.write_queue.get(), timeout=wait_time)
                    # Normalize if 9-element bar tuple from legacy callers: (ts, sym, o, h, l, c, v, sess, src)
                    if len(tick) == 9:
                        tick = (tick[0], tick[1], tick[5], tick[6], None, None, tick[8], tick[7])
                    buffer.append(tick)
                    while len(buffer) < 1000:
                        try:
                            t = self.write_queue.get_nowait()
                            if len(t) == 9:
                                t = (t[0], t[1], t[5], t[6], None, None, t[8], t[7])
                            buffer.append(t)
                        except asyncio.QueueEmpty:
                            break
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                should_flush = (
                    len(buffer) >= 1000 or
                    (buffer and (now - last_flush) >= self.flush_interval) or
                    (not self.running and buffer)
                )

                if should_flush:
                    batch = list(buffer)
                    buffer.clear()
                    batch_len = len(batch)
                    try:
                        publish_task = asyncio.create_task(self.writer.publish_batch_async(batch))
                        try:
                            await asyncio.shield(publish_task)
                        except asyncio.CancelledError:
                            cancelled = True
                            await publish_task
                        self.total_ticks_persisted += batch_len
                        self.total_ticks_saved += batch_len
                        for _ in range(batch_len):
                            self.write_queue.task_done()
                        last_flush = time.monotonic()
                    except asyncio.CancelledError:
                        cancelled = True
                        raise
                    except Exception as exc:
                        logger.error(f"Lake writer worker error: {exc}")
                        self._record_drop(batch_len)
                        for _ in range(batch_len):
                            self.write_queue.task_done()

            except asyncio.CancelledError:
                cancelled = True
                break
            except Exception as e:
                logger.error(f"Lake writer worker error: {e}")
                await asyncio.sleep(0.1)

        # Final flush on worker exit if buffer still has items
        if buffer:
            batch = list(buffer)
            buffer.clear()
            batch_len = len(batch)
            try:
                publish_task = asyncio.create_task(self.writer.publish_batch_async(batch))
                try:
                    await asyncio.shield(publish_task)
                except asyncio.CancelledError:
                    cancelled = True
                    await publish_task
                self.total_ticks_persisted += batch_len
                self.total_ticks_saved += batch_len
                for _ in range(batch_len):
                    self.write_queue.task_done()
            except asyncio.CancelledError:
                cancelled = True
                raise
            except Exception as e:
                logger.error(f"Error during final lake flush: {e}")
                self._record_drop(batch_len)
                for _ in range(batch_len):
                    self.write_queue.task_done()

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
        else:
            s_map = get_streaming_database_symbols_from_db()
            capital_symbols = []
            self.active_streaming_symbols = set()
            self.epic_to_display = {}
            if s_map:
                for display_name, tickers in s_map.items():
                    if tickers.get("is_active", True):
                        self.active_streaming_symbols.add(display_name)
                        c_ticker = tickers.get("capital_ticker")
                        if c_ticker:
                            capital_symbols.append(c_ticker)
                            self.active_streaming_symbols.add(c_ticker)
                            self.epic_to_display[c_ticker] = display_name
                        else:
                            self.epic_to_display[display_name] = display_name

                if not capital_symbols:
                    capital_symbols = ["AAPL", "NVDA", "TSLA", "AMD", "AMZN", "MSFT"]
            else:
                capital_symbols = ["AAPL", "NVDA", "TSLA", "AMD", "AMZN", "MSFT"]

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
                        # Debounce rapid burst of signals
                        debounce = getattr(self, "registry_debounce_interval", 0.05)
                        if debounce > 0:
                            await asyncio.sleep(debounce)

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
        if self.writer is None:
            logger.info(f"Initializing streaming database schema ({self.db_path})...")
            init_conn = get_streaming_db_connection(self.db_path)
            init_streaming_db(init_conn)
            if init_conn:
                init_conn.close()

        # Discover symbols: when self.registry is configured, read active symbols directly via self.registry.get_active_symbols() bypassing DuckDB calls entirely
        self._subscriptions_initialized = True
        capital_symbols = []
        self.active_streaming_symbols = set()
        self.epic_to_display = {}
        s_map = None

        if self.registry is not None:
            try:
                active_entries = self.registry.get_active_symbols()
            except RegistryError as e:
                logger.warning(f"Failed to load active symbols from registry: {e}")
                active_entries = []
            for entry in active_entries:
                sym = entry.symbol
                self.active_streaming_symbols.add(sym)
                c_ticker = entry.capital_ticker or sym
                self.active_streaming_symbols.add(c_ticker)
                capital_symbols.append(c_ticker)
                self.epic_to_display[c_ticker] = sym
        else:
            s_map = get_streaming_database_symbols_from_db()
            if s_map:
                for display_name, tickers in s_map.items():
                    if tickers.get("is_active", True):
                        self.active_streaming_symbols.add(display_name)
                        c_ticker = tickers.get("capital_ticker")
                        if c_ticker:
                            capital_symbols.append(c_ticker)
                            self.active_streaming_symbols.add(c_ticker)
                            self.epic_to_display[c_ticker] = display_name
                        else:
                            self.epic_to_display[display_name] = display_name

        if not capital_symbols:
            capital_symbols = ["AAPL", "NVDA", "TSLA", "AMD", "AMZN", "MSFT"]
            for s in capital_symbols:
                self.active_streaming_symbols.add(s)
                self.epic_to_display[s] = s

        logger.info(f"🎯 Target Capital.com symbols for live quotes ({len(capital_symbols)}): {capital_symbols[:6]}...")

        # Initialize Capital.com streamer (or MockStreamer in mock_mode)
        if self.mock_mode:
            self.capital_streamer = MockStreamer(
                epics=capital_symbols,
                on_tick_callback=self._handle_capital_tick,
                ticks_per_sec=self.mock_ticks_per_sec,
            )
        else:
            self.capital_streamer = CapitalStreamer(
                epics=capital_symbols,
                on_tick_callback=self._handle_capital_tick
            )

        if self.writer is not None:
            writer_worker_task = asyncio.create_task(self._lake_writer_worker())
        else:
            writer_worker_task = asyncio.create_task(self._duckdb_writer_worker())

        tasks = [
            writer_worker_task,
            asyncio.create_task(self.capital_streamer.start()),
            asyncio.create_task(self._symbol_watcher_worker()),
        ]

        # Optional Binance streamer (disabled by default per user specification)
        if self.enable_binance:
            binance_symbols = []
            if self.registry is not None:
                for entry in self.registry.get_active_symbols():
                    if entry.binance_ticker:
                        binance_symbols.append(entry.binance_ticker)
            elif s_map:
                binance_symbols = [
                    tickers.get("binance_ticker") for display_name, tickers in s_map.items()
                    if tickers.get("binance_ticker")
                ]
            if not binance_symbols:
                binance_symbols = ["btcusdt", "ethusdt"]
            self.binance_streamer = BinanceStreamer(
                symbols=binance_symbols,
                stream_type="trade",
                on_tick_callback=self._handle_binance_tick
            )
            tasks.append(asyncio.create_task(self.binance_streamer.start()))

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
        if self.binance_streamer:
            self.binance_streamer.stop()

    async def shutdown(self, drain_timeout: float = 10.0):
        """Gracefully drains remaining queued ticks and closes TickLakeWriter."""
        self.running = False
        if self.drain_delay > 0:
            await asyncio.sleep(self.drain_delay)

        if self.capital_streamer:
            self.capital_streamer.stop()
        if self.binance_streamer:
            self.binance_streamer.stop()

        # Await write queue drain
        if hasattr(self, "write_queue"):
            try:
                await asyncio.wait_for(self.write_queue.join(), timeout=drain_timeout)
            except asyncio.TimeoutError:
                unfinished = getattr(self.write_queue, "unfinished_tasks", 0)
                self._record_drop(unfinished)
                logger.warning(
                    "Timed out waiting for write queue to drain during shutdown (%d unfinished tasks dropped)",
                    unfinished,
                )

        if self.writer is not None:
            try:
                await self.writer.flush_async()
            except Exception as e:
                logger.warning(f"Error during writer flush_async: {e}")
            try:
                await self.writer.close_async()
            except Exception as e:
                logger.warning(f"Error during writer close_async: {e}")

        if self.db_conn:
            self.db_conn.close()
            self.db_conn = None


def main():
    import argparse
    parser = argparse.ArgumentParser(description="24/7 Capital.com Tick Streaming Engine")
    parser.add_argument("--lake-root", type=str, default=None, help="Path to tick lake root")
    parser.add_argument("--writer-id", type=str, default="writer_1", help="Writer ID for LakePublisher")
    parser.add_argument("--flush-interval", type=float, default=2.0, help="Batch flush interval in seconds")
    parser.add_argument("--max-queue-size", type=int, default=10000, help="Max bounded queue size")
    parser.add_argument("--mock", action="store_true", help="Run with MockStreamer generating synthetic ticks")
    parser.add_argument("--fail-immediately", action="store_true", help="Exit immediately with returncode 1 (for supervisor flapping chaos)")
    parser.add_argument("--drain-delay", type=float, default=0.0, help="Artificial delay during shutdown drain in seconds")
    parser.add_argument("--ticks-per-sec", type=float, default=50.0, help="Tick production rate in mock mode")
    parser.add_argument("--enable-binance", action="store_true", help="Enable optional Binance streamer")

    args, _ = parser.parse_known_args()

    if args.fail_immediately:
        sys.exit(1)

    lake_root = Path(args.lake_root).resolve() if args.lake_root else None
    engine = StreamingEngine(
        lake_root=lake_root,
        writer_id=args.writer_id,
        flush_interval=args.flush_interval,
        max_queue_size=args.max_queue_size,
        enable_binance=args.enable_binance,
        mock_mode=args.mock,
        drain_delay=args.drain_delay,
        mock_ticks_per_sec=args.ticks_per_sec,
    )

    async def _async_main():
        stop_event = asyncio.Event()

        def _handle_signal():
            logger.info("Received exit signal, initiating graceful shutdown...")
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_signal)
            except NotImplementedError:
                signal.signal(sig, lambda s, f: stop_event.set())

        engine_task = asyncio.create_task(engine.start())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [engine_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_event.is_set():
            engine.stop()
            await engine.shutdown()

        for t in pending:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        logger.info("Streaming Engine process stopped.")


if __name__ == "__main__":
    main()
