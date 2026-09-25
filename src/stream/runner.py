"""
24/7 Capital.com Exclusive Streaming Runner.
Connects to Capital.com WebSocket, receives real-time tick quotes,
and flushes them into dedicated streaming.duckdb in batched transactions.
Supports dynamic subscription reload without dropping the WebSocket connection.
"""
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
import os
from src.database.connection import get_streaming_db_connection, DEFAULT_STREAMING_DB_PATH
from src.database.schema import init_streaming_db
from src.database.operations import save_ticks_to_storage, get_symbol_map_from_db
from src.stream.binance_stream import BinanceStreamer
from src.stream.capital_stream import CapitalStreamer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("stream_runner")


class StreamingEngine:
    """Master streaming orchestrator saving pure tick-by-tick data into streaming.duckdb."""
    def __init__(self, db_path=None, flush_interval=2.0, enable_binance=False):
        self.db_path = db_path or DEFAULT_STREAMING_DB_PATH
        self.flush_interval = flush_interval
        self.enable_binance = enable_binance
        self.running = False
        self.write_queue = asyncio.Queue()
        self.reload_event = asyncio.Event()
        self.db_conn = None
        self.total_ticks_saved = 0

        self.binance_streamer = None
        self.capital_streamer = None

    def _enqueue_tick(self, tick_tuple):
        """Pushes an individual tick into the async write queue."""
        self.write_queue.put_nowait(tick_tuple)

    def _enqueue_bar(self, bar_tuple):
        """Backward-compatible bar enqueueing."""
        self.write_queue.put_nowait(bar_tuple)

    async def _handle_binance_tick(self, tick_tuple):
        """Feeds a raw trade tick from Binance directly into the write queue (for backward compatibility)."""
        self._enqueue_tick(tick_tuple)

    async def _handle_binance_bar(self, bar_tuple, is_closed=True):
        """Backward-compatible handler for Binance bars."""
        if is_closed:
            self._enqueue_tick(bar_tuple)

    async def _handle_capital_tick(self, tick):
        """Feeds a tick from Capital.com directly into the write queue."""
        if isinstance(tick, dict):
            symbol = tick.get("epic", "")
            price = float(tick.get("price", 0.0))
            ts = tick.get("timestamp")
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S.%f') if isinstance(ts, datetime) else str(ts)
            bid = tick.get("bid")
            ask = tick.get("ask")
            tick_tuple = (ts_str, symbol, price, 1.0, bid, ask, "CAPITAL", "REG")
        else:
            tick_tuple = tick
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

    def trigger_reload(self):
        """Signals the background watcher to reload symbol subscriptions immediately."""
        self.reload_event.set()

    async def reload_symbols(self, symbols_override=None):
        """Re-reads symbol_map and updates live Capital.com subscriptions on the fly."""
        if symbols_override is not None:
            capital_symbols = list(symbols_override)
        else:
            symbol_map = get_symbol_map_from_db()
            capital_symbols = []
            for display_name, tickers in symbol_map.items():
                c_ticker = tickers.get("capital_ticker")
                if c_ticker:
                    capital_symbols.append(c_ticker)

        if not capital_symbols:
            capital_symbols = ["AAPL", "NVDA", "TSLA", "SPY", "QQQ", "AMD", "AMZN", "MSFT"]

        logger.info(f"🔄 Reloading active Capital.com symbols: {capital_symbols}")
        if self.capital_streamer:
            success = await self.capital_streamer.update_subscriptions(capital_symbols)
            return success
        return False

    async def _symbol_watcher_worker(self):
        """Background worker that waits for reload events or checks periodically for changes."""
        while self.running:
            try:
                # Wait for explicit reload event or 60s timeout
                try:
                    await asyncio.wait_for(self.reload_event.wait(), timeout=60.0)
                    self.reload_event.clear()
                    logger.info("⚡ Live reload triggered via signal.")
                    await self.reload_symbols()
                except asyncio.TimeoutError:
                    # Periodic sanity check
                    pass
            except Exception as e:
                logger.error(f"Symbol watcher error: {e}")
                await asyncio.sleep(5)

    async def start(self):
        self.running = True
        logger.info(f"Initializing streaming database schema ({self.db_path})...")
        self.db_conn = get_streaming_db_connection(self.db_path)
        init_streaming_db(self.db_conn)

        # Discover symbols from symbol_map
        symbol_map = get_symbol_map_from_db()
        capital_symbols = []

        for display_name, tickers in symbol_map.items():
            c_ticker = tickers.get("capital_ticker")
            if c_ticker:
                capital_symbols.append(c_ticker)

        if not capital_symbols:
            capital_symbols = ["AAPL", "NVDA", "TSLA", "SPY", "QQQ", "AMD", "AMZN", "MSFT"]

        logger.info(f"🎯 Target Capital.com symbols for live quotes ({len(capital_symbols)}): {capital_symbols[:6]}...")

        # Initialize Capital.com streamer exclusively
        self.capital_streamer = CapitalStreamer(
            epics=capital_symbols,
            on_tick_callback=self._handle_capital_tick
        )

        tasks = [
            asyncio.create_task(self._duckdb_writer_worker()),
            asyncio.create_task(self.capital_streamer.start()),
            asyncio.create_task(self._symbol_watcher_worker()),
        ]

        # Optional Binance streamer (disabled by default per user specification)
        if self.enable_binance:
            binance_symbols = [
                tickers.get("binance_ticker") for display_name, tickers in symbol_map.items()
                if tickers.get("binance_ticker")
            ] or ["btcusdt", "ethusdt"]
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
            if self.db_conn:
                self.db_conn.close()

    def stop(self):
        logger.info("Stopping Streaming Engine...")
        self.running = False
        if self.capital_streamer:
            self.capital_streamer.stop()
        if self.binance_streamer:
            self.binance_streamer.stop()


def main():
    engine = StreamingEngine()

    def handle_signal(sig, frame):
        logger.info(f"Received exit signal ({sig}), shutting down...")
        engine.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(engine.start())


if __name__ == "__main__":
    main()
