"""
24/7 Master Streaming Runner.
Coordinates Capital.com and Binance WebSocket streamers, aggregates incoming data,
and flushes 1-minute OHLCV bars into DuckDB in batched transactions.
"""
import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
from src.database.connection import get_duckdb_connection
from src.database.schema import init_db
from src.database.operations import _save_to_client, get_symbol_map_from_db
from src.stream.binance_stream import BinanceStreamer
from src.stream.capital_stream import CapitalStreamer
from src.stream.aggregator import CandleAggregator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("stream_runner")


class StreamingEngine:
    """Master streaming orchestrator."""
    def __init__(self, db_path=None, flush_interval=5.0):
        self.db_path = db_path
        self.flush_interval = flush_interval
        self.running = False
        self.write_queue = asyncio.Queue()
        self.db_conn = None
        self.aggregator = CandleAggregator(on_candle_closed=self._enqueue_bar)

        self.binance_streamer = None
        self.capital_streamer = None
        self.total_bars_saved = 0

    def _enqueue_bar(self, bar_tuple):
        """Pushes a closed 1-minute bar into the async write queue."""
        self.write_queue.put_nowait(bar_tuple)

    async def _handle_binance_bar(self, bar_tuple, is_closed):
        """Handles a bar emitted by Binance."""
        if is_closed:
            self._enqueue_bar(bar_tuple)

    async def _handle_capital_tick(self, tick):
        """Feeds a tick from Capital.com into the aggregator."""
        symbol = tick["epic"]
        price = tick["price"]
        ts = tick["timestamp"]
        self.aggregator.process_tick(symbol, price, ts, volume=1.0, source="CAPITAL")

    async def _duckdb_writer_worker(self):
        """Worker that drains the write queue and batches writes into DuckDB."""
        buffer = []
        last_flush = time.time()

        while self.running or not self.write_queue.empty():
            try:
                # Wait for items with timeout
                try:
                    bar = await asyncio.wait_for(self.write_queue.get(), timeout=1.0)
                    buffer.append(bar)
                    self.write_queue.task_done()
                except asyncio.TimeoutError:
                    pass

                # Check if we should flush buffer
                now = time.time()
                should_flush = (
                    len(buffer) >= 50 or
                    (buffer and (now - last_flush) >= self.flush_interval) or
                    (not self.running and buffer)
                )

                if should_flush:
                    count = len(buffer)
                    _save_to_client(self.db_conn, buffer, label="DuckDB-Stream")
                    self.total_bars_saved += count
                    logger.info(f"💾 Committed {count} bars to DuckDB (Total session: {self.total_bars_saved})")
                    buffer.clear()
                    last_flush = now

                # Flush any stale candles from aggregator
                self.aggregator.flush_stale_candles(max_age_seconds=90)

            except Exception as e:
                logger.error(f"DuckDB writer worker error: {e}")
                await asyncio.sleep(1)

    async def start(self):
        self.running = True
        logger.info("Initializing DuckDB database schema...")
        self.db_conn = get_duckdb_connection(self.db_path)
        init_db(self.db_conn)

        # Discover symbols from symbol_map
        symbol_map = get_symbol_map_from_db(self.db_conn)
        binance_symbols = []
        capital_symbols = []

        for display_name, tickers in symbol_map.items():
            b_ticker = tickers.get("binance_ticker")
            c_ticker = tickers.get("capital_ticker")

            if b_ticker:
                binance_symbols.append(b_ticker)
            if c_ticker:
                capital_symbols.append(c_ticker)

        # Fallbacks if empty
        if not binance_symbols:
            binance_symbols = ["btcusdt", "ethusdt", "paxgusdt"]
        if not capital_symbols:
            capital_symbols = ["AAPL", "NVDA", "TSLA", "SPY", "QQQ", "AMD", "AMZN", "MSFT"]

        logger.info(f"Targeting {len(binance_symbols)} Binance symbols: {binance_symbols}")
        logger.info(f"Targeting {len(capital_symbols)} Capital.com symbols: {capital_symbols[:6]}...")

        # Initialize streamers
        self.binance_streamer = BinanceStreamer(
            symbols=binance_symbols,
            on_bar_callback=self._handle_binance_bar
        )
        self.capital_streamer = CapitalStreamer(
            epics=capital_symbols,
            on_tick_callback=self._handle_capital_tick
        )

        # Launch all tasks
        tasks = [
            asyncio.create_task(self._duckdb_writer_worker()),
            asyncio.create_task(self.binance_streamer.start()),
            asyncio.create_task(self.capital_streamer.start())
        ]

        logger.info("🚀 24/7 Streaming Engine started successfully.")
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
        if self.binance_streamer:
            self.binance_streamer.stop()
        if self.capital_streamer:
            self.capital_streamer.stop()


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
