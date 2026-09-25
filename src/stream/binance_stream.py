"""
Binance WebSocket Streamer for 24/7 Crypto and Gold.
Connects to Binance public Kline stream and emits 1-minute OHLCV bars.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
import websockets

logger = logging.getLogger(__name__)

DEFAULT_BINANCE_SYMBOLS = ["btcusdt", "ethusdt", "paxgusdt"]


class BinanceStreamer:
    """Consumes 1-minute klines from Binance WebSockets."""
    def __init__(self, symbols=None, on_bar_callback=None):
        self.symbols = [s.lower() for s in (symbols or DEFAULT_BINANCE_SYMBOLS)]
        self.on_bar_callback = on_bar_callback
        self.running = False
        self._task = None

    def _build_url(self) -> str:
        streams = [f"{s}@kline_1m" for s in self.symbols]
        return f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    async def start(self):
        self.running = True
        url = self._build_url()
        backoff = 1

        while self.running:
            try:
                logger.info(f"Connecting to Binance stream ({len(self.symbols)} symbols)...")
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as ws:
                    logger.info("✅ Connected to Binance WebSocket stream.")
                    backoff = 1

                    while self.running:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        kline_data = data.get("data", {}).get("k")
                        if not kline_data:
                            continue

                        # Extract OHLCV bar
                        # t is open time in milliseconds
                        open_time_ms = kline_data.get("t")
                        dt_utc = datetime.fromtimestamp(open_time_ms / 1000.0, tz=timezone.utc)
                        ts_str = dt_utc.strftime('%Y-%m-%d %H:%M:%S')

                        symbol = kline_data.get("s").upper()
                        open_p = float(kline_data.get("o", 0.0))
                        high_p = float(kline_data.get("h", 0.0))
                        low_p = float(kline_data.get("l", 0.0))
                        close_p = float(kline_data.get("c", 0.0))
                        vol = float(kline_data.get("v", 0.0))
                        is_closed = kline_data.get("x", False)

                        bar = (ts_str, symbol, open_p, high_p, low_p, close_p, vol, "REG", "BINANCE")

                        if self.on_bar_callback:
                            try:
                                if asyncio.iscoroutinefunction(self.on_bar_callback):
                                    await self.on_bar_callback(bar, is_closed)
                                else:
                                    self.on_bar_callback(bar, is_closed)
                            except Exception as cb_err:
                                logger.error(f"Binance callback error: {cb_err}")

            except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as e:
                if not self.running:
                    break
                logger.warning(f"Binance connection dropped ({e}). Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                if not self.running:
                    break
                logger.error(f"Unexpected Binance error: {e}. Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self):
        self.running = False
