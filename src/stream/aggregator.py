"""
Real-Time Tick-to-Bar Aggregator.
Aggregates tick quotes into clean 1-minute OHLCV candlesticks for DuckDB ingestion.
"""
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class CandleAggregator:
    """Builds 1-minute OHLCV bars from live tick data."""
    def __init__(self, on_candle_closed=None):
        self.on_candle_closed = on_candle_closed
        # symbol -> current bar dict
        self._current_bars = {}

    def _get_minute_str(self, dt: datetime) -> str:
        return dt.strftime('%Y-%m-%d %H:%M:00')

    def process_tick(self, symbol: str, price: float, dt: datetime, volume: float = 1.0, source: str = "CAPITAL"):
        """Processes a new tick price and updates or closes 1-minute candle."""
        minute_str = self._get_minute_str(dt)
        current = self._current_bars.get(symbol)

        if current and current["minute_str"] != minute_str:
            # Previous minute candle has closed
            closed_bar = (
                current["minute_str"],
                symbol,
                current["open"],
                current["high"],
                current["low"],
                current["close"],
                current["volume"],
                "REG",
                current["source"]
            )
            if self.on_candle_closed:
                try:
                    self.on_candle_closed(closed_bar)
                except Exception as e:
                    logger.error(f"Error handling closed candle for {symbol}: {e}")

            # Start new candle
            self._current_bars[symbol] = {
                "minute_str": minute_str,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "source": source
            }
        elif current:
            # Update existing minute candle
            current["high"] = max(current["high"], price)
            current["low"] = min(current["low"], price)
            current["close"] = price
            current["volume"] += volume
        else:
            # Initialize first candle for this symbol
            self._current_bars[symbol] = {
                "minute_str": minute_str,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": volume,
                "source": source
            }

    def flush_stale_candles(self, max_age_seconds: int = 120):
        """Flushes any open candles older than max_age_seconds."""
        now = datetime.now(timezone.utc)
        symbols_to_flush = []

        for symbol, current in self._current_bars.items():
            candle_dt = datetime.strptime(current["minute_str"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            if (now - candle_dt).total_seconds() > max_age_seconds:
                symbols_to_flush.append(symbol)

        for symbol in symbols_to_flush:
            current = self._current_bars.pop(symbol, None)
            if current and self.on_candle_closed:
                closed_bar = (
                    current["minute_str"],
                    symbol,
                    current["open"],
                    current["high"],
                    current["low"],
                    current["close"],
                    current["volume"],
                    "REG",
                    current["source"]
                )
                try:
                    self.on_candle_closed(closed_bar)
                except Exception as e:
                    logger.error(f"Error flushing candle for {symbol}: {e}")
