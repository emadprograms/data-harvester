"""BASE-01 — the lake's candle output, recorded as reference behaviour.

Captured 2026-10-05, before the v5.0 deletion work touched the reader. v5.0 keeps
DuckDB as the in-memory query engine and does **not** rewrite `reader.py`, so the
candle maths must come out of this refactor byte-for-byte unchanged. This file
exists so that an accidental change is detectable instead of silent.

The population is a deterministic 90-minute session: four ticks per minute at
seconds 0/15/30/45, price 220.00 + 0.10/minute with a fixed intra-minute ladder
(+0.40 / -0.30 / +0.20), 100 volume per tick. Every number below was read off
the current implementation and is a golden value, not a computed expectation.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from tests.fixtures.deterministic_quotes import QuoteTick

SYMBOL = "AAPL"
SESSION_DATE = "2026-07-10"  # Friday
SESSION_START_UTC = datetime(2026, 7, 10, 13, 30, 0)  # 09:30 ET (EDT)
SESSION_MINUTES = 90
TICKS_PER_MINUTE = 4

# timeframe -> (candle count, first bucket label, first OHLCV, last OHLCV)
BASELINE = {
    "1m": (90, "2026-07-10 09:30:00", (220.0, 220.4, 219.7, 220.2, 400.0), (228.9, 229.3, 228.6, 229.1, 400.0)),
    "5m": (18, "2026-07-10 09:30:00", (220.0, 220.8, 219.7, 220.6, 2000.0), (228.5, 229.3, 228.2, 229.1, 2000.0)),
    "15m": (6, "2026-07-10 09:30:00", (220.0, 221.8, 219.7, 221.6, 6000.0), (227.5, 229.3, 227.2, 229.1, 6000.0)),
    "30m": (3, "2026-07-10 09:30:00", (220.0, 223.3, 219.7, 223.1, 12000.0), (226.0, 229.3, 225.7, 229.1, 12000.0)),
    "1h": (2, "2026-07-10 09:00:00", (220.0, 223.3, 219.7, 223.1, 12000.0), (223.0, 229.3, 222.7, 229.1, 24000.0)),
    "4h": (1, "2026-07-10 08:00:00", (220.0, 229.3, 219.7, 229.1, 36000.0), (220.0, 229.3, 219.7, 229.1, 36000.0)),
    "1d": (1, "2026-07-10 00:00:00", (220.0, 229.3, 219.7, 229.1, 36000.0), (220.0, 229.3, 219.7, 229.1, 36000.0)),
}


@pytest.fixture(scope="module")
def baseline_lake(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("candle_baseline") / "lake"
    init_tick_lake(root)

    ticks = []
    for minute in range(SESSION_MINUTES):
        base = round(220.0 + minute * 0.10, 4)
        ladder = ((0, base), (15, base + 0.40), (30, base - 0.30), (45, base + 0.20))
        for offset, (second, price) in enumerate(ladder):
            ticks.append(
                QuoteTick(
                    timestamp=SESSION_START_UTC + timedelta(minutes=minute, seconds=second),
                    symbol=SYMBOL,
                    price=round(price, 4),
                    volume=100.0,
                    bid=round(price - 0.05, 4),
                    ask=round(price + 0.05, 4),
                    source="CAPITAL",
                    session="REG",
                    ingest_id=f"base_{minute:03d}_{offset}",
                )
            )

    writer = TickLakeWriter(root=root, writer_id="baseline", max_batch_rows=10**9)
    writer.write_ticks(ticks)
    writer.flush(block=True)
    writer.close()
    return root


def _candles(baseline_lake: Path, timeframe: str) -> dict:
    reader = TickLakeReader(root=baseline_lake)
    return reader.get_candles(SYMBOL, timeframe=timeframe, date=SESSION_DATE, limit=5000)


def _ohlcv(candle: dict) -> tuple:
    return (
        candle["open"],
        candle["high"],
        candle["low"],
        candle["close"],
        candle["volume"],
    )


@pytest.mark.parametrize("timeframe", sorted(BASELINE))
def test_recorded_candle_output_is_unchanged(baseline_lake, timeframe) -> None:
    expected_count, expected_first_label, expected_first, expected_last = BASELINE[timeframe]
    result = _candles(baseline_lake, timeframe)
    candles = result["candles"]

    assert result["symbol"] == SYMBOL
    assert result["timeframe"] == timeframe
    assert result["database"] == "streaming"
    assert result["timezone"] == "America/New_York"

    assert len(candles) == expected_count, f"{timeframe}: candle count drifted"
    assert candles[0]["time_str"] == expected_first_label, f"{timeframe}: first bucket moved"
    assert _ohlcv(candles[0]) == expected_first, f"{timeframe}: first candle drifted"
    assert _ohlcv(candles[-1]) == expected_last, f"{timeframe}: last candle drifted"


# The extended window is 04:00-20:00 ET and the regular window 09:30-16:00 ET.
# The fixture covers 09:30-11:00, so the surrounding silence is reported as gaps.
GAP_BASELINE = {
    "extended": (("04:00", "09:29", 330), ("11:00", "19:59", 540)),
    "regular": (("11:00", "15:59", 300),),
}


def test_every_tick_is_accounted_for(baseline_lake) -> None:
    expected_ticks = SESSION_MINUTES * TICKS_PER_MINUTE
    for timeframe in sorted(BASELINE):
        result = _candles(baseline_lake, timeframe)
        assert result["day_total_ticks"] == expected_ticks, f"{timeframe}: ticks lost"
        candles = result["candles"]
        assert sum(c["tick_count"] for c in candles) == expected_ticks, f"{timeframe}: tick_count drift"
        assert sum(c["volume"] for c in candles) == expected_ticks * 100.0, f"{timeframe}: volume not conserved"


@pytest.mark.parametrize("hours", sorted(GAP_BASELINE))
def test_session_window_gaps_are_unchanged(baseline_lake, hours) -> None:
    """Pins the DST-aware session windows that drive the chart's gap shading."""
    reader = TickLakeReader(root=baseline_lake)
    result = reader.get_candles(
        SYMBOL, timeframe="1m", date=SESSION_DATE, limit=5000, hours=hours
    )
    observed = tuple((g["start_str"], g["end_str"], g["duration"]) for g in result["gaps"])
    assert observed == GAP_BASELINE[hours]


# ------------------------------------------------------------------ CO-02
#
# Verification that the query engine was not modified by v5.0: the golden values
# above are the evidence for the maths, and the two checks below pin the
# interfaces and the dashboard layer's pass-through behaviour, so a future change
# cannot quietly reshape candle output on its way to the UI.

def test_candle_entry_point_signatures_are_unchanged() -> None:
    import inspect

    from src.storage.reader import TickLakeReader

    get_candles = inspect.signature(TickLakeReader.get_candles)
    assert list(get_candles.parameters) == [
        "self", "symbol", "timeframe", "start", "end", "limit", "date", "hours",
    ]
    query_candles = inspect.signature(TickLakeReader.query_candles)
    assert list(query_candles.parameters) == [
        "self", "symbol", "timeframe", "start", "end", "limit", "inclusive_end",
    ]


def test_dashboard_returns_reader_candles_without_resampling_them(monkeypatch) -> None:
    """`analytics.get_streaming_candles` must hand back the reader's result as-is."""
    from unittest.mock import MagicMock

    from src.dashboard import analytics

    sentinel = {
        "symbol": "AAPL",
        "timeframe": "1m",
        "count": 1,
        "candles": [{"time": 1, "open": 2.0, "high": 3.0, "low": 1.0, "close": 2.5, "volume": 10.0}],
    }
    reader = MagicMock()
    reader.get_candles.return_value = sentinel
    monkeypatch.setattr(analytics, "_get_lake_reader", lambda: reader)

    result = analytics.get_streaming_candles("AAPL", timeframe="1m")
    assert result is sentinel, "the dashboard layer transformed candle output"
    reader.get_candles.assert_called_once_with(
        symbol="AAPL", timeframe="1m", start=None, end=None, limit=1000, date=None, hours="extended"
    )
