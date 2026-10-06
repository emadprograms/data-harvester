"""Phase 50: new quotes store bid_price and ask_price, and the chart reads the bid."""

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.data.databento_backfill import normalize_tbbo_frame, publish_ticks_to_lake
from src.storage.config import init_tick_lake
from src.storage.parquet_writer import TickLakeWriter
from src.storage.reader import TickLakeReader
from src.storage.schema import ticks_to_table
from src.stream.capital_stream import quote_tick_from_capital_payload
from tests.fixtures.deterministic_quotes import QuoteTick


CHART_JS = Path("src/dashboard/static/js/chart.js")
FORBIDDEN_COLUMNS = {"price", "volume", "bid", "ask", "size", "bid_sz_00", "ask_sz_00"}


def _write_v2_quotes(root: Path, rows: list[dict]) -> None:
    writer = TickLakeWriter(root=root, writer_id="phase50_quote")
    try:
        writer.write_ticks(rows)
        receipt = writer.flush(block=True)
        assert receipt is not None
        assert receipt.row_count == len(rows)
    finally:
        writer.close()


def _parquet_files(root: Path) -> list[Path]:
    return sorted((root / "ticks").glob("symbol=*/date=*/*.parquet"))


def test_quote_01_capital_quote_stores_bid_and_ask_only(tmp_path):
    """QUOTE-01: feed bid and ofr are stored as-is. No midpoint and no volume."""
    payload = {
        "epic": "NVDA",
        "bid": 100.00,
        "ofr": 100.04,
        "bidQty": 10,
        "ofrQty": 8,
        "timestamp": 1760000000000,
    }
    tick = quote_tick_from_capital_payload(payload)
    assert tick is not None
    assert tick["bid_price"] == pytest.approx(100.00)
    assert tick["ask_price"] == pytest.approx(100.04)
    assert "price" not in tick
    assert "volume" not in tick
    assert tick["bid_price"] != pytest.approx((100.00 + 100.04) / 2.0)

    tick["symbol"] = tick.pop("epic")
    lake = tmp_path / "lake"
    _write_v2_quotes(lake, [tick])

    files = _parquet_files(lake)
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert set(table.column_names).isdisjoint(FORBIDDEN_COLUMNS)
    assert table.column_names[:4] == ["timestamp", "symbol", "bid_price", "ask_price"] or (
        "bid_price" in table.column_names and "ask_price" in table.column_names
    )
    assert table["bid_price"][0].as_py() == pytest.approx(100.00)
    assert table["ask_price"][0].as_py() == pytest.approx(100.04)
    assert 100.02 not in {round(v, 2) for v in table["bid_price"].to_pylist()}


def test_quote_02_databento_tbbo_maps_trade_price_to_bid_and_ask(tmp_path):
    """QUOTE-02: Databento trade price is mapped to both bid_price and ask_price to avoid BBO wicks."""
    import pandas as pd

    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(["2026-10-02 14:30:00.123456+00:00"]),
            "symbol": ["NVDA"],
            "price": [100.50],
            "size": [12],
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
            "bid_sz_00": [3],
            "ask_sz_00": [4],
        }
    )
    frame = normalize_tbbo_frame(raw, date(2026, 10, 2))
    assert list(frame.columns) == [
        "timestamp",
        "symbol",
        "bid_price",
        "ask_price",
        "source",
        "session",
    ]
    assert frame.iloc[0]["bid_price"] == pytest.approx(100.50)
    assert frame.iloc[0]["ask_price"] == pytest.approx(100.50)
    assert frame.iloc[0]["source"] == "DATABENTO"
    assert "price" not in frame.columns
    assert "volume" not in frame.columns
    assert "size" not in frame.columns

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    published = publish_ticks_to_lake(frame, lake_root=lake)
    assert published == 1
    table = pq.read_table(_parquet_files(lake)[0])
    assert set(table.column_names).isdisjoint(FORBIDDEN_COLUMNS)
    assert table["bid_price"][0].as_py() == pytest.approx(100.50)
    assert table["ask_price"][0].as_py() == pytest.approx(100.50)
    assert "12" not in {str(v) for v in table.to_pylist()[0].values()}


def test_quote_02_drops_databento_when_trade_price_is_missing():
    """A missing Databento trade price is dropped."""
    import pandas as pd

    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(["2026-10-02 14:31:00+00:00"]),
            "symbol": ["NVDA"],
            "price": [None],
            "size": [1],
            "bid_px_00": [100.00],
            "ask_px_00": [100.04],
        }
    )
    frame = normalize_tbbo_frame(raw, date(2026, 10, 2))
    assert frame.empty
    assert "price" not in frame.columns


def test_quote_03_chart_uses_bid_price_and_has_no_volume_series(tmp_path):
    """QUOTE-03: the inspection candle is the bid, and the volume histogram is gone."""
    source = CHART_JS.read_text(encoding="utf-8")
    assert "addHistogramSeries" not in source
    assert "streamingVolumeSeries.setData" not in source

    lake = tmp_path / "lake"
    _write_v2_quotes(
        lake,
        [
            {
                "timestamp": datetime(2026, 10, 2, 14, 30, 0, tzinfo=timezone.utc),
                "symbol": "NVDA",
                "bid_price": 100.00,
                "ask_price": 100.04,
                "source": "CAPITAL",
                "session": "REG",
            },
            {
                "timestamp": datetime(2026, 10, 2, 14, 30, 30, tzinfo=timezone.utc),
                "symbol": "NVDA",
                "bid_price": 100.10,
                "ask_price": 100.16,
                "source": "CAPITAL",
                "session": "REG",
            },
        ],
    )
    reader = TickLakeReader(root=lake)
    result = reader.get_candles(symbol="NVDA", timeframe="1m", date="2026-10-02", hours="regular")
    assert result["count"] == 1
    candle = result["candles"][0]
    assert candle["open"] == pytest.approx(100.00)
    assert candle["high"] == pytest.approx(100.10)
    assert candle["low"] == pytest.approx(100.00)
    assert candle["close"] == pytest.approx(100.10)
    # The midpoint of the first quote is 100.02. The candle must not use it.
    assert candle["open"] != pytest.approx(100.02)


def test_reader_still_reads_schema_v1_price_candles(tmp_path):
    """Existing v1 files remain readable. Their candle still uses the stored price."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    tick = QuoteTick(
        timestamp=datetime(2026, 10, 2, 14, 30, 0),
        symbol="NVDA",
        price=50.25,
        volume=1.0,
        bid=50.00,
        ask=50.50,
        source="CAPITAL",
        session="REG",
        ingest_id="v1_guard",
    )
    table = ticks_to_table([tick], validate=True)
    dest = lake / "ticks" / "symbol=NVDA" / "date=2026-10-02"
    dest.mkdir(parents=True)
    pq.write_table(table, dest / "batch_v1.parquet")

    reader = TickLakeReader(root=lake)
    candles = reader.query_candles("NVDA", "1m")
    assert len(candles) == 1
    assert candles[0]["open"] == pytest.approx(50.25)
    assert candles[0]["open"] != pytest.approx(50.00)
