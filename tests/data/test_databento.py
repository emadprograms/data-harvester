"""
Unit and integration tests for Databento tick backfill engine.
Validates symbol filtering (single-stocks only), trading session calculation,
TBBO record normalization, and budget cap safety guards.
"""
from datetime import date
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd

import src.data.databento_backfill as backfill_module
from src.data.databento_backfill import (
    get_target_stock_symbols,
    generate_candidate_trading_days,
    run_databento_backfill,
    EXCLUDED_SYMBOLS,
)
from src.storage.config import init_tick_lake


def test_databento_sdk_is_only_required_for_live_client(monkeypatch):
    """Offline backfill helpers remain importable without the optional SDK."""
    monkeypatch.setattr(backfill_module, "db", None)
    with pytest.raises(RuntimeError, match="Databento SDK is required for live backfill"):
        backfill_module.get_databento_client(api_key="test-key")


def test_target_stock_symbols_filtering():
    """Verify that only single stock symbols are returned, excluding ETFs, crypto, and futures."""
    symbols = get_target_stock_symbols()
    assert len(symbols) > 0

    # Ensure major stocks are present
    for expected in ["AAPL", "NVDA", "MSFT", "AMZN", "GOOGL"]:
        assert expected in symbols, f"Expected stock {expected} not found in target symbols"

    # Ensure all ETFs, Crypto, and Commodities are excluded
    for excluded in ["SPY", "QQQ", "IWM", "DIA", "BTCUSDT", "ETHUSDT", "CL=F", "VIX"]:
        assert excluded not in symbols, f"Excluded asset {excluded} was unexpectedly included"

    for sym in symbols:
        assert sym not in EXCLUDED_SYMBOLS
        assert not sym.endswith("USDT")
        assert "=" not in sym


def test_old_0900_1600_day_helpers_are_gone():
    """The live fill path must not keep a 09:00–16:00 whole-day download helper."""
    import inspect

    source = inspect.getsource(backfill_module)
    assert "def get_day_trading_bounds" not in source
    assert "def estimate_day_cost" not in source
    assert "def fetch_and_normalize_day" not in source
    assert "def is_day_already_backfilled" not in source


def test_generate_candidate_trading_days():
    """Verify candidate trading days going backward are strictly weekdays (Mon-Fri)."""
    start = date(2026, 9, 25)
    days = generate_candidate_trading_days(start, count=15)
    assert len(days) == 15

    for d in days:
        assert d.weekday() < 5, f"Date {d} is a weekend day (weekday {d.weekday()})"

    # Verify strictly descending order
    for i in range(len(days) - 1):
        assert days[i] > days[i + 1]


def test_f6_post_market_tbbo_is_labeled_post():
    """F6: a 17:00 ET quote is POST, not REG."""
    from src.data.databento_backfill import normalize_tbbo_frame

    raw = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(
                [
                    "2026-10-02 08:30:00+00:00",  # 04:30 ET
                    "2026-10-02 14:30:00+00:00",  # 10:30 ET
                    "2026-10-02 21:00:00+00:00",  # 17:00 ET
                ]
            ),
            "symbol": ["NVDA", "NVDA", "NVDA"],
            "bid_px_00": [100.00, 100.10, 100.20],
            "ask_px_00": [100.04, 100.14, 100.24],
        }
    )
    frame = normalize_tbbo_frame(raw, date(2026, 10, 2))
    assert list(frame["session"]) == ["PRE", "REG", "POST"]


def test_budget_cap_stops_execution(tmp_path):
    """Verify that the backfill loop respects max_budget and stops immediately when cap is reached."""
    from src.data.gap_fill import GapFillBudgetExceeded

    lake = tmp_path / "lake"
    init_tick_lake(lake)
    mock_client = MagicMock()

    def fake_fill(trading_date, client, lake_root, symbols, remaining_budget=None, **kwargs):
        if remaining_budget is not None and remaining_budget < 10.0:
            raise GapFillBudgetExceeded("interval estimate exceeds remaining budget")
        return {
            "requests": 1,
            "follow_up_requests": 0,
            "rows": 1,
            "stretches": [],
            "estimated_cost": 10.0,
        }

    with patch("src.data.databento_backfill.get_target_stock_symbols", return_value=["NVDA"]):
        with patch("src.data.gap_fill.fill_named_day", side_effect=fake_fill) as mock_fetch:
            # Budget is $25.0 -> can only afford 2 days ($20 total). 3rd day exceeds $25.
            res = run_databento_backfill(
                max_budget=25.0,
                max_days=10,
                start_date=date(2026, 9, 24),
                client=mock_client,
                lake_root=lake,
            )

    assert res["success"] is True
    assert res["completed_days_count"] == 2
    assert res["total_cost_usd"] == 20.0
    assert res["remaining_budget_usd"] == 5.0
    assert mock_fetch.call_count == 3


# ---------------------------------------------------------------------------
# v6.0 gap-fill remediation (quick task 261006-hfu): named-batch publication
#
# P1: named batches at sequence zero shared one physical filename per
#     (symbol, UTC date) partition, so a second interval raised
#     BatchCollisionError after its download had already been paid for.
# ---------------------------------------------------------------------------

import json

import pyarrow.parquet as pq

from src.data.databento_backfill import publish_ticks_to_lake
from src.storage.publication import LakePublisher


def _quote_frame(symbol: str, timestamp: str, ingest_id: str):
    return pd.DataFrame(
        {
            "timestamp": [timestamp],
            "symbol": [symbol],
            "bid_price": [100.00],
            "ask_price": [100.04],
            "source": ["DATABENTO"],
            "session": ["REG"],
            "ingest_id": [ingest_id],
        }
    )


def _request_scope(batch_id: str, start: str = "2026-10-02T10:01:00-04:00"):
    return {
        "date": "2026-10-02",
        "dataset": "DBEQ.BASIC",
        "schema": "tbbo",
        "symbols": ["AAPL", "NVDA"],
        "start": start,
        "end": "2026-10-02T10:06:00-04:00",
        "batch_id": batch_id,
    }


def _published_files(lake):
    return sorted((lake / "ticks").glob("symbol=*/date=*/*.parquet"))


def test_named_batches_at_sequence_zero_get_distinct_files(tmp_path):
    """Two intervals at sequence zero must not share one physical filename."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)

    first = _quote_frame("NVDA", "2026-10-02 14:01:00", "gfill_first")
    second = _quote_frame("NVDA", "2026-10-02 14:11:00", "gfill_second")
    rows_first = publish_ticks_to_lake(
        first, lake_root=lake, batch_id="gfill_first", writer_id="gap_fill", sequence=0
    )
    rows_second = publish_ticks_to_lake(
        second, lake_root=lake, batch_id="gfill_second", writer_id="gap_fill", sequence=0
    )

    assert (rows_first, rows_second) == (1, 1)
    files = _published_files(lake)
    assert len(files) == 2, f"expected two files, got {[path.name for path in files]}"
    assert len({path.name for path in files}) == 2
    assert all("db_" in path.name for path in files)
    assert len(list((lake / "_control" / "receipts").glob("gfill_*.json"))) == 2


def test_replaying_named_batch_writes_no_additional_file(tmp_path):
    """A stable batch identity replays its receipt instead of appending a copy."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    frame = _quote_frame("NVDA", "2026-10-02 14:01:00", "gfill_replay")

    publish_ticks_to_lake(frame, lake_root=lake, batch_id="gfill_replay", writer_id="gap_fill", sequence=0)
    publish_ticks_to_lake(frame, lake_root=lake, batch_id="gfill_replay", writer_id="gap_fill", sequence=0)

    files = _published_files(lake)
    assert len(files) == 1
    assert len(list((lake / "_control" / "receipts").glob("gfill_*.json"))) == 1
    assert pq.read_table(files[0]).num_rows == 1


def test_named_batch_receipt_records_the_request_scope(tmp_path):
    """The receipt carries the request scope that produced it."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    frame = _quote_frame("NVDA", "2026-10-02 14:01:00", "gfill_scope")
    scope = _request_scope("gfill_scope")

    publish_ticks_to_lake(
        frame,
        lake_root=lake,
        batch_id="gfill_scope",
        writer_id="gap_fill",
        sequence=0,
        request_scope=scope,
    )

    receipt = json.loads(
        (lake / "_control" / "receipts" / "gfill_scope.json").read_text(encoding="utf-8")
    )
    assert receipt["request"] == scope
    assert receipt["request"]["batch_id"] == receipt["batch_id"]


def test_named_empty_batch_publishes_a_zero_row_receipt(tmp_path):
    """An empty named response is a recoverable publication, not a silent no-op."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)

    written = publish_ticks_to_lake(
        pd.DataFrame(),
        lake_root=lake,
        batch_id="gfill_empty",
        writer_id="gap_fill",
        sequence=0,
        request_scope=_request_scope("gfill_empty"),
    )

    assert written == 0
    assert _published_files(lake) == []
    receipt = json.loads(
        (lake / "_control" / "receipts" / "gfill_empty.json").read_text(encoding="utf-8")
    )
    assert receipt["row_count"] == 0
    assert receipt["file_paths"] == []
    assert receipt["request"]["batch_id"] == "gfill_empty"


def test_legacy_unnamespaced_receipt_still_replays(tmp_path):
    """Receipts written before the namespace change keep replaying."""
    lake = tmp_path / "lake"
    init_tick_lake(lake)
    frame = _quote_frame("NVDA", "2026-10-02 14:01:00", "gfill_legacy")
    records = frame.to_dict("records")

    with LakePublisher(root=lake, writer_id="gap_fill") as publisher:
        publisher.publish_batch(records, batch_id="gfill_legacy", sequence=0)
    legacy_files = _published_files(lake)
    assert [path.name for path in legacy_files] == ["batch_gap_fill_000000.parquet"]

    written = publish_ticks_to_lake(
        frame, lake_root=lake, batch_id="gfill_legacy", writer_id="gap_fill", sequence=0
    )

    assert written == 1
    files = _published_files(lake)
    assert [path.name for path in files] == ["batch_gap_fill_000000.parquet"]
    assert len(list((lake / "_control" / "receipts").glob("gfill_*.json"))) == 1
    assert pq.read_table(files[0]).num_rows == 1
