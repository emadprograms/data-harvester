"""
Tests for Data Integrity & Health Engine (Phase 7).
Validates 1-minute historical gap detection, stream quiet interval detection,
OHLCV anomaly verification, cross-database price drift reconciliation, and health reporting.
"""
import os
import tempfile
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta

from src.database.connection import DuckDBClient
from src.database.schema import init_historical_db, init_streaming_db
from src.database.operations import save_data_to_storage, save_ticks_to_storage
from src.utils.integrity import (
    detect_1m_gaps,
    detect_stream_quiet_intervals,
    validate_ohlcv_anomalies,
    analyze_price_drift,
    get_database_health_report,
)


@pytest.fixture
def temp_integrity_dbs():
    """Provides temporary isolated historical and streaming databases for integrity testing."""
    temp_dir = tempfile.mkdtemp()
    hist_path = os.path.join(temp_dir, "test_hist.duckdb")
    stream_path = os.path.join(temp_dir, "test_stream.duckdb")

    h_client = DuckDBClient(hist_path)
    s_client = DuckDBClient(stream_path)

    init_historical_db(h_client)
    init_streaming_db(s_client)

    yield {
        "dir": temp_dir,
        "hist_path": hist_path,
        "stream_path": stream_path,
        "hist_client": h_client,
        "stream_client": s_client,
    }

    h_client.close()
    s_client.close()
    try:
        import shutil
        shutil.rmtree(temp_dir)
    except Exception:
        pass


def test_detect_1m_gaps_clean_series(temp_integrity_dbs):
    """Verify that continuous 1-minute data has 0 gaps and passes."""
    h_client = temp_integrity_dbs["hist_client"]
    t0 = datetime(2026, 9, 25, 14, 0, 0, tzinfo=timezone.utc)

    # 10 continuous 1-minute candles
    rows = []
    for i in range(10):
        ts = t0 + timedelta(minutes=i)
        rows.append({
            "timestamp": ts,
            "symbol": "SPY",
            "open": 580.0, "high": 581.0, "low": 579.5, "close": 580.5,
            "volume": 100.0, "session": "REG", "source": "MASSIVE"
        })
    save_data_to_storage(pd.DataFrame(rows), archive_client=h_client)

    res = detect_1m_gaps("SPY", t0, t0 + timedelta(minutes=15), client=h_client)
    assert res["passed"] is True
    assert res["missing_minutes"] == 0
    assert len(res["gaps"]) == 0
    assert res["coverage_pct"] == 100.0


def test_detect_1m_gaps_with_artificial_gap(temp_integrity_dbs):
    """Verify that missing minutes between candles are detected accurately."""
    h_client = temp_integrity_dbs["hist_client"]
    t0 = datetime(2026, 9, 25, 14, 0, 0, tzinfo=timezone.utc)

    # Candles at min 0, 1, 2, then skip to min 10
    rows = [
        {"timestamp": t0, "symbol": "QQQ", "open": 490.0, "high": 491.0, "low": 489.0, "close": 490.5, "volume": 50.0, "session": "REG", "source": "MASSIVE"},
        {"timestamp": t0 + timedelta(minutes=1), "symbol": "QQQ", "open": 490.5, "high": 491.5, "low": 490.0, "close": 491.0, "volume": 60.0, "session": "REG", "source": "MASSIVE"},
        {"timestamp": t0 + timedelta(minutes=2), "symbol": "QQQ", "open": 491.0, "high": 492.0, "low": 490.5, "close": 491.5, "volume": 70.0, "session": "REG", "source": "MASSIVE"},
        {"timestamp": t0 + timedelta(minutes=10), "symbol": "QQQ", "open": 495.0, "high": 496.0, "low": 494.0, "close": 495.5, "volume": 80.0, "session": "REG", "source": "MASSIVE"},
    ]
    save_data_to_storage(pd.DataFrame(rows), archive_client=h_client)

    res = detect_1m_gaps("QQQ", t0, t0 + timedelta(minutes=15), client=h_client)
    assert res["passed"] is False
    assert res["missing_minutes"] == 7  # from minute 2 to 10 is 8 min difference -> 7 missing bars
    assert len(res["gaps"]) == 1
    gap = res["gaps"][0]
    assert gap["missing_minutes"] == 7
    assert res["coverage_pct"] < 100.0


def test_detect_stream_quiet_intervals_clean(temp_integrity_dbs):
    """Verify stream continuity detects normal flow with no quiet intervals."""
    s_client = temp_integrity_dbs["stream_client"]
    now = datetime.now(timezone.utc)

    # Insert ticks arriving every 15 seconds
    ticks = [
        (now - timedelta(seconds=60 - i * 15), "AAPL", 150.0, 1.0, 149.9, 150.1, "CAPITAL", "REG")
        for i in range(5)
    ]
    save_ticks_to_storage(s_client, ticks)

    res = detect_stream_quiet_intervals("AAPL", lookback_minutes=5, threshold_seconds=30, client=s_client)
    assert len(res["quiet_intervals"]) == 0
    assert res["ticks_in_window"] == 5


def test_detect_stream_quiet_intervals_with_gap(temp_integrity_dbs):
    """Verify stream continuity flags intervals exceeding threshold."""
    s_client = temp_integrity_dbs["stream_client"]
    now = datetime.now(timezone.utc)

    # Tick 1: 5 minutes ago, Tick 2: 1 minute ago (gap of 240 seconds > threshold 120)
    ticks = [
        (now - timedelta(minutes=5), "NVDA", 120.0, 1.0, 119.9, 120.1, "CAPITAL", "REG"),
        (now - timedelta(minutes=1), "NVDA", 121.0, 1.0, 120.9, 121.1, "CAPITAL", "REG"),
    ]
    save_ticks_to_storage(s_client, ticks)

    res = detect_stream_quiet_intervals("NVDA", lookback_minutes=10, threshold_seconds=120, client=s_client)
    assert len(res["quiet_intervals"]) == 1
    assert res["quiet_intervals"][0]["gap_seconds"] >= 230


def test_validate_ohlcv_anomalies_clean(temp_integrity_dbs):
    """Verify anomaly check passes on logically sound OHLCV data."""
    h_client = temp_integrity_dbs["hist_client"]
    t0 = datetime(2026, 9, 25, 14, 0, 0, tzinfo=timezone.utc)

    df = pd.DataFrame([{
        "timestamp": t0,
        "symbol": "TSLA",
        "open": 250.0, "high": 255.0, "low": 248.0, "close": 252.0,
        "volume": 5000.0, "session": "REG", "source": "MASSIVE"
    }])
    save_data_to_storage(df, archive_client=h_client)

    res = validate_ohlcv_anomalies("TSLA", client=h_client)
    assert res["passed"] is True
    assert res["anomaly_count"] == 0


def test_validate_ohlcv_anomalies_detects_bad_data(temp_integrity_dbs):
    """Verify anomaly check flags high < low and negative price records."""
    h_client = temp_integrity_dbs["hist_client"]
    t0 = datetime(2026, 9, 25, 14, 0, 0, tzinfo=timezone.utc)

    # Bad rows: high < low, and negative price
    rows = [
        {"timestamp": t0, "symbol": "BAD", "open": 100.0, "high": 90.0, "low": 110.0, "close": 95.0, "volume": 10.0, "session": "REG", "source": "TEST"},
        {"timestamp": t0 + timedelta(minutes=1), "symbol": "BAD", "open": -5.0, "high": 10.0, "low": -10.0, "close": 5.0, "volume": 10.0, "session": "REG", "source": "TEST"}
    ]
    # Direct insert bypassing df clean to simulate corrupted raw records
    for r in rows:
        h_client.execute(
            "INSERT INTO market_data (timestamp, symbol, open, high, low, close, volume, session, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [str(r["timestamp"]), r["symbol"], r["open"], r["high"], r["low"], r["close"], r["volume"], r["session"], r["source"]]
        )

    res = validate_ohlcv_anomalies("BAD", client=h_client)
    assert res["passed"] is False
    assert res["anomaly_count"] == 2
    reasons = [a["reason"] for a in res["anomalies"]]
    assert "HIGH_LESS_THAN_LOW" in reasons
    assert "NON_POSITIVE_PRICE" in reasons


def test_analyze_price_drift_reconciliation(temp_integrity_dbs):
    """Verify cross-database drift calculation between historical candles and streaming ticks."""
    h_client = temp_integrity_dbs["hist_client"]
    s_client = temp_integrity_dbs["stream_client"]
    hist_path = temp_integrity_dbs["hist_path"]
    stream_path = temp_integrity_dbs["stream_path"]

    t0 = datetime(2026, 9, 25, 15, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 9, 25, 15, 1, 0, tzinfo=timezone.utc)

    # 1. Historical candles
    hist_df = pd.DataFrame([
        {"timestamp": t0, "symbol": "AMD", "open": 160.0, "high": 161.0, "low": 159.0, "close": 160.50, "volume": 100.0, "session": "REG", "source": "MASSIVE"},
        {"timestamp": t1, "symbol": "AMD", "open": 160.5, "high": 162.0, "low": 160.0, "close": 161.80, "volume": 150.0, "session": "REG", "source": "MASSIVE"},
    ])
    save_data_to_storage(hist_df, archive_client=h_client)

    # 2. Streaming ticks matching timestamps closely (drift 0.05 on bar 1, 0.02 on bar 2)
    stream_ticks = [
        (t0 + timedelta(seconds=59), "AMD", 160.55, 1.0, 160.50, 160.60, "CAPITAL", "REG"),
        (t1 + timedelta(seconds=59), "AMD", 161.82, 1.0, 161.80, 161.85, "CAPITAL", "REG"),
    ]
    save_ticks_to_storage(s_client, stream_ticks)

    # Close initial write connections before attach
    h_client.close()
    s_client.close()

    res = analyze_price_drift("AMD", tolerance=0.10, hist_path=hist_path, stream_path=stream_path)
    assert res["passed"] is True
    assert res["overlapping_bars"] == 2
    assert res["mean_absolute_drift"] < 0.10
    assert res["max_drift"] == pytest.approx(0.05, abs=0.001)

    # Reopen fixtures for clean teardown
    temp_integrity_dbs["hist_client"] = DuckDBClient(hist_path)
    temp_integrity_dbs["stream_client"] = DuckDBClient(stream_path)


def test_get_database_health_report(temp_integrity_dbs):
    """Verify database health report computes sizes, counts, and status."""
    hist_path = temp_integrity_dbs["hist_path"]
    stream_path = temp_integrity_dbs["stream_path"]

    report = get_database_health_report(historical_path=hist_path, streaming_path=stream_path)
    assert report["status"] == "HEALTHY"
    assert report["historical"]["exists"] is True
    assert report["historical"]["size_mb"] >= 0.0
    assert report["streaming"]["exists"] is True
    assert report["streaming"]["size_mb"] >= 0.0
