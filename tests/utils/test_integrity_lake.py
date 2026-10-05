"""Phase 46 DASH-03 — the integrity engine reads the Parquet tick lake only.

v5.0 has no disk-database backend, so the health engine must read the lake and the
bar-era checks (minute-bar gaps, cross-store drift, database fingerprint/MD5) must be
gone rather than left dormant.

Written before the implementation (TDD): the database-independence, removal and
lake-reading assertions are all expected to fail until Phase 46 is implemented.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.storage.parquet_writer import TickLakeWriter
from tests.fixtures.deterministic_quotes import QuoteTick

REPO_ROOT = Path(__file__).resolve().parents[2]

LEGACY_INTEGRITY_SURFACE = (
    "detect_1m_gaps",
    "analyze_price_drift",
    "get_database_health_report",
    "compute_fingerprint",
    "verify_db_md5",
)


def _utc_now_naive() -> datetime:
    """Lake Schema v1 stores UTC-naive timestamps."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _tick(ts: datetime, symbol: str = "AAPL", price: float = 150.0) -> QuoteTick:
    return QuoteTick(
        timestamp=ts,
        symbol=symbol,
        price=price,
        volume=1.0,
        bid=price - 0.05,
        ask=price + 0.05,
        source="CAPITAL",
        session="REG",
        ingest_id=f"{symbol}-{ts.isoformat()}",
    )


def _write_lake(lake_root: Path, ticks) -> None:
    writer = TickLakeWriter(root=lake_root, writer_id="integrity_test", max_batch_rows=10**9)
    writer.write_ticks(list(ticks))
    writer.flush(block=True)
    writer.close()


def test_integrity_module_references_no_database_backend() -> None:
    source = (REPO_ROOT / "src" / "utils" / "integrity.py").read_text(encoding="utf-8")
    for legacy in (
        "src.database",
        "DEFAULT_STREAMING_DB_PATH",
        "DEFAULT_HISTORICAL_DB_PATH",
        "get_streaming_db_connection",
        "get_historical_db_connection",
        "tick_data",
        "minute_data",
    ):
        assert legacy not in source, f"legacy database reference remains in integrity.py: {legacy}"


def test_bar_era_integrity_checks_are_removed() -> None:
    import src.utils.integrity as integrity

    remaining = [name for name in LEGACY_INTEGRITY_SURFACE if hasattr(integrity, name)]
    assert not remaining, f"bar-era integrity checks remain: {remaining}"


def test_detect_stream_quiet_intervals_reads_the_lake(tmp_path, monkeypatch) -> None:
    now = _utc_now_naive()
    lake_root = tmp_path / "lake"
    _write_lake(
        lake_root,
        [
            _tick(now - timedelta(minutes=30), price=150.0),
            _tick(now - timedelta(minutes=5), price=151.0),
        ],
    )
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    from src.utils.integrity import detect_stream_quiet_intervals

    result = detect_stream_quiet_intervals("AAPL", lookback_minutes=60, threshold_seconds=120)

    assert result["ticks_in_window"] == 2
    assert len(result["quiet_intervals"]) == 1
    assert result["quiet_intervals"][0]["gap_seconds"] >= 25 * 60
    assert result["passed"] is False


def test_detect_stream_quiet_intervals_reports_stalled_when_no_recent_ticks(tmp_path, monkeypatch) -> None:
    lake_root = tmp_path / "lake"
    _write_lake(lake_root, [_tick(_utc_now_naive() - timedelta(hours=5))])
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    from src.utils.integrity import detect_stream_quiet_intervals

    result = detect_stream_quiet_intervals("AAPL", lookback_minutes=60, threshold_seconds=120)

    assert result["ticks_in_window"] == 0
    assert result["is_stalled"] is True
    assert result["passed"] is False
    assert result["last_tick_time"] is not None


def test_healthy_stream_passes(tmp_path, monkeypatch) -> None:
    now = _utc_now_naive()
    lake_root = tmp_path / "lake"
    _write_lake(lake_root, [_tick(now - timedelta(seconds=30)), _tick(now - timedelta(seconds=5))])
    monkeypatch.setenv("TICK_LAKE_ROOT", str(lake_root))

    from src.utils.integrity import detect_stream_quiet_intervals

    result = detect_stream_quiet_intervals("AAPL", lookback_minutes=60, threshold_seconds=120)

    assert result["ticks_in_window"] == 2
    assert result["quiet_intervals"] == []
    assert result["is_stalled"] is False
    assert result["passed"] is True
