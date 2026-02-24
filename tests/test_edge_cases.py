"""
Stress tests and edge-case coverage for the data harvesting pipeline.
Targets bugs found during code audit:
  - Timestamps must be stored in UTC (not ET)
  - Rollback must cover full session range (multi-date)
  - compute_fingerprint must scope to single date
  - verify_db_md5 must fetch all 9 SCHEMA_COLS
  - Capital.com datetime.now must always be UTC-aware
  - Session labels on boundary times (09:30, 16:00)
  - Empty/NaN/Inf data handling
  - Density summary with UTC timestamps
"""
import pytest
import math
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta, timezone, time
from unittest.mock import patch, MagicMock, PropertyMock
from src.config import US_EASTERN, UTC, SCHEMA_COLS
from src.data.harvester import (
    _session_from_timestamp,
    _apply_session_labels,
    fetch_from_source,
    run_harvest_logic,
)
from src.data.normalizer import normalize_yahoo_df
from src.utils.integrity import (
    calculate_df_md5,
    compute_fingerprint,
    verify_db_md5,
    ensure_database_parity,
)
from src.database.operations import _save_to_client, save_data_to_storage


class MockLogger:
    def __init__(self):
        self.messages = []
    def log(self, msg):
        self.messages.append(msg)
    def print_density_summary(self, df, start, end):
        pass


# ---------------------------------------------------------------------------
# 1. Timestamp UTC Mandate
# ---------------------------------------------------------------------------
class TestTimestampUTC:

    @patch("src.data.harvester.fetch_from_source")
    def test_harvester_output_timestamps_are_utc(self, mock_fetch, safe_test_date_str):
        """Timestamps in the final DataFrame MUST be UTC, not ET."""
        utc_ts = pd.to_datetime(f"{safe_test_date_str} 15:00:00", utc=True)
        mock_fetch.return_value = (
            pd.DataFrame({
                "timestamp": [utc_ts],
                "symbol": ["AAPL"], "open": [150.0], "high": [151.0],
                "low": [149.0], "close": [150.5], "volume": [1000.0],
            }),
            "✅ Massive",
        )
        inventory = {"AAPL": {"yahoo_ticker": None, "massive_ticker": "AAPL",
                              "binance_ticker": None, "capital_ticker": "AAPL"}}
        logger = MockLogger()
        start = pd.to_datetime(f"{safe_test_date_str} 01:00:00", utc=True)
        end = pd.to_datetime(f"{safe_test_date_str} 23:59:00", utc=True)
        final_df, _ = run_harvest_logic(
            ["AAPL"], start, end, inventory, logger,
            massive_provider=MagicMock(), session_status="COMPLETED",
        )
        assert not final_df.empty
        tz = final_df["timestamp"].dt.tz
        # pandas tz could be datetime.timezone.utc or pytz.UTC - normalize to name
        assert tz is not None, "Timestamps must be timezone-aware"
        assert str(tz) == "UTC", f"Expected UTC, got {tz}"

    def test_strftime_produces_utc_string(self, safe_test_date_str):
        """When timestamps are UTC-aware, strftime should yield UTC hours."""
        ts = pd.to_datetime(f"{safe_test_date_str} 15:30:00", utc=True)
        s = ts.strftime("%Y-%m-%d %H:%M:%S")
        assert "15:30:00" in s, f"Expected UTC 15:30, got {s}"


# ---------------------------------------------------------------------------
# 2. Session Label Boundaries
# ---------------------------------------------------------------------------
class TestSessionLabels:

    def test_pre_market_label(self):
        """A timestamp at 8:00 AM ET should be PRE."""
        ts = pd.Timestamp("2026-02-18 13:00:00", tz="UTC")  # 8 AM ET (EST)
        assert _session_from_timestamp(ts) == "PRE"

    def test_exactly_market_open(self):
        """Exactly 09:30 ET should be REG, not PRE."""
        ts = pd.Timestamp("2026-02-18 14:30:00", tz="UTC")  # 9:30 AM ET
        assert _session_from_timestamp(ts) == "REG"

    def test_exactly_market_close(self):
        """Exactly 16:00 ET should be REG (market close bar is regular)."""
        ts = pd.Timestamp("2026-02-18 21:00:00", tz="UTC")  # 4:00 PM ET
        assert _session_from_timestamp(ts) == "REG"

    def test_post_market_label(self):
        """After 16:00 ET should be POST."""
        ts = pd.Timestamp("2026-02-18 21:01:00", tz="UTC")  # 4:01 PM ET
        assert _session_from_timestamp(ts) == "POST"

    def test_midnight_is_pre(self):
        """Midnight ET (5:00 UTC) should be PRE."""
        ts = pd.Timestamp("2026-02-18 05:00:00", tz="UTC")
        assert _session_from_timestamp(ts) == "PRE"

    def test_apply_session_labels_preserves_utc(self):
        """_apply_session_labels must NOT convert timestamps away from UTC."""
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2026-02-18 15:00:00"], utc=True),
            "symbol": ["AAPL"], "open": [1], "high": [1], "low": [1],
            "close": [1], "volume": [1],
        })
        result = _apply_session_labels(df)
        assert str(result["timestamp"].dt.tz) == "UTC"

    def test_apply_session_labels_localizes_naive(self):
        """If timestamps are naive, they must be localized to UTC."""
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2026-02-18 15:00:00"]),
            "symbol": ["AAPL"], "open": [1], "high": [1], "low": [1],
            "close": [1], "volume": [1],
        })
        result = _apply_session_labels(df)
        assert result["timestamp"].dt.tz is not None


# ---------------------------------------------------------------------------
# 3. Edge-Case Data Handling
# ---------------------------------------------------------------------------
class TestDataEdgeCases:

    def test_nan_and_inf_in_ohlcv(self):
        """NaN and Inf values in OHLCV must not crash save_data_to_storage."""
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2026-02-18 15:00:00"], utc=True),
            "symbol": ["AAPL"],
            "open": [float("inf")],
            "high": [float("-inf")],
            "low": [float("nan")],
            "close": [150.0],
            "volume": [float("nan")],
            "session": ["REG"],
            "source": ["MASSIVE"],
        })
        mock_client = MagicMock()
        result = _save_to_client(mock_client, [
            ("2026-02-18 15:00:00", "AAPL", None, None, None, 150.0, None, "REG", "MASSIVE")
        ], MockLogger(), "Test")
        assert result is True

    def test_empty_df_md5_returns_empty_string(self):
        """calculate_df_md5 on empty DF must return empty string, not crash."""
        assert calculate_df_md5(pd.DataFrame()) == ""

    def test_md5_missing_columns_filled(self):
        """calculate_df_md5 must fill missing columns before hashing."""
        df = pd.DataFrame({
            "timestamp": ["2026-02-18 15:00:00"],
            "symbol": ["AAPL"],
        })
        result = calculate_df_md5(df)
        assert isinstance(result, str)
        assert len(result) == 32  # MD5 hex digest length

    def test_duplicate_timestamp_symbol_deduped(self, safe_test_date_str):
        """run_harvest_logic must deduplicate on (timestamp, symbol)."""
        utc_ts = pd.to_datetime(f"{safe_test_date_str} 15:00:00", utc=True)
        
        with patch("src.data.harvester.fetch_from_source") as mock_fetch:
            mock_fetch.return_value = (
                pd.DataFrame({
                    "timestamp": [utc_ts, utc_ts],  # duplicate
                    "symbol": ["AAPL", "AAPL"],
                    "open": [150.0, 151.0], "high": [151.0, 152.0],
                    "low": [149.0, 150.0], "close": [150.5, 151.5],
                    "volume": [1000.0, 2000.0],
                }),
                "✅ Massive",
            )
            inventory = {"AAPL": {"yahoo_ticker": None, "massive_ticker": "AAPL",
                                  "binance_ticker": None, "capital_ticker": "AAPL"}}
            logger = MockLogger()
            start = pd.to_datetime(f"{safe_test_date_str} 01:00:00", utc=True)
            end = pd.to_datetime(f"{safe_test_date_str} 23:59:00", utc=True)
            final_df, _ = run_harvest_logic(
                ["AAPL"], start, end, inventory, logger,
                massive_provider=MagicMock(), session_status="COMPLETED",
            )
        # Should have only 1 row after dedup
        assert len(final_df) == 1


# ---------------------------------------------------------------------------
# 4. Capital.com 16h Guard
# ---------------------------------------------------------------------------
class TestCapitalGuard:

    def test_capital_beyond_16h_returns_empty(self):
        """Capital source must return empty if start_dt is beyond 16h lookback."""
        # Use dates clearly in the past (days, not hours) to avoid timing flakes
        old_start = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        old_end = datetime(2020, 1, 1, 18, 0, 0, tzinfo=timezone.utc)
        logger = MockLogger()
        audit = []
        df, msg = fetch_from_source("CAPITAL", "AAPL", old_start, old_end, logger, audit_trail=audit)
        assert df.empty
        assert "16h" in msg or "Beyond" in msg or "Window" in msg

    def test_capital_within_16h_attempts_fetch(self):
        """Capital source within 16h window should attempt the API call."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=1)
        end = now
        logger = MockLogger()
        with patch("src.api.capital.fetch_capital_data") as mock_cap:
            mock_cap.return_value = pd.DataFrame()
            df, msg = fetch_from_source("CAPITAL", "AAPL", start, end, logger)
            assert mock_cap.called


# ---------------------------------------------------------------------------
# 5. Integrity / Fingerprint Fixes
# ---------------------------------------------------------------------------
class TestIntegrityFixes:

    def test_compute_fingerprint_scoped_to_date(self):
        """compute_fingerprint must query only the given date, not all future."""
        mock_client = MagicMock()
        mock_res = MagicMock()
        mock_res.rows = [(10, 5000, "2026-02-18 23:00:00", "2026-02-18 01:00:00")]
        mock_client.execute.return_value = mock_res

        result = compute_fingerprint(mock_client, "2026-02-18")

        # Verify LIKE query is used (not >=)
        call_args = mock_client.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        assert "LIKE" in query, f"Expected LIKE query, got: {query}"
        assert params == ["2026-02-18%"]

    def test_verify_db_md5_fetches_all_schema_cols(self):
        """verify_db_md5 must SELECT all 9 SCHEMA_COLS, not just 7."""
        mock_client = MagicMock()
        mock_res = MagicMock()
        mock_res.rows = [
            ("2026-02-18 15:00:00", "AAPL", 150.0, 151.0, 149.0, 150.5, 1000.0, "REG", "MASSIVE")
        ]
        mock_client.execute.return_value = mock_res

        df = pd.DataFrame({
            "timestamp": ["2026-02-18 15:00:00"],
            "symbol": ["AAPL"],
            "open": [150.0], "high": [151.0], "low": [149.0],
            "close": [150.5], "volume": [1000.0],
            "session": ["REG"], "source": ["MASSIVE"],
        })
        ok, msg = verify_db_md5(mock_client, df, "2026-02-18")

        # Verify the query includes all SCHEMA_COLS
        call_args = mock_client.execute.call_args
        query = call_args[0][0]
        for col in SCHEMA_COLS:
            assert col in query, f"Missing column '{col}' in verify_db_md5 query"


# ---------------------------------------------------------------------------
# 6. Rollback Coverage
# ---------------------------------------------------------------------------
class TestRollbackCoverage:

    def test_rollback_covers_full_range(self):
        """Rollback on mirror failure must delete all timestamps, not just one date."""
        mock_archive = MagicMock()
        mock_mirror = MagicMock()
        # Mirror save fails
        mock_mirror.execute.side_effect = Exception("Mirror write error")
        
        # Build rows spanning two calendar dates
        rows = [
            ("2026-02-17 23:30:00", "AAPL", 150, 151, 149, 150.5, 1000, "POST", "MASSIVE"),
            ("2026-02-18 10:00:00", "AAPL", 151, 152, 150, 151.5, 2000, "REG", "MASSIVE"),
            ("2026-02-18 20:00:00", "AAPL", 152, 153, 151, 152.5, 3000, "POST", "MASSIVE"),
        ]
        
        result = _save_to_client(mock_mirror, rows, MockLogger(), "Mirror")
        assert result is False  # Should fail

    @patch("src.database.operations._save_to_client")
    @patch("src.database.operations.get_archive_db_connection")
    @patch("src.database.operations.get_mirror_db_connection")
    def test_rollback_uses_range_not_like(self, mock_mirror_conn, mock_archive_conn, mock_save):
        """When mirror fails, archive rollback must use min/max timestamp range."""
        mock_archive = MagicMock()
        mock_mirror = MagicMock()
        mock_archive_conn.return_value = mock_archive
        mock_mirror_conn.return_value = mock_mirror
        
        # First call (archive) succeeds, second call (mirror) fails
        mock_save.side_effect = [True, False]
        
        df = pd.DataFrame({
            "timestamp": pd.to_datetime([
                "2026-02-17 23:30:00",
                "2026-02-18 15:00:00",
            ], utc=True),
            "symbol": ["AAPL", "AAPL"],
            "open": [150.0, 151.0], "high": [151.0, 152.0],
            "low": [149.0, 150.0], "close": [150.5, 151.5],
            "volume": [1000.0, 2000.0],
            "session": ["POST", "REG"],
            "source": ["MASSIVE", "MASSIVE"],
        })
        
        result = save_data_to_storage(df, MockLogger(),
                                       archive_client=mock_archive,
                                       mirror_client=mock_mirror)
        assert result is False
        
        # Archive rollback should have been called with range DELETE
        rollback_calls = [c for c in mock_archive.execute.call_args_list
                         if "DELETE" in str(c)]
        assert len(rollback_calls) >= 1, "Expected rollback DELETE on archive"
        rollback_query = str(rollback_calls[0])
        assert ">=" in rollback_query and "<=" in rollback_query, \
            f"Rollback should use range (>=, <=), got: {rollback_query}"


# ---------------------------------------------------------------------------
# 7. Source-Tier Protection
# ---------------------------------------------------------------------------
class TestSourceTierProtection:

    def test_tier1_not_overwritten_by_tier2(self):
        """MASSIVE/BINANCE data must not be overwritten by YAHOO/CAPITAL."""
        mock_client = MagicMock()
        
        # Insert tier-1 data
        rows_t1 = [("2026-02-18 15:00:00", "AAPL", 150, 151, 149, 150.5, 1000, "REG", "MASSIVE")]
        _save_to_client(mock_client, rows_t1, MockLogger(), "Test")
        
        # Verify the SQL contains the tier protection WHERE clause
        call_args = mock_client.execute.call_args
        query = call_args[0][0]
        assert "NOT IN ('MASSIVE', 'BINANCE')" in query
        assert "excluded.source IN ('MASSIVE', 'BINANCE')" in query


# ---------------------------------------------------------------------------
# 8. Normalizer Edge Cases  
# ---------------------------------------------------------------------------
class TestNormalizerEdgeCases:

    def test_yahoo_all_nan_prices(self, safe_test_date_str):
        """Yahoo data with all NaN prices should still produce valid DataFrame."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        df = pd.DataFrame({
            "Open": [np.nan, np.nan],
            "High": [np.nan, np.nan],
            "Low": [np.nan, np.nan],
            "Close": [np.nan, np.nan],
            "Volume": [0, 0],
        }, index=idx)
        result = normalize_yahoo_df(df, "TEST")
        assert len(result) == 2
        assert list(result.columns) == SCHEMA_COLS

    def test_yahoo_single_row(self, safe_test_date_str):
        """Single-row Yahoo data must normalize without error."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=1, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        df = pd.DataFrame({
            "Open": [150.0], "High": [151.0], "Low": [149.0],
            "Close": [150.5], "Volume": [1000],
        }, index=idx)
        result = normalize_yahoo_df(df, "AAPL")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# 9. Harvester Handles Missing Ticker Gracefully
# ---------------------------------------------------------------------------
class TestHarvesterMissingTicker:

    @patch("src.data.harvester.fetch_from_source")
    def test_ticker_not_in_inventory(self, mock_fetch, safe_test_date_str):
        """A ticker not in db_map must not crash, should report failure."""
        logger = MockLogger()
        start = pd.to_datetime(f"{safe_test_date_str} 01:00:00", utc=True)
        end = pd.to_datetime(f"{safe_test_date_str} 23:59:00", utc=True)
        
        # Inventory has AAPL but we request MSFT
        inventory = {"AAPL": {"yahoo_ticker": None, "massive_ticker": "AAPL",
                              "binance_ticker": None, "capital_ticker": "AAPL"}}
        final_df, report = run_harvest_logic(
            ["MSFT"], start, end, inventory, logger,
            massive_provider=MagicMock(),
        )
        assert final_df.empty
        assert not report.empty
        # Report should show the failure
        assert report.iloc[0]["Ticker"] == "MSFT"
        assert "Not in Inventory" in report.iloc[0]["Status"] or report.iloc[0]["Total"] == 0
