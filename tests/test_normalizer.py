"""
Tests for src/data/normalizer.py — Verify normalization handles edge cases.
"""
import unittest
import pandas as pd
import numpy as np
from src.data.normalizer import normalize_yahoo_df, normalize_massive_df
from src.config import SCHEMA_COLS


class TestNormalizeYahoo(unittest.TestCase):

    def test_empty_dataframe(self):
        """Must return empty DF with correct columns if input is empty."""
        result = normalize_yahoo_df(pd.DataFrame(), "AAPL")
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), SCHEMA_COLS)

    def test_standard_yahoo_data(self):
        """Standard Yahoo data with timezone-aware index must normalize correctly."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=3, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        df = pd.DataFrame({
            "Open": [150.0, 150.5, 151.0],
            "High": [150.5, 151.0, 151.5],
            "Low": [149.5, 150.0, 150.5],
            "Close": [150.2, 150.8, 151.2],
            "Volume": [1000, 2000, 3000],
        }, index=idx)
        
        result = normalize_yahoo_df(df, "AAPL")
        self.assertEqual(len(result), 3)
        self.assertEqual(list(result.columns), SCHEMA_COLS)
        self.assertEqual(result['symbol'].iloc[0], "AAPL")
        # Timestamps should be UTC after normalization
        self.assertIsNotNone(result['timestamp'].dt.tz)

    def test_yahoo_missing_volume(self):
        """VIX and similar indices may have no Volume column — must fill with 0."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        df = pd.DataFrame({
            "Open": [20.0, 20.5],
            "High": [20.5, 21.0],
            "Low": [19.5, 20.0],
            "Close": [20.2, 20.8],
            # No Volume column
        }, index=idx)
        
        result = normalize_yahoo_df(df, "^VIX")
        self.assertEqual(len(result), 2)
        self.assertTrue((result['volume'] == 0.0).all(), 
                        "Missing volume should default to 0.0")

    def test_yahoo_nan_volume(self):
        """Volume with NaN values must be filled with 0."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        df = pd.DataFrame({
            "Open": [150.0, 150.5],
            "High": [150.5, 151.0],
            "Low": [149.5, 150.0],
            "Close": [150.2, 150.8],
            "Volume": [np.nan, np.nan],
        }, index=idx)
        
        result = normalize_yahoo_df(df, "AAPL")
        self.assertTrue((result['volume'] == 0).all())

    def test_yahoo_naive_timestamps(self):
        """Naive (no timezone) timestamps must be localized to US/Eastern then converted to UTC."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min"),
            name="Datetime"
        )
        df = pd.DataFrame({
            "Open": [150.0, 150.5],
            "High": [150.5, 151.0],
            "Low": [149.5, 150.0],
            "Close": [150.2, 150.8],
            "Volume": [1000, 2000],
        }, index=idx)
        
        result = normalize_yahoo_df(df, "AAPL")
        self.assertIsNotNone(result['timestamp'].dt.tz)

    def test_yahoo_multiindex_columns(self):
        """Yahoo sometimes returns MultiIndex columns — must flatten them."""
        idx = pd.DatetimeIndex(
            pd.date_range("2025-01-15 09:30", periods=2, freq="1min", tz="US/Eastern"),
            name="Datetime"
        )
        arrays = [["Open", "High", "Low", "Close", "Volume"], 
                  ["AAPL", "AAPL", "AAPL", "AAPL", "AAPL"]]
        cols = pd.MultiIndex.from_arrays(arrays)
        df = pd.DataFrame(
            [[150.0, 150.5, 149.5, 150.2, 1000],
             [150.5, 151.0, 150.0, 150.8, 2000]], 
            index=idx, columns=cols
        )
        
        result = normalize_yahoo_df(df, "AAPL")
        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.columns), SCHEMA_COLS)


class TestNormalizeMassive(unittest.TestCase):

    def test_empty_dataframe(self):
        """Must return empty DF with correct columns if input is empty."""
        result = normalize_massive_df(pd.DataFrame(), "AAPL")
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), SCHEMA_COLS)

    def test_standard_massive_data(self):
        """Standard Massive data must normalize correctly."""
        df = pd.DataFrame({
            "SnapshotTime": pd.to_datetime(["2025-01-15 14:30:00", "2025-01-15 14:31:00"]).tz_localize("UTC"),
            "Open": [150.0, 150.5],
            "High": [150.5, 151.0],
            "Low": [149.5, 150.0],
            "Close": [150.2, 150.8],
            "Volume": [1000, 2000]
        })
        
        result = normalize_massive_df(df, "AAPL")
        self.assertEqual(len(result), 2)
        self.assertEqual(list(result.columns), SCHEMA_COLS)
        self.assertEqual(result['symbol'].iloc[0], "AAPL")

    def test_massive_naive_timestamps(self):
        """Naive timestamps must be localized to UTC."""
        df = pd.DataFrame({
            "SnapshotTime": pd.to_datetime(["2025-01-15 14:30:00", "2025-01-15 14:31:00"]),
            "Open": [150.0, 150.5],
            "High": [150.5, 151.0],
            "Low": [149.5, 150.0],
            "Close": [150.2, 150.8],
            "Volume": [1000, 2000]
        })
        
        result = normalize_massive_df(df, "AAPL")
        self.assertIsNotNone(result['timestamp'].dt.tz)

    def test_no_capital_normalizer_exists(self):
        """Capital.com normalizer should have been removed."""
        import src.data.normalizer as norm_module
        self.assertFalse(hasattr(norm_module, 'normalize_capital_df'),
                         "normalize_capital_df should have been removed")


if __name__ == '__main__':
    unittest.main()
