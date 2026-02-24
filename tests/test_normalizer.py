"""
Tests for src/data/normalizer.py — Verify normalization handles edge cases.
"""
import pytest
import pandas as pd
import numpy as np
from src.data.normalizer import normalize_yahoo_df
from src.config import SCHEMA_COLS


class TestNormalizeYahoo:

    def test_empty_dataframe(self):
        """Must return empty DF with correct columns if input is empty."""
        result = normalize_yahoo_df(pd.DataFrame(), "AAPL")
        assert result.empty
        assert list(result.columns) == SCHEMA_COLS

    def test_standard_yahoo_data(self, safe_test_date_str):
        """Standard Yahoo data with timezone-aware index must normalize correctly."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=3, freq="1min", tz="US/Eastern"),
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
        assert len(result) == 3
        assert list(result.columns) == SCHEMA_COLS
        assert result['symbol'].iloc[0] == "AAPL"
        # Timestamps should be UTC after normalization
        assert result['timestamp'].dt.tz is not None

    def test_yahoo_missing_volume(self, safe_test_date_str):
        """VIX and similar indices may have no Volume column — must fill with 0."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=2, freq="1min", tz="US/Eastern"),
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
        assert len(result) == 2
        assert (result['volume'] == 0.0).all()

    def test_yahoo_nan_volume(self, safe_test_date_str):
        """Volume with NaN values must be filled with 0."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=2, freq="1min", tz="US/Eastern"),
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
        assert (result['volume'] == 0).all()

    def test_yahoo_naive_timestamps(self, safe_test_date_str):
        """Naive (no timezone) timestamps must be localized to US/Eastern then converted to UTC."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=2, freq="1min"),
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
        assert result['timestamp'].dt.tz is not None

    def test_yahoo_multiindex_columns(self, safe_test_date_str):
        """Yahoo sometimes returns MultiIndex columns — must flatten them."""
        idx = pd.DatetimeIndex(
            pd.date_range(f"{safe_test_date_str} 09:30", periods=2, freq="1min", tz="US/Eastern"),
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
        assert len(result) == 2
        assert list(result.columns) == SCHEMA_COLS
