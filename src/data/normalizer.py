"""
Data normalization functions to convert raw API data to unified schema.
"""
import pandas as pd
from src.config import SCHEMA_COLS, US_EASTERN, UTC


def normalize_capital_df(df: pd.DataFrame, symbol: str, session_label: str) -> pd.DataFrame:
    """Normalizes Capital.com data to target schema."""
    if df.empty:
        return pd.DataFrame(columns=SCHEMA_COLS)
    df_norm = df.copy()
    df_norm.rename(columns={
        'SnapshotTime': 'timestamp', 
        'Open': 'open', 
        'High': 'high', 
        'Low': 'low', 
        'Close': 'close', 
        'Volume': 'volume'
    }, inplace=True)
    df_norm['symbol'] = symbol
    df_norm['session'] = session_label
    return df_norm[SCHEMA_COLS]


def normalize_yahoo_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Normalizes Yahoo Finance data to target schema."""
    if df.empty:
        return pd.DataFrame(columns=SCHEMA_COLS)
    df_norm = df.copy()
    if isinstance(df_norm.columns, pd.MultiIndex):
        df_norm.columns = df_norm.columns.get_level_values(0)
    df_norm.reset_index(inplace=True)
    df_norm.rename(columns={
        'Datetime': 'timestamp', 
        'Open': 'open', 
        'High': 'high', 
        'Low': 'low', 
        'Close': 'close', 
        'Volume': 'volume'
    }, inplace=True)
    if df_norm['timestamp'].dt.tz is not None:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_convert('UTC')
    else:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
    df_norm['symbol'] = symbol
    df_norm['session'] = 'REG'
    df_norm.columns = [c.lower() for c in df_norm.columns]
    return df_norm[SCHEMA_COLS]
