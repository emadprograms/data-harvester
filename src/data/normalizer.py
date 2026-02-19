"""
Data normalization functions to convert raw API data to unified schema.
"""
import pandas as pd
from src.config import SCHEMA_COLS, US_EASTERN, UTC




def normalize_yahoo_df(df: pd.DataFrame, symbol: str, session_label: str = 'REG') -> pd.DataFrame:
    """Normalizes Yahoo Finance data to target schema."""
    if df.empty:
        return pd.DataFrame(columns=SCHEMA_COLS)
    df_norm = df.copy()
    
    # ... (Previous index handling code) ...
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
    
    # --- VIX FIX: Handle Missing Volume for Indices ---
    if 'volume' not in df_norm.columns:
        df_norm['volume'] = 0.0
    else:
        df_norm['volume'] = df_norm['volume'].fillna(0)
    # --------------------------------------------------

    # ... (Rest of standard normalization) ...
    if df_norm['timestamp'].dt.tz is not None:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_convert('UTC')
    else:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
        
    df_norm['symbol'] = symbol
    df_norm['session'] = session_label
    df_norm.columns = [c.lower() for c in df_norm.columns]
    return df_norm[SCHEMA_COLS]


def normalize_massive_df(df, symbol, session_label="REG"):
    """
    Normalizes Massive (Polygon) data to the standard schema.
    """
    if df.empty:
        return pd.DataFrame(columns=SCHEMA_COLS)

    df = df.copy()
    
    # Rename columns to match Schema (lowercase)
    df.rename(columns={
        "Open": "open",
        "High": "high",
        "Low": "low", 
        "Close": "close",
        "Volume": "volume",
        "SnapshotTime": "timestamp"
    }, inplace=True)
    
    # Ensure UTC Datetime
    if df['timestamp'].dt.tz is None:
         df['timestamp'] = df['timestamp'].dt.tz_localize(UTC)
    else:
         df['timestamp'] = df['timestamp'].dt.tz_convert(UTC)

    df['symbol'] = symbol
    df['session'] = session_label
    
    # Reorder and Select
    final_df = df[SCHEMA_COLS]
    return final_df