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
    
    # Flatten MultiIndex FIRST (Yahoo sometimes returns these)
    if isinstance(df_norm.columns, pd.MultiIndex):
        df_norm.columns = df_norm.columns.get_level_values(0)
    
    # Now safe to lowercase and deduplicate
    df_norm.columns = [str(c).lower() for c in df_norm.columns]
    df_norm = df_norm.loc[:, ~df_norm.columns.duplicated()]
    
    df_norm.reset_index(inplace=True)
    # Lowercase again to catch the index column name (e.g. 'Datetime' -> 'datetime')
    df_norm.columns = [str(c).lower() for c in df_norm.columns]
    df_norm = df_norm.loc[:, ~df_norm.columns.duplicated()]
    
    # Rename datetime -> timestamp (the only non-obvious mapping)
    if 'datetime' in df_norm.columns:
        df_norm.rename(columns={'datetime': 'timestamp'}, inplace=True)
    
    # --- VIX FIX: Handle Missing Volume for Indices ---
    if 'volume' not in df_norm.columns:
        df_norm['volume'] = 0.0
    
    df_norm['volume'] = df_norm['volume'].fillna(0)
    # --------------------------------------------------

    # ... (Rest of standard normalization) ...
    if df_norm['timestamp'].dt.tz is not None:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_convert('UTC')
    else:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
        
    df_norm['symbol'] = symbol
    df_norm['session'] = session_label
    
    # Final safety check: ensure exactly SCHEMA_COLS in correct order
    return df_norm[SCHEMA_COLS].copy()


def normalize_capital_df(df: pd.DataFrame, symbol: str, session_label: str = 'REG') -> pd.DataFrame:
    """Normalizes Capital.com data to target schema."""
    if df.empty:
        return pd.DataFrame(columns=SCHEMA_COLS)
    
    df_norm = df.copy()
    
    # Normalize column names to lowercase IMMEDIATELY
    df_norm.columns = [c.lower() for c in df_norm.columns]
    df_norm = df_norm.loc[:, ~df_norm.columns.duplicated()]

    df_norm['symbol'] = symbol
    df_norm['session'] = session_label
    
    # Handle missing volume
    if 'volume' not in df_norm.columns:
        df_norm['volume'] = 0.0
    else:
        df_norm['volume'] = df_norm['volume'].fillna(0.0)
    
    # Ensure timestamp is UTC
    if df_norm['timestamp'].dt.tz is None:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_localize('UTC')
    else:
        df_norm['timestamp'] = df_norm['timestamp'].dt.tz_convert('UTC')
        
    return df_norm[SCHEMA_COLS]
