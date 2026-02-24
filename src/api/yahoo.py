"""
Yahoo Finance data fetching.
"""
import pandas as pd
import yfinance as yf
from datetime import datetime


def fetch_yahoo_market_data(ticker: str, start_dt: datetime, end_dt: datetime, logger) -> pd.DataFrame:
    """
    Fetches 1-min Yahoo Finance data within a UTC range.
    """
    try:
        # Yahoo expects YYYY-MM-DD for start/end in download(), 
        # then we filter locally for the exact UTC range.
        df = yf.download(
            ticker, 
            start=start_dt.strftime('%Y-%m-%d'), 
            end=(end_dt + pd.Timedelta(days=1)).strftime('%Y-%m-%d'), 
            interval="1m", 
            prepost=True,  
            progress=False,
            auto_adjust=False
        )
        
        if df.empty:
            return pd.DataFrame()
        
        # Ensure timezone awareness
        if df.index.tz is None:
            df.index = df.index.tz_localize('US/Eastern')
        
        # Convert index to UTC for filtering
        df.index = df.index.tz_convert('UTC')
        
        # Filter strictly for the range
        mask = (df.index >= start_dt) & (df.index < end_dt)
        df = df.loc[mask].copy()
        
        return df
        
    except Exception as e:
        logger.log(f"   ❌ Error fetching Yahoo data: {e}")
        return pd.DataFrame()