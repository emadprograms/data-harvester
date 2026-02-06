import requests
import pandas as pd
from datetime import datetime
from src.infisical_manager import InfisicalManager
from src.config import UTC
from src.api.retry import get_retry_session

def fetch_twelve_data(ticker: str, start_date, end_date, logger) -> tuple[pd.DataFrame, str]:
    """
    Fetches 1-min intraday data from Twelve Data.
    Note: Twelve Data Free Tier limits history. 
    Usually restricts to recent data for 1min interval.
    """
    mgr = InfisicalManager()
    api_key = mgr.get_twelve_data_key()
    
    if not api_key:
        msg = "Missing Twelve Data API Key"
        logger.log(f"   ❌ {msg}")
        return pd.DataFrame(), msg

    # Format dates
    # Twelve Data uses YYYY-MM-DD HH:MM:SS
    # But start_date/end_date passed here are usually 'date' objects or datetimes.
    # We'll use just YYYY-MM-DD for wider coverage request
    
    s_str = start_date.strftime("%Y-%m-%d")
    # For Twelve, end_date is exclusive? 
    # Let's request slightly more to be safe
    e_str = end_date.strftime("%Y-%m-%d") 
    
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": ticker,
        "interval": "1min",
        "start_date": s_str,
        "end_date": e_str,
        "apikey": api_key,
        "outputsize": 5000,
        "timezone": "UTC"
    }
    
    session = get_retry_session()
    try:
        response = session.get(url, params=params, timeout=15)
        data = response.json()
        
        if data.get("status") == "error":
            err = data.get("message", "Unknown Error")
            # Usually strict limit or symbol not found
            logger.log(f"   ❌ Twelve Data Error {ticker}: {err}")
            return pd.DataFrame(), f"Error: {err}"
            
        points = data.get("values", [])
        if not points:
            return pd.DataFrame(), "Empty"
            
        # Parse
        df = pd.DataFrame(points)
        # Columns: datetime, open, high, low, close, volume (sometimes)
        # Note: Twelve Data volume might be missing for Forex/CFD
        
        rename_map = {
            "datetime": "timestamp",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume"
        }
        df.rename(columns=rename_map, inplace=True)
        
        # Ensure Types
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(UTC)
        cols = ['open', 'high', 'low', 'close']
        for c in cols:
            df[c] = pd.to_numeric(df[c])
            
        if 'volume' in df.columns:
            df['volume'] = pd.to_numeric(df['volume'])
        else:
            df['volume'] = 0
            
        # Sort
        df.sort_values('timestamp', inplace=True)
        
        return df, ""

    except Exception as e:
        logger.log(f"   ❌ Twelve Data Exception {ticker}: {e}")
        return pd.DataFrame(), f"Ex: {str(e)[:20]}"
