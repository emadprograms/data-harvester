"""
Yahoo Finance data fetching.
"""
import pandas as pd
import yfinance as yf


def fetch_yahoo_market_data(ticker: str, target_date_et, logger) -> pd.DataFrame:
    """
    Fetches 1-min Yahoo Finance data for the FULL day (including Pre/Post).
    """
    try:
        start = target_date_et
        end = start + pd.Timedelta(days=1)
        
        # Added prepost=True to get extended hours
        df = yf.download(
            ticker, 
            start=start.strftime('%Y-%m-%d'), 
            end=end.strftime('%Y-%m-%d'), 
            interval="1m", 
            prepost=True,  
            progress=False,
            auto_adjust=False
        )
        
        if df.empty:
            return pd.DataFrame()
        
        # Ensure timezone awareness
        if df.index.tz is None:
            # Yahoo usually returns NY time, but sometimes naive. 
            # Safer to localize to America/New_York if naive.
            df.index = df.index.tz_localize('US/Eastern')
        else:
            df.index = df.index.tz_convert('US/Eastern')
            
        # We NO LONGER filter for 9:30-16:00 here. 
        # We return the whole dataset and let the Harvester slice it.
        
        return df
        
    except Exception as e:
        logger.log(f"   ❌ Error fetching Yahoo data: {e}")
        return pd.DataFrame()