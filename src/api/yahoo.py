"""
Yahoo Finance data fetching.
"""
import pandas as pd
import yfinance as yf


def fetch_yahoo_market_data(ticker: str, target_date_et, logger) -> pd.DataFrame:
    """Fetches 1-min Yahoo Finance data for the regular session."""
    try:
        start = target_date_et
        end = start + pd.Timedelta(days=1)
        df = yf.download(
            ticker, 
            start=start.strftime('%Y-%m-%d'), 
            end=end.strftime('%Y-%m-%d'), 
            interval="1m", 
            progress=False
        )
        if df.empty:
            return pd.DataFrame()
        
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        
        df_est = df.tz_convert('US/Eastern')
        df_market = df_est.between_time("09:30", "16:00")
        if df_market.empty:
            logger.log(f"   ⚠️ Yahoo returned data, but none in 9:30-16:00 window.")
        return df_market
    except Exception as e:
        logger.log(f"   ❌ Error fetching Yahoo data: {e}")
        return pd.DataFrame()
