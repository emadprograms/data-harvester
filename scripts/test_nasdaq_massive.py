import sys
import pandas as pd
from datetime import datetime, timedelta
from src.api.massive import fetch_massive_data
from src.config import UTC

class Logger:
    def log(self, msg):
        print(msg)

def test_nasdaq_indices():
    print("🔬 Verifying Nasdaq Index data on Massive...")
    logger = Logger()
    
    # Use yesterday or a recent weekday
    target_date = datetime.now(UTC) - timedelta(days=1)
    if target_date.weekday() >= 5: # Weekend, go to Friday
        target_date -= timedelta(days=target_date.weekday() - 4)
    
    start_utc = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = start_utc + timedelta(hours=23, minutes=59)
    
    indices = ["I:NDX", "I:IXIC", "NDAQ", "QQQ", "TQQQ"]
    
    for ticker in indices:
        print(f"\n--- Testing Ticker: {ticker} ---")
        df, err = fetch_massive_data(ticker, start_utc, end_utc, logger)
        if not df.empty:
            print(f"✅ Success! Got {len(df)} rows.")
            # Check for pre-market data (before 9:30 AM ET)
            df['t_et'] = df['SnapshotTime'].dt.tz_convert('US/Eastern')
            pre_market = df[df['t_et'].dt.time < pd.Timestamp("09:30:00").time()]
            print(f"   Pre-market rows: {len(pre_market)}")
            print(f"   Time range: {df['t_et'].min()} to {df['t_et'].max()}")
        else:
            print(f"❌ Failed: {err}")

if __name__ == "__main__":
    test_nasdaq_indices()
