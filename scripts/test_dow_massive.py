import sys
import pandas as pd
from datetime import datetime, timedelta
from src.api.massive import fetch_massive_data
from src.config import UTC

class Logger:
    def log(self, msg):
        print(msg)

def test_dow_indices():
    print("🔬 Verifying Dow Jones data on Massive...")
    logger = Logger()
    
    # Use Friday (Jan 23)
    target_date = datetime(2026, 1, 23, tzinfo=UTC)
    start_utc = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = start_utc + timedelta(hours=23, minutes=59)
    
    tickers = ["DIA", "I:DJI"]
    
    for ticker in tickers:
        print(f"\n--- Testing Ticker: {ticker} ---")
        df, err = fetch_massive_data(ticker, start_utc, end_utc, logger)
        if not df.empty:
            print(f"✅ Success! Got {len(df)} rows.")
            # Check for pre-market data
            df['t_et'] = df['SnapshotTime'].dt.tz_convert('US/Eastern')
            pre_market = df[df['t_et'].dt.time < pd.Timestamp("09:30:00").time()]
            print(f"   Pre-market rows: {len(pre_market)}")
            if len(pre_market) > 0:
                print(f"   Pre-market range: {pre_market['t_et'].min()} to {pre_market['t_et'].max()}")
            print(f"   Total range: {df['t_et'].min()} to {df['t_et'].max()}")
        else:
            print(f"❌ Failed: {err}")

if __name__ == "__main__":
    test_dow_indices()
