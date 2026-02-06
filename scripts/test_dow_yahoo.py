import yfinance as yf
import pandas as pd
from src.config import US_EASTERN, UTC

def test_dow_yahoo():
    print("🔬 Verifying Dow Proxies on Yahoo...")
    
    # Friday Jan 23
    start = "2026-01-23"
    end = "2026-01-24"
    
    tickers = ["DIA", "YM=F", "^DJI"]
    
    for t in tickers:
        print(f"\n--- Testing Ticker: {t} ---")
        df = yf.download(t, start=start, end=end, interval="1m", prepost=True, progress=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            print(f"✅ Success! Got {len(df)} rows.")
            df.index = df.index.tz_convert(US_EASTERN)
            pre_market = df[df.index.time < pd.Timestamp("09:30:00").time()]
            print(f"   Pre-market rows: {len(pre_market)}")
            print(f"   Time range: {df.index.min()} to {df.index.max()}")
        else:
            print(f"❌ Failed (Empty)")

if __name__ == "__main__":
    test_dow_yahoo()
