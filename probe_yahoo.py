import yfinance as yf
import pandas as pd

tickers = ["EURUSDT", "EURUSD=X", "BTC-USD", "BTCUSDT"]
print("Using yfinance version:", yf.__version__)

for t in tickers:
    print(f"Checking {t}...")
    try:
        # Fetching strictly 1 day
        data = yf.download(t, period="1d", interval="1h", progress=False)
        if not data.empty:
            print(f"✅ Found {t} with {len(data)} rows. Last: {data.index[-1]}")
        else:
            print(f"❌ {t} returned empty")
    except Exception as e:
        print(f"❌ {t} error: {e}")
