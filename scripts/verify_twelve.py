from src.api.twelve_data import fetch_twelve_data
from src.config import UTC
from datetime import datetime, timedelta

class Logger:
    def log(self, msg):
        print(msg)

def verify_twelve():
    print("🔍 Testing Twelve Data (WTI/USD)...")
    logger = Logger()
    
    # Yesterday (Weekday)
    # If today is Sat, yesterday is Fri.
    target = datetime.now(UTC) - timedelta(days=1)
    if target.weekday() > 4: # If Sat/Sun
         target -= timedelta(days=2)
         
    print(f"Target: {target.date()}")
    
    df, err = fetch_twelve_data("WTI/USD", target, target + timedelta(days=1), logger)
    
    if not df.empty:
        print(f"✅ Success! Got {len(df)} rows.")
        print(df.head())
    else:
        print(f"❌ Failed: {err}")

if __name__ == "__main__":
    verify_twelve()
