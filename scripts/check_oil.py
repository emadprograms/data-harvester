from src.database.connection import get_db_connection
from src.api.massive import fetch_massive_data
from src.config import UTC
from datetime import datetime, timedelta

class Logger:
    def log(self, msg):
        print(msg)

def check_oil():
    client = get_db_connection()
    res = client.execute("SELECT display_name, massive_ticker, priority_1 FROM market_symbols WHERE display_name = 'CL=F'")
    if not res.rows:
        print("❌ 'CL=F' not found in DB.")
        return

    row = res.rows[0]
    print(f"Display: {row[0]}")
    print(f"Masive Ticker: {row[1]}")
    print(f"Priority 1: {row[2]}")
    
    # Test Massive Ticker
    print(f"\nTesting Massive with '{row[1]}'...")
    
    target = datetime.now(UTC) - timedelta(days=3) # Past weekday
    
    logger = Logger()
    df, err = fetch_massive_data(row[1], target, target+timedelta(hours=24), logger)
    
    if not df.empty:
        print(f"✅ Success! Got {len(df)} rows.")
    else:
        print(f"❌ Failed: {err}")
        
    # Check if 'CL' works?
    print("\nTesting 'CL' (Generic)...")
    df2, err2 = fetch_massive_data("CL", target, target+timedelta(hours=24), logger)
    if not df2.empty:
        print(f"✅ 'CL' worked! ({len(df2)} rows)")
    else:
        print(f"❌ 'CL' Failed: {err2}")

if __name__ == "__main__":
    check_oil()
