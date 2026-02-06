import sys
import os
import time
from datetime import datetime, timedelta
import pandas as pd
from src.database.connection import get_db_connection
from src.api.massive import fetch_massive_data
from src.config import UTC

class MockLogger:
    def log(self, msg):
        pass # Silent logic

def get_heuristic_ticker(display_name):
    """
    Guesses the Massive/Polygon ticker based on display name.
    """
    t = display_name.upper().strip()
    
    # 1. Crypto: BTCUSDT -> X:BTCUSD
    if t.endswith("USDT"):
        base = t.replace("USDT", "")
        return f"X:{base}USD"
    
    # 2. VIX
    if t == "VIX":
        return "I:VIX"
        
    # 3. Indices often tracked by ETFs
    # If user has "SPY", Massive is "SPY".
    # If user has "US500", Massive is "I:SPX" (S&P 500 Index) OR "SPY" (ETF).
    # Since migration copied Capital epics (e.g. US500, US30), we need to check display_name
    
    # If display name is standard stock like AAPL, AMD -> AAPL, AMD
    return t

def fix_tickers():
    print("🚀 Starting Massive Ticker Verification...")
    client = get_db_connection()
    if not client:
        print("❌ DB Connection Failed")
        return

    # Fetch all
    res = client.execute("SELECT display_name, massive_ticker FROM market_symbols")
    rows = res.rows
    print(f"Found {len(rows)} symbols.")
    
    logger = MockLogger()
    # Test range: Last Friday (ensure data exists)
    now = datetime.now(UTC)
    end_dt = now
    start_dt = now - timedelta(days=5) 
    
    updates = []
    
    for row in rows:
        disp_name, current_massive = row
        
        # 1. Determine Candidate
        candidate = get_heuristic_ticker(disp_name)
        
        print(f"\n🔎 Checking {disp_name}...")
        print(f"   Current: {current_massive}")
        print(f"   Candidate: {candidate}")
        
        # 2. Test Candidate
        # Optimized: If candidate == current, and it looks like a Capital Epic (e.g. US500), we still test.
        # But if candidate is just AAPL, we test to be sure.
        
        # Wait a bit to avoid rate limits
        time.sleep(0.5) 
        
        df = fetch_massive_data(candidate, start_dt, end_dt, logger)
        
        if not df.empty:
            print(f"   ✅ Valid! ({len(df)} rows)")
            if candidate != current_massive:
                updates.append((candidate, disp_name))
                print(f"   📝 Queued update: {current_massive} -> {candidate}")
        else:
            print(f"   ❌ Invalid/No Data for {candidate}")
            # Fallback: try raw display name if candidate was transformation
            if candidate != disp_name:
                print(f"   🔄 Retrying with raw {disp_name}...")
                df_retry = fetch_massive_data(disp_name, start_dt, end_dt, logger)
                if not df_retry.empty:
                    print(f"   ✅ Raw {disp_name} worked!")
                    if disp_name != current_massive:
                        updates.append((disp_name, disp_name))
                else:
                    print("   ❌ Raw failed too.")

    # Apply Updates
    if updates:
        print(f"\n💾 Applying {len(updates)} updates...")
        for new_ticker, target in updates:
            client.execute("UPDATE market_symbols SET massive_ticker = ? WHERE display_name = ?", [new_ticker, target])
            print(f"   Updated {target} -> {new_ticker}")
    else:
        print("\n✨ No updates needed.")

if __name__ == "__main__":
    fix_tickers()
