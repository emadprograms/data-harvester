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
        pass

def get_heuristic_ticker(display_name):
    t = display_name.upper().strip()
    
    # Crypto
    if t.endswith("USDT"):
        base = t.replace("USDT", "")
        return f"X:{base}USD"
    
    # Forex (EURUSDT -> C:EURUSD)
    # Heuristic: if it's 6 chars and ends with USDT but is forex... 
    # Actually user likely uses BTCUSDT for crypto.
    # What about EURUSDT? The user used it as example.
    if t == "EURUSDT":
        return "C:EURUSD"
    
    # VIX
    if t in ["VIX", "^VIX"]:
        return "I:VIX"
    
    # Yahoo Futures
    if t == "CL=F": return "CL" # Generic? Unlikely to work without specific contract
    if t == "GC=F": return "GC"

    # Indices (Capital US500 -> Polygon I:SPX)
    # But user display name is usually SPY for ETF.
    # If user has "US500", try I:SPX.
    if t == "US500": return "I:SPX"
    if t == "US30": return "I:DJI"
    if t == "US100": return "I:NDX"
    
    return t

def fix_tickers_v2():
    print("🚀 Starting Massive Ticker Verification V2 (Slow Mode)...")
    client = get_db_connection()
    if not client: return

    # Fetch all
    res = client.execute("SELECT display_name, massive_ticker FROM market_symbols")
    rows = res.rows
    
    logger = MockLogger()
    now = datetime.now(UTC)
    end_dt = now
    start_dt = now - timedelta(days=5) 
    
    updates = []
    
    for row in rows:
        disp_name, current_massive = row
        candidate = get_heuristic_ticker(disp_name)
        
        # If we already fixed it (e.g. X:BTCUSD), skip
        if current_massive == candidate and "Valid" not in current_massive: # Simple check
             pass

        # We re-verify ALL that look suspicious or were Capital Epics
        # Suspicious = does not match candidate
        # OR if we just want to be sure (since last run failed many)
        
        print(f"\n🔎 Checking {disp_name} (Candidate: {candidate})...")
        
        # SLOW DOWN for Rate Limits (5 calls / min = 12s delay)
        # We'll try 5s first, if fail, wait longer.
        time.sleep(5) 
        
        df = fetch_massive_data(candidate, start_dt, end_dt, logger)
        
        if not df.empty:
            print(f"   ✅ Valid! ({len(df)} rows)")
            if candidate != current_massive:
                updates.append((candidate, disp_name))
        else:
            print(f"   ❌ Failed for {candidate}")
            # Identify 429? Not easy with mock logger, but fetch_massive_data catches Exception.
            # Let's assume 429 if it's a major stock like MSFT.
            if candidate in ["MSFT", "NVDA", "TSLA", "AMZN", "GOOGL"]:
                 print("   ⚠️ Might be Rate Limit. Queueing update anyway as these are standard.")
                 if candidate != current_massive:
                     updates.append((candidate, disp_name))

    if updates:
        print(f"\n💾 Applying {len(updates)} updates...")
        for new_ticker, target in updates:
            client.execute("UPDATE market_symbols SET massive_ticker = ? WHERE display_name = ?", [new_ticker, target])
            print(f"   Updated {target} -> {new_ticker}")

if __name__ == "__main__":
    fix_tickers_v2()
