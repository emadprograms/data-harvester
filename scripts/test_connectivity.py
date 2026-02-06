import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from src.infisical_manager import InfisicalManager
from src.config import UTC
from src.api.yahoo import fetch_yahoo_market_data

class Logger:
    def log(self, msg):
        print(msg)

def test_connectivity():
    print("🔬 STARTING CONNECTIVITY DIAGNOSTIC\n")
    logger = Logger()
    
    # --- PHASE 1: MASSIVE (POLYGON) ---
    print("--- [MASSIVE / POLYGON] ---")
    mgr = InfisicalManager()
    keys = mgr.get_massive_api_keys()
    print(f"🔑 Detected Keys: {len(keys)}")
    
    if not keys:
        print("❌ CRITICAL: No Massive Keys found in Infisical!")
    else:
        for i, k in enumerate(keys):
            masked = k[:4] + "..." + k[-4:] if len(k) > 8 else "****"
            print(f"\nTesting Key #{i+1} ({masked}):")
            
            # Simple Ticker Test (AAPL)
            url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2026-01-09/2026-01-09"
            params = {"adjusted": "true", "limit": 10, "apiKey": k}
            
            try:
                t0 = time.time()
                resp = requests.get(url, params=params, timeout=10)
                dur = time.time() - t0
                
                print(f"   Status Code: {resp.status_code}")
                if resp.status_code == 200:
                    data = resp.json()
                    res_count = len(data.get("results", []))
                    if res_count > 0:
                        print(f"   ✅ SUCCESS! Got {res_count} rows in {dur:.2f}s")
                    else:
                        print(f"   ⚠️ OK (200) but no data? Status: {data.get('status')}")
                elif resp.status_code == 429:
                    print(f"   ❌ RATE LIMITED (429)")
                elif resp.status_code == 401 or resp.status_code == 403:
                    print(f"   ❌ AUTH FAILED (Check Key Validity)")
                else:
                    print(f"   ❌ Error: {resp.text[:100]}")
                    
            except Exception as e:
                print(f"   ❌ EXCEPTION: {e}")

    # --- PHASE 2: YAHOO FINANCE ---
    print("\n\n--- [YAHOO FINANCE] ---")
    
    # Test AAPL (Simple)
    print("\nTesting 'AAPL' (Stock):")
    try:
        t0 = time.time()
        # We use a date known to have data usually (e.g. recent weekday)
        # But script uses current date dynamic. Let's use fetch_yahoo_market_data's logic
        # target_date is a date object.
        target_date = datetime.now(UTC).date() - timedelta(days=3) # Go back a few days to ensure not weekend/future
        # Adjust if weekend
        if target_date.weekday() > 4: 
             target_date -= timedelta(days=2)
             
        print(f"   Target Date: {target_date}")
        df = fetch_yahoo_market_data("AAPL", target_date, logger)
        if not df.empty:
            print(f"   ✅ SUCCESS! Got {len(df)} rows.")
            print(f"   Sample: {df.index[0]} - {df.index[-1]}")
        else:
            print(f"   ❌ FAILED (Empty DataFrame)")
            
    except Exception as e:
         print(f"   ❌ EXCEPTION: {e}")

    # Test BTC-USD (Special char)
    print("\nTesting 'BTC-USD' (Crypto):")
    try:
        df = fetch_yahoo_market_data("BTC-USD", target_date, logger)
        if not df.empty:
            print(f"   ✅ SUCCESS! Got {len(df)} rows.")
        else:
            print(f"   ❌ FAILED (Empty DataFrame)")
    except Exception as e:
         print(f"   ❌ EXCEPTION: {e}")

if __name__ == "__main__":
    test_connectivity()
