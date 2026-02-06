import sys
import pandas as pd
from datetime import datetime
from src.database.connection import get_db_connection
from src.database.operations import get_symbol_map_from_db
from src.data.harvester import fetch_from_source
from src.config import UTC

class Logger:
    def log(self, msg):
        print(msg)

def test_aapl_harvest():
    print("🔬 DEBUG: Simulate AAPL Harvest for 2026-01-23")
    logger = Logger()
    
    # Target Date: 2026-01-23 (Friday)
    target_date = datetime(2026, 1, 23).date()
    
    # 1. Get Inventory Rules
    print("\n1. Fetching Inventory Rules for AAPL...")
    db_map = get_symbol_map_from_db()
    if "AAPL" not in db_map:
        print("❌ AAPL not in db_map!")
        return

    rules = db_map["AAPL"]
    print(f"   Rules: {rules}")
    
    p1 = rules.get('p1', 'UNKNOWN')
    p2 = rules.get('p2', 'UNKNOWN')
    t_y = rules.get('yahoo_ticker', 'AAPL')
    t_m = rules.get('massive_ticker', 'AAPL')
    
    # 2. Try Massive (P1)
    print(f"\n2. Attempting P1: {p1} (Ticker: {t_m})")
    df_m, msg_m = fetch_from_source(p1, t_m, target_date, logger)
    print(f"   Result: {msg_m}")
    if not df_m.empty:
        print(f"   ✅ Data Found: {len(df_m)} rows")
    else:
        print("   ❌ Empty DataFrame")
        
    # 3. Try Yahoo (P2)
    print(f"\n3. Attempting P2: {p2} (Ticker: {t_y})")
    df_y, msg_y = fetch_from_source(p2, t_y, target_date, logger)
    print(f"   Result: {msg_y}")
    if not df_y.empty:
        print(f"   ✅ Data Found: {len(df_y)} rows")
        print("   Sample Data:")
        print(df_y.head())
    else:
        print("   ❌ Empty DataFrame")

if __name__ == "__main__":
    test_aapl_harvest()
