from datetime import datetime, date
import pandas as pd
from src.data.harvester import run_harvest_logic
from src.database.operations import get_symbol_map_from_db
from src.config import US_EASTERN

class MockLogger:
    def log(self, msg):
        print(f"   [LOG] {msg}")

def verify_hybrid():
    print("🔬 SPLICED HYBRID VERIFICATION (AAPL)")
    logger = MockLogger()
    
    # Yesterday: Feb 18, 2026
    target_date = date(2026, 2, 18)
    
    # 1. Update Symbol Map locally for test if needed (or fetch from DB)
    db_map = get_symbol_map_from_db()
    
    # Ensure AAPL has the correct keys
    # Note: AAPL on Capital is usually 'AAPL'
    if "AAPL" not in db_map:
        print("⚠️ AAPL not in DB. Using defaults.")
        db_map["AAPL"] = {
            "yahoo_ticker": "AAPL",
            "capital_epic": "AAPL",
            "binance_ticker": None,
            "p1": "YAHOO",
            "p2": "CAPITAL"
        }
    
    print(f"📬 Harvesting AAPL on {target_date}...")
    final_df, report_df = run_harvest_logic(
        tickers_to_harvest=["AAPL"],
        target_date=target_date,
        db_map=db_map,
        logger=logger
    )
    
    if final_df.empty:
        print("❌ FAILED: No data harvested.")
    else:
        print("✅ SUCCESS!")
        print(f"📊 Total Rows: {len(final_df)}")
        
        pre = final_df[final_df['session'] == 'PRE']
        reg = final_df[final_df['session'] == 'REG']
        post = final_df[final_df['session'] == 'POST']
        
        print(f"   - PRE:  {len(pre)} rows")
        print(f"   - REG:  {len(reg)} rows (Volume check: {reg['volume'].sum():.0f})")
        print(f"   - POST: {len(post)} rows")
        
        if not reg.empty and reg['volume'].sum() > 0:
            print("   💎 Volume confirmed for Regular Session.")
        elif not reg.empty:
            print("   ⚠️ Regular session has 0 volume? (Check if Yahoo worked)")

        if not pre.empty:
            print(f"   🕒 Pre-market sample: {pre.iloc[0]['timestamp']} ET")

if __name__ == "__main__":
    verify_hybrid()
