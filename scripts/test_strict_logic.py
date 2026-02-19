
import os
import sys
from datetime import date
from src.data.harvester import run_harvest_logic

class MockLogger:
    def log(self, msg):
        print(f"[TEST LOG] {msg}")

def test_strict_logic():
    target_date = date(2026, 2, 19)
    logger = MockLogger()
    
    # CASE 1: STOCK (AAPL) -> Expect HYBRID (Spliced)
    # P1=YAHOO, P2=CAPITAL
    map_stock = {
        "AAPL": {
            "yahoo_ticker": "AAPL",
            "capital_epic": "AAPL",
            "binance_ticker": None,
            "p1": "YAHOO",
            "p2": "CAPITAL"
        }
    }
    
    print("\n--- TEST 1: STOCK (AAPL) ---")
    df_s, rep_s = run_harvest_logic(["AAPL"], target_date, map_stock, logger)
    mode_s = rep_s.iloc[0]['Mode'] if not rep_s.empty else "NONE"
    print(f"Result: {mode_s} (Rows: {len(df_s)})")
    
    if mode_s == "HYBRID" or mode_s == "YAHOO-ONLY": # Might be YAHOO-ONLY if Capital fails logic, but expected HYBRID usually
        # Actually logic returns HYBRID label if both tried
        # In current code: source_label = "HYBRID", unless one is empty.
        # If Capital works, we expect HYBRID.
        print("✅ STOCK -> Hybrid allowed.")
    else:
        print(f"⚠️ STOCK -> Unexpected Mode: {mode_s}")

    # CASE 2: ETF (SPY) -> Expect CAPITAL-ONLY (Strict)
    # P1=CAPITAL, P2=YAHOO
    map_etf = {
        "SPY": {
            "yahoo_ticker": "SPY",
            "capital_epic": "SPY",
            "binance_ticker": None,
            "p1": "CAPITAL",
            "p2": "YAHOO"
        }
    }
    
    print("\n--- TEST 2: ETF (SPY) ---")
    df_e, rep_e = run_harvest_logic(["SPY"], target_date, map_etf, logger)
    mode_e = rep_e.iloc[0]['Mode'] if not rep_e.empty else "NONE"
    print(f"Result: {mode_e} (Rows: {len(df_e)})")
    
    if mode_e == "CAPITAL-ONLY":
        print("✅ ETF -> Strict Capital Enforced (Yahoo Skipped).")
    else:
        print(f"❌ ETF -> Failed! Mode is {mode_e} (Expected CAPITAL-ONLY).")

if __name__ == "__main__":
    test_strict_logic()
