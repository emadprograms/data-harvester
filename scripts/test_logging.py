
import os
import sys
import glob
from datetime import date
from src.data.harvester import run_harvest_logic

class MockLogger:
    def log(self, msg):
        print(f"[TEST LOG] {msg}")

def test_logging_and_logic():
    # 1. Setup
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        
    print(f"Checking logs in {log_dir}...")
    existing_logs = glob.glob(os.path.join(log_dir, "harvest_*.log"))
    count_before = len(existing_logs)

    # 2. Run Harvest Logic (Strict Capital Test)
    target_date = date(2026, 2, 19)
    # Mock Map: SPY with P1=CAPITAL (Should Skip Yahoo)
    mock_map = {
        "SPY": {
            "yahoo_ticker": "SPY",
            "capital_epic": "SPY",
            "binance_ticker": None,
            "p1": "CAPITAL",
            "p2": "YAHOO"
        }
    }
    logger = MockLogger()
    
    print("\nRunning harvest logic for SPY (P1=CAPITAL)...")
    try:
        df, report = run_harvest_logic(["SPY"], target_date, mock_map, logger)
        print(f"Harvest complete. Rows: {len(df)}")
        if not report.empty:
            mode = report.iloc[0]['Mode']
            print(f"Report Mode: {mode}")
            
            # Verify Mode is CAPITAL-ONLY (because Yahoo was skipped)
            # Wait, if Yahoo is skipped, df_yho is empty.
            # source_label logic: if df_yho.empty -> CAPITAL-ONLY.
            if mode == "CAPITAL-ONLY":
                print("✅ SUCCESS: Logic enforced Strict Capital (Skipped Yahoo).")
            else:
                print(f"❌ FAILURE: Mode is {mode}, expected CAPITAL-ONLY.")
                
    except Exception as e:
        print(f"Harvest failed: {e}")
        import traceback
        traceback.print_exc()

    # 3. Verify Log File
    current_logs = glob.glob(os.path.join(log_dir, "harvest_*.log"))
    count_after = len(current_logs)
    
    if count_after > count_before:
        print("✅ SUCCESS: New log file created!")
        latest_log = max(current_logs, key=os.path.getctime)
        print(f"Latest Log: {latest_log}")
    else:
        print("❌ FAILURE: No new log file found.")

if __name__ == "__main__":
    test_logging_and_logic()
