"""
User Resilience Check: Run this to verify your fallbacks manually.
This script DOES NOT save data to the database; it only tests the fetching pipeline.
"""
import os
import toml
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch

# Load secrets from the other repo for local testing
secrets_path = "../data-viewer/.streamlit/secrets.toml"
if os.path.exists(secrets_path):
    data = toml.load(secrets_path)
    os.environ["INFISICAL_CLIENT_ID"] = data["infisical"]["client_id"]
    os.environ["INFISICAL_CLIENT_SECRET"] = data["infisical"]["client_secret"]
    os.environ["INFISICAL_PROJECT_ID"] = data["infisical"]["project_id"]

from src.data.harvester import run_harvest_logic
from src.database.operations import get_symbol_map_from_db
from src.config import US_EASTERN
from src.infisical_manager import InfisicalManager

# Speed up bootstrapping for the test
with patch.object(InfisicalManager, 'get_massive_api_keys', return_value=["DUMMY_KEY"]):
    pass

class TestLogger:
    def log(self, msg):
        print(f"  [Log] {msg}")

def run_user_test():
    logger = TestLogger()
    target_date = (datetime.now(US_EASTERN) - timedelta(days=1)).date()
    
    symbol_map = get_symbol_map_from_db()
    # Pick one stock and one crypto
    test_tickers = []
    if "AAPL" in symbol_map: test_tickers.append("AAPL")
    if "BTC/USD" in symbol_map: test_tickers.append("BTC/USD")
    
    if not test_tickers:
        test_tickers = list(symbol_map.keys())[:2]

    print(f"🏁 Starting Resilience Check for: {test_tickers}")
    print(f"📅 Target Date: {target_date}")

    print("\n🛡 TEST 1: Normal Harvest (Massive -> Yahoo)")
    _, report = run_harvest_logic(test_tickers, target_date, symbol_map, logger)
    print(report[["Ticker", "Mode", "Status"]].to_string(index=False))

    print("\n🛡 TEST 2: Forced Fallback (Massive Outage -> Yahoo)")
    with patch("src.data.harvester.fetch_massive_data", return_value=(pd.DataFrame(), "Simulated Outage")):
        _, report = run_harvest_logic(test_tickers, target_date, symbol_map, logger)
        print(report[["Ticker", "Mode", "Status"]].to_string(index=False))

    print("\n🛡 TEST 3: Extreme Outage (Massive + Yahoo Down)")
    with patch("src.data.harvester.fetch_massive_data", return_value=(pd.DataFrame(), "Simulated")):
        with patch("src.data.harvester.fetch_yahoo_market_data", return_value=pd.DataFrame()):
            # For crypto, it might still fall back to Binance
            _, report = run_harvest_logic(test_tickers, target_date, symbol_map, logger)
            print(report[["Ticker", "Mode", "Status"]].to_string(index=False))

    print("\n✅ Resilience Check Complete. Check the 'Mode' column above to see the fallbacks in action.")

if __name__ == "__main__":
    run_user_test()
