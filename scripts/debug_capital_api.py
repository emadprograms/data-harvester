"""
Diagnostic script to probe Capital.com API behavior.
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
import sys

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Mock a logger
class MockLogger:
    def log(self, msg):
        print(f"LOG: {msg}")

logger = MockLogger()

# 1. Set up Environment (Credentials)
try:
    from src.api.capital import create_capital_session
    from src.config import CAPITAL_API_URL_BASE, UTC, BAHRAIN_TZ
    from src.api.retry import get_retry_session
except ImportError as e:
    print(f"❌ Error: Could not import project modules: {e}")
    sys.exit(1)

def run_diagnostic():
    print("\n🔍 Starting Capital.com API Diagnostic...")
    print("--------------------------------------------")

    # 2. Authenticate
    print("1. Authenticating...")
    cst, xst = create_capital_session()
    if not cst:
        print("❌ Authentication Failed. Check your .streamlit/secrets.toml or Env Vars.")
        return

    print("✅ Authenticated successfully.")

    # 3. Define Test Window
    today = datetime.now(UTC).date()
    yesterday = today - timedelta(days=1)
    
    start_utc = datetime(yesterday.year, yesterday.month, yesterday.day, 21, 0, 0, tzinfo=UTC)
    end_utc = start_utc + timedelta(hours=4)

    print(f"\n2. Test Parameters:")
    print(f"   Target Symbol: NVDA")
    print(f"   Window (UTC): {start_utc} -> {end_utc}")

    epic = "NVDA"

    # 4. Make the Request
    print(f"\n3. Executing Request...")
    
    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_utc.strftime('%Y-%m-%dT%H:%M:%S')

    price_params = {
        "resolution": "MINUTE",
        "max": 10,
        'from': start_str,
        'to': end_str
    }

    session = get_retry_session()
    url = f"{CAPITAL_API_URL_BASE}/prices/{epic}"

    try:
        response = session.get(
            url,
            headers={'X-SECURITY-TOKEN': xst, 'CST': cst},
            params=price_params,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        prices = data.get('prices', [])

        print(f"\n4. Response Summary:")
        print(f"   HTTP Status: {response.status_code}")
        print(f"   Rows Returned: {len(prices)}")

        if prices:
            print("\n5. Raw Data Inspection (First 3 rows):")
            for p in prices[:3]:
                print(f"   SnapshotTime (Raw): {p.get('snapshotTime')}")

    except Exception as e:
        print(f"   ❌ Request Failed: {e}")

if __name__ == "__main__":
    run_diagnostic()
