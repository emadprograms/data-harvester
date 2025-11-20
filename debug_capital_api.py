"""
Diagnostic script to probe Capital.com API behavior.
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
import sys

# Mock a logger
class MockLogger:
    def log(self, msg):
        print(f"LOG: {msg}")

logger = MockLogger()

# 1. Set up Environment (Credentials)
# The user will need to ensure these env vars are set, or we can mock the import
# assuming we are running in the repo root where src is available.

try:
    from src.api.capital import create_capital_session
    from src.config import CAPITAL_API_URL_BASE, UTC, BAHRAIN_TZ
    from src.api.retry import get_retry_session
except ImportError:
    print("❌ Error: Could not import project modules. Make sure you are in the repo root.")
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
    # We want to test a "Post-Market" window for "Yesterday" to ensure data exists.
    # Post-Market is 16:00 ET to 20:00 ET.
    # In UTC, that is 21:00 UTC to 01:00 UTC (+1 day).

    # Let's pick a safe date: Yesterday
    today = datetime.now(UTC).date()
    yesterday = today - timedelta(days=1)

    # 16:00 ET yesterday = 21:00 UTC yesterday
    start_utc = datetime(yesterday.year, yesterday.month, yesterday.day, 21, 0, 0, tzinfo=UTC)
    # 20:00 ET yesterday = 01:00 UTC today
    end_utc = start_utc + timedelta(hours=4)

    print(f"\n2. Test Parameters:")
    print(f"   Target Symbol: NVDA (Assuming it exists in your account)")
    print(f"   Window (UTC): {start_utc} -> {end_utc}")
    print(f"   Window (Bahrain): {start_utc.astimezone(BAHRAIN_TZ)} -> {end_utc.astimezone(BAHRAIN_TZ)}")

    epic = "NVDA"

    # 4. Make the Request (MANUAL Fetch to show URL)
    print(f"\n3. Executing Request (Reverted Logic - UTC Params)...")

    # Logic from reverted capital.py:
    # price_params = { ... 'from': start_utc.strftime(...), 'to': end_utc.strftime(...) }

    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_utc.strftime('%Y-%m-%dT%H:%M:%S')

    price_params = {
        "resolution": "MINUTE",
        "max": 10, # Limit to 10 rows to keep output clean
        'from': start_str,
        'to': end_str
    }

    session = get_retry_session()
    url = f"{CAPITAL_API_URL_BASE}/prices/{epic}"

    print(f"   URL: {url}")
    print(f"   Params: {price_params}")

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

            # Simulate Parsing Logic
            print("\n6. Parsed Data (Simulating src.api.capital logic):")
            extracted = [{'SnapshotTime': p.get('snapshotTime')} for p in prices]
            df = pd.DataFrame(extracted)
            df['SnapshotTime'] = pd.to_datetime(df['SnapshotTime'])

            # Apply Timezone Logic
            if df['SnapshotTime'].dt.tz is None:
                df['SnapshotTime'] = df['SnapshotTime'].dt.tz_localize(BAHRAIN_TZ)
                tz_note = "Localized to Bahrain (was naive)"
            else:
                df['SnapshotTime'] = df['SnapshotTime'].dt.tz_convert(BAHRAIN_TZ)
                tz_note = "Converted to Bahrain"

            df['UTC_Time'] = df['SnapshotTime'].dt.tz_convert(UTC)

            print(f"   Logic Applied: {tz_note}")
            print(df[['SnapshotTime', 'UTC_Time']].head(3).to_string())

            # Check if data matches the requested window
            first_ts = df['UTC_Time'].iloc[0]
            print(f"\n   First Timestamp (UTC): {first_ts}")
            if first_ts >= start_utc and first_ts < end_utc:
                print("   ✅ Data falls WITHIN the requested UTC window.")
            else:
                print("   ⚠️ Data falls OUTSIDE the requested UTC window (Timezone Mismatch?).")

        else:
            print("   ⚠️ No data returned for this window.")

    except Exception as e:
        print(f"   ❌ Request Failed: {e}")

if __name__ == "__main__":
    run_diagnostic()
