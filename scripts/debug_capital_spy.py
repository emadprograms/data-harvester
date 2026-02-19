
import requests
import json
from datetime import datetime, timedelta
from src.infisical_manager import InfisicalManager
from src.config import UTC

# Session Setup
mgr = InfisicalManager()
creds = mgr.get_capital_credentials()
if not all(creds.values()):
    print("Missing credentials")
    exit(1)

session_url = "https://api-capital.backend-capital.com/api/v1/session"
headers = {
    'X-CAP-API-KEY': creds['api_key'],
    'Content-Type': 'application/json'
}
payload = {
    "identifier": creds['identifier'],
    "password": creds['password']
}
resp = requests.post(session_url, headers=headers, json=payload)
if resp.status_code != 200:
    print(f"Session Error: {resp.text}")
    exit(1)

cst = resp.headers.get('CST')
x_security_token = resp.headers.get('X-SECURITY-TOKEN')

# Fetch SPY
epic = "SPY"
start_utc = datetime(2026, 2, 19, 5, 0, 0) # 00:00 EST
end_utc = datetime(2026, 2, 19, 17, 0, 0) # 12h chunk (Valid)

url = f"https://api-capital.backend-capital.com/api/v1/prices/{epic}"

headers = {
    "X-CAP-API-KEY": creds['api_key'],
    "CST": cst,
    "X-SECURITY-TOKEN": x_security_token
}

params = {
    "resolution": "MINUTE",
    "from": start_utc.strftime("%Y-%m-%dT%H:%M:%S"),
    "to": end_utc.strftime("%Y-%m-%dT%H:%M:%S"),
    "max": 1500 # Testing max=1500
}

print(f"Requesting {url} with params {params}")
resp = requests.get(url, params=params, headers=headers)

if resp.status_code == 200:
    data = resp.json()
    prices = data.get('prices', [])
    print(f"Response Status: {resp.status_code}")
    print(f"Total Prices Returned: {len(prices)}")
    if prices:
        print("First Candle:", json.dumps(prices[0], indent=2))
        print("Last Candle:", json.dumps(prices[-1], indent=2))
        
        # Check intervals
        # Print timestamps of first 5
        print("First 5 Timestamps:")
        for p in prices[:5]:
            print(p['snapshotTimeUTC'])
else:
    print(f"Error: {resp.status_code} - {resp.text}")
