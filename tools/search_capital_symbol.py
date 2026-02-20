
import requests
import os
import sys
from src.api.capital import _get_session

def search_symbol(search_term):
    session_data = _get_session()
    if not session_data:
        print("Failed to acquire Capital.com session.")
        return

    url = "https://api-capital.backend-capital.com/api/v1/markets"
    params = {"searchTerm": search_term}
    headers = {
        "X-CAP-API-KEY": session_data["api_key"],
        "CST": session_data["CST"],
        "X-SECURITY-TOKEN": session_data["X-SECURITY-TOKEN"]
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        if not markets:
            print(f"No markets found for '{search_term}'.")
            return

        print(f"Search results for '{search_term}':")
        for m in markets:
            print(f"  Epic: {m.get('epic')}, Name: {m.get('instrumentName')}, Symbol: {m.get('symbol')}")
    except Exception as e:
        print(f"Error searching symbol: {e}")

if __name__ == "__main__":
    term = sys.argv[1] if len(sys.argv) > 1 else "EURUSD"
    search_symbol(term)
