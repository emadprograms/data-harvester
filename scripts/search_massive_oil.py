import requests
from src.infisical_manager import InfisicalManager

def search_oil():
    print("🔍 Searching Massive (Polygon) for Oil/WTI Tickers...")
    mgr = InfisicalManager()
    keys = mgr.get_massive_api_keys()
    if not keys: return
    api_key = keys[0]

    # Search Queries
    queries = ["OIL", "WTI", "XTI", "BRENT"]
    
    for q in queries:
        print(f"\n--- Results for '{q}' ---")
        url = f"https://api.polygon.io/v3/reference/tickers?search={q}&active=true&limit=10&apiKey={api_key}"
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if "results" in data:
                for item in data["results"]:
                    # Filter for Currencies/Indices which are likely Spot/CFD
                    if item.get("market") in ["fx", "indices", "currencies"]: 
                        print(f"   [{item.get('market').upper()}] {item.get('ticker')} - {item.get('name')}")
                    # Also print stocks just in case valid ETF
                    elif item.get("market") == "stocks":
                         pass # Skip stocks to reduce noise (USO etc)
            else:
                print("   No results.")
        except Exception as e:
            print(f"Error: {e}")

    # Check Specific Forex Candidates directly
    candidates = ["C:XTIUSD", "C:WTIUSD", "C:BCOUSD", "I:WTI"]
    print(f"\n--- Checking Specific Candidates ---")
    for c in candidates:
        url = f"https://api.polygon.io/v3/reference/tickers/{c}?apiKey={api_key}"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                d = resp.json().get("results", {})
                print(f"✅ FOUND: {d.get('ticker')} - {d.get('name')}")
            else:
                print(f"❌ {c} not found ({resp.status_code})")
        except:
            pass

if __name__ == "__main__":
    search_oil()
