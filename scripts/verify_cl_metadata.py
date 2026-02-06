import requests
from src.infisical_manager import InfisicalManager

def verify_cl_identity():
    mgr = InfisicalManager()
    keys = mgr.get_massive_api_keys()
    if not keys: 
        print("No keys")
        return

    api_key = keys[0]
    # Fetch Ticker Details v3
    url = f"https://api.polygon.io/v3/reference/tickers/CL?apiKey={api_key}"
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
        print(f"Status: {resp.status_code}")
        
        if "results" in data:
            res = data["results"]
            print(f"Ticker: {res.get('ticker')}")
            print(f"Name: {res.get('name')}")
            print(f"Market: {res.get('market')}")
            print(f"Type: {res.get('type')}")
        else:
            print(f"No results found. Status: {data.get('status')}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_cl_identity()
