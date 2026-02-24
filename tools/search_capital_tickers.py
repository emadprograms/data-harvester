"""
Surgical search for instrument epics on Capital.com
Usage: python3 tools/search_capital_tickers.py --query "DIA"
"""
import sys
import argparse
import requests
from src.infisical_manager import InfisicalManager

def search_capital(query):
    mgr = InfisicalManager()
    api_key = mgr.get_secret("capital_com_x_cap_api_key")
    identifier = mgr.get_secret("capital_com_identifier")
    password = mgr.get_secret("capital_com_password")
    
    # 1. Login to get session
    login_url = "https://api-capital.backend-capital.com/api/v1/session"
    headers = {"X-CAP-API-KEY": api_key}
    payload = {"identifier": identifier, "password": password}
    
    resp = requests.post(login_url, json=payload, headers=headers)
    if resp.status_code != 200:
        print(f"❌ Login failed: {resp.text}")
        return
    
    session_token = resp.headers.get("CST")
    security_token = resp.headers.get("X-SECURITY-TOKEN")
    
    auth_headers = {
        "X-CAP-API-KEY": api_key,
        "CST": session_token,
        "X-SECURITY-TOKEN": security_token
    }

    # Search endpoint using /markets
    print(f"🔍 Searching /markets for '{query}'...")
    url = f"https://api-capital.backend-capital.com/api/v1/markets"
    params = {"searchTerm": query}
    
    s_resp = requests.get(url, params=params, headers=auth_headers)
    if s_resp.status_code == 200:
        data = s_resp.json()
        markets = data.get("markets", [])
        print(f"\n✅ Found {len(markets)} results:")
        print(f"{'Epic':<25} | {'Symbol':<15} | {'Description':<40}")
        print("-" * 85)
        for m in markets:
            print(f"{str(m.get('epic')):<25} | {str(m.get('symbol')):<15} | {str(m.get('instrumentName')):<40}")
    else:
        print(f"❌ Search failed: {s_resp.status_code} - {s_resp.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    args = parser.parse_args()
    search_capital(args.query)
