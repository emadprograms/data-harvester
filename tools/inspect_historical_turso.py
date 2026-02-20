from libsql_client import create_client_sync
from src.infisical_manager import InfisicalManager
import os

def inspect_turso_tables():
    mgr = InfisicalManager()
    url = mgr.get_secret("turso_emadprograms_analystworkbench_db_url")
    token = mgr.get_secret("turso_emadprograms_analystworkbench_auth_token")
    
    if not url or not token:
        print("❌ Missing credentials for analyst_workbench")
        return

    # Force HTTPS
    http_url = url.replace("libsql://", "https://")
    config = {"url": http_url, "auth_token": token}
    client = create_client_sync(**config)
    
    print(f"--- Schema for market_data ---")
    schema_res = client.execute("PRAGMA table_info(market_data)")
    for row in schema_res.rows:
        print(f"Column: {row[1]} ({row[2]})")
        
    print(f"--- Sample rows ---")
    sample_res = client.execute("SELECT * FROM market_data LIMIT 3")
    for row in sample_res.rows:
        print(row)
        
if __name__ == "__main__":
    inspect_turso_tables()
