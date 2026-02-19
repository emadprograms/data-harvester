"""
Database connection management for Turso (libSQL).
"""
from libsql_client import create_client_sync
import os

def get_db_connection():
    """Establishes a synchronous connection to the remote Turso database."""
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        
        url = mgr.get_secret("turso_arshademad_stockdataarchive_db_url")
        token = mgr.get_secret("turso_arshademad_stockdataarchive_auth_token")
        
        if not url or not token:
            print("❌ Missing Turso credentials. Check Infisical Access.")
            return None
        
        # Force HTTPS for reliability
        http_url = url.replace("libsql://", "https://")
        config = {"url": http_url, "auth_token": token}
        return create_client_sync(**config)
    except Exception as e:
        print(f"❌ Turso Connection Error: {e}")
        return None

def get_local_db_connection():
    """Establishes a synchronous connection to the local SQLite database."""
    try:
        local_db_path = "file:local_market_data.db"
        config = {"url": local_db_path}
        return create_client_sync(**config)
    except Exception as e:
        print(f"❌ Local DB Connection Error: {e}")
        return None
