"""
Database connection management for Turso (libSQL).
"""
from libsql_client import create_client_sync
import os

def get_archive_db_connection():
    """Establishes a synchronous connection to the Turso Stock Data Archive database."""
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        creds = mgr.get_turso_archive_creds()
        
        if not creds['url'] or not creds['token']:
            print("❌ Missing Turso Archive credentials.")
            return None
        
        http_url = creds['url'].replace("libsql://", "https://")
        return create_client_sync(url=http_url, auth_token=creds['token'])
    except Exception as e:
        print(f"❌ Turso Archive Connection Error: {e}")
        return None

def get_mirror_db_connection():
    """Establishes a synchronous connection to the Turso Stock Data Archive Mirror 1 database."""
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        creds = mgr.get_turso_mirror_creds()
        
        if not creds['url'] or not creds['token']:
            print("❌ Missing Turso Mirror credentials.")
            return None
        
        http_url = creds['url'].replace("libsql://", "https://")
        return create_client_sync(url=http_url, auth_token=creds['token'])
    except Exception as e:
        print(f"❌ Turso Mirror Connection Error: {e}")
        return None


def get_db_connection():
    """Backward-compatible default DB connection alias."""
    return get_archive_db_connection()


def get_local_db_connection():
    """Backward-compatible local DB alias.

    This project currently uses a remote mirror instead of a local sqlite file,
    so we return mirror connection for compatibility.
    """
    return get_mirror_db_connection()
