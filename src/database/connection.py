"""
Database connection management for Turso (libSQL).
"""
from libsql_client import create_client_sync

def _sanitize_url(url: str) -> str:
    """Converts libsql:// to https:// for the synchronous client."""
    if not url:
        return ""
    return url.replace("libsql://", "https://")

def get_archive_db_connection():
    """Establishes a synchronous connection to the Turso Stock Data Archive database."""
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        creds = mgr.get_turso_archive_creds()
        
        url = _sanitize_url(creds.get('url'))
        token = creds.get('token')
        
        if not url or not token:
            print("❌ Missing Turso Archive credentials.")
            return None
        
        return create_client_sync(url=url, auth_token=token)
    except Exception as e:
        print(f"❌ Turso Archive Connection Error: {e}")
        return None

def get_mirror_db_connection():
    """Establishes a synchronous connection to the Turso Stock Data Archive Mirror 1 database."""
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        creds = mgr.get_turso_mirror_creds()
        
        url = _sanitize_url(creds.get('url'))
        token = creds.get('token')
        
        if not url or not token:
            print("❌ Missing Turso Mirror credentials.")
            return None
        
        return create_client_sync(url=url, auth_token=token)
    except Exception as e:
        print(f"❌ Turso Mirror Connection Error: {e}")
        return None
