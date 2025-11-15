"""
Database connection management for Turso (libSQL).
"""
import streamlit as st
from libsql_client import create_client_sync
import os


@st.cache_resource
def get_db_connection():
    """Establishes a synchronous connection to the Turso database."""
    try:
        # Priority: Streamlit Secrets (Local) -> Env Vars (GitHub/Worker)
        if "turso" in st.secrets:
            url = st.secrets["turso"]["db_url"]
            token = st.secrets["turso"]["auth_token"]
        else:
            url = os.environ.get("TURSO_DB_URL")
            token = os.environ.get("TURSO_AUTH_TOKEN")
        
        if not url or not token:
            if st.runtime.exists():
                st.error("Missing Turso credentials. Check secrets.toml or Environment Variables.")
            return None
        
        # Force HTTPS for reliability
        http_url = url.replace("libsql://", "https://")
        config = {"url": http_url, "auth_token": token}
        return create_client_sync(**config)
    except Exception as e:
        if st.runtime.exists():
            st.error(f"Failed to create Turso client: {e}")
        else:
            print(f"DB Connection Error: {e}")
        return None
