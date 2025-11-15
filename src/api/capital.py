"""
Capital.com API session management and data fetching.
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
from src.config import CAPITAL_API_URL_BASE, BAHRAIN_TZ, UTC
from src.api.retry import get_retry_session


@st.cache_resource(ttl=600)
def create_capital_session():
    """Creates a Capital.com session and caches tokens."""
    # Priority: Streamlit Secrets (Local) -> Env Vars (GitHub/Worker)
    if "capital_com" in st.secrets:
        api_key = st.secrets["capital_com"]["X_CAP_API_KEY"]
        identifier = st.secrets["capital_com"]["identifier"]
        password = st.secrets["capital_com"]["password"]
    else:
        api_key = os.environ.get("CAPITAL_X_CAP_API_KEY")
        identifier = os.environ.get("CAPITAL_IDENTIFIER")
        password = os.environ.get("CAPITAL_PASSWORD")
    
    if not api_key or not identifier or not password:
        return None, None

    session = get_retry_session()
    try:
        response = session.post(
            f"{CAPITAL_API_URL_BASE}/session", 
            headers={'X-CAP-API-KEY': api_key, 'Content-Type': 'application/json'}, 
            json={"identifier": identifier, "password": password}, 
            timeout=15
        )
        response.raise_for_status()
        return response.headers.get('CST'), response.headers.get('X-SECURITY-TOKEN')
    except Exception:
        return None, None


def fetch_capital_data_range(epic: str, cst: str, xst: str, start_utc, end_utc, logger) -> pd.DataFrame:
    """Fetches 1-min Capital.com data for a specific epic and UTC time window."""
    now_utc = datetime.now(UTC)
    limit_16h_ago = now_utc - timedelta(hours=16)
    
    if start_utc < limit_16h_ago:
        logger.log(f"   ⚠️ Start time clamped to 16h limit.")
        start_utc = limit_16h_ago + timedelta(minutes=1)
        
    if start_utc >= end_utc:
        return pd.DataFrame()
    if end_utc > now_utc:
        end_utc = now_utc
    
    price_params = {
        "resolution": "MINUTE", "max": 1000, 
        'from': start_utc.strftime('%Y-%m-%dT%H:%M:%S'), 
        'to': end_utc.strftime('%Y-%m-%dT%H:%M:%S')
    }
    session = get_retry_session()
    try:
        response = session.get(
            f"{CAPITAL_API_URL_BASE}/prices/{epic}", 
            headers={'X-SECURITY-TOKEN': xst, 'CST': cst}, 
            params=price_params, 
            timeout=15
        )
        response.raise_for_status()
        prices = response.json().get('prices', [])
        if not prices:
            return pd.DataFrame()
        
        extracted = [
            {
                'SnapshotTime': p.get('snapshotTime'), 
                'Open': p.get('openPrice', {}).get('bid'), 
                'High': p.get('highPrice', {}).get('bid'), 
                'Low': p.get('lowPrice', {}).get('bid'), 
                'Close': p.get('closePrice', {}).get('bid'), 
                'Volume': p.get('lastTradedVolume')
            } for p in prices
        ]
        df = pd.DataFrame(extracted)
        
        df['SnapshotTime'] = pd.to_datetime(df['SnapshotTime'])
        if df['SnapshotTime'].dt.tz is None:
            df['SnapshotTime'] = df['SnapshotTime'].dt.tz_localize(BAHRAIN_TZ)
        else:
            df['SnapshotTime'] = df['SnapshotTime'].dt.tz_convert(BAHRAIN_TZ)
        df['SnapshotTime'] = df['SnapshotTime'].dt.tz_convert(UTC)
        return df
    except Exception as e:
        logger.log(f"   ❌ Error fetching Capital data for {epic}: {e}")
        return pd.DataFrame()
