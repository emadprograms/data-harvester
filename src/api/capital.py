"""
Capital.com API implementation for pre/post-market data.
"""
import requests
import pandas as pd
import threading
import time
from datetime import datetime, timedelta
from src.infisical_manager import InfisicalManager
from src.config import SCHEMA_COLS, UTC, US_EASTERN
from src.api.retry import get_retry_session

# Session Cache
_CAPITAL_SESSION = None
_SESSION_LOCK = threading.Lock()

# Rate Limiter
_RATE_LIMIT_LOCK = threading.Lock()
_LAST_REQUEST_TIME = 0.0

def _get_session():
    """Acquires/Refreshes a Capital.com session."""
    global _CAPITAL_SESSION
    with _SESSION_LOCK:
        if _CAPITAL_SESSION:
            return _CAPITAL_SESSION
        
        mgr = InfisicalManager()
        creds = mgr.get_capital_credentials()
        
        if not all(creds.values()):
            return None
        
        url = "https://api-capital.backend-capital.com/api/v1/session"
        headers = {
            'X-CAP-API-KEY': creds['api_key'],
            'Content-Type': 'application/json'
        }
        payload = {
            "identifier": creds['identifier'],
            "password": creds['password']
        }
        
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                _CAPITAL_SESSION = {
                    "CST": resp.headers.get('CST'),
                    "X-SECURITY-TOKEN": resp.headers.get('X-SECURITY-TOKEN'),
                    "api_key": creds['api_key']
                }
                return _CAPITAL_SESSION
        except Exception:
            pass
    return None

def fetch_capital_data(epic: str, start_dt: datetime, end_dt: datetime, logger) -> pd.DataFrame:
    """
    Fetches 1-minute historical candles from Capital.com within a UTC range.
    """
    session_data = _get_session()
    if not session_data:
        return pd.DataFrame()

    session = get_retry_session()
    
    # --- 16 HOUR LOOKBACK CLAMP ---
    # Capital.com only provides the last 16 hours of minute data.
    # We clip the start_dt to this limit to prevent the API from returning current data for old dates.
    now_utc = datetime.now(timezone.utc if start_dt.tzinfo else None)
    lookback_limit = now_utc - timedelta(hours=15, minutes=50) # 10m buffer for safety
    
    effective_start = max(start_dt, lookback_limit)
    
    if effective_start >= end_dt:
        logger.log(f"   ℹ️ {epic}: Requested range is beyond Capital.com 16h window. Skipping.")
        return pd.DataFrame()
        
    if effective_start > start_dt:
        logger.log(f"   ℹ️ {epic}: Clipping Capital.com request to 16h window (Start: {effective_start.strftime('%H:%M')} UTC)")

    chunk_start = effective_start
    all_rows = []

    while chunk_start < end_dt:
        if chunk_start >= now_utc:
            break
            
        chunk_end = min(end_dt, chunk_start + timedelta(hours=12))
        if chunk_end > now_utc:
            chunk_end = now_utc
            
        from_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S")
        to_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        
        url = f"https://api-capital.backend-capital.com/api/v1/prices/{epic}"
        params = {
            "resolution": "MINUTE",
            "from": from_str,
            "to": to_str,
            "max": 1000
        }
        
        headers = {
            "X-CAP-API-KEY": session_data["api_key"],
            "CST": session_data["CST"],
            "X-SECURITY-TOKEN": session_data["X-SECURITY-TOKEN"]
        }
        
        # --- RATE LIMITER (1 Req/Sec) ---
        global _LAST_REQUEST_TIME
        with _RATE_LIMIT_LOCK:
            now = time.time()
            since = now - _LAST_REQUEST_TIME
            if since < 1.1:
                time.sleep(1.1 - since)
            _LAST_REQUEST_TIME = time.time()

        try:
            resp = session.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 401:
                # Token expired mid-chunking
                global _CAPITAL_SESSION
                with _SESSION_LOCK:
                    _CAPITAL_SESSION = None
                session_data = _get_session()
                if not session_data: break
                continue # Retry same chunk with new session
                
            resp.raise_for_status()
            data = resp.json()
            
            prices = data.get("prices", [])
            if prices:
                for p in prices:
                    # Use Mid prices (Avg of Bid/Ask)
                    op = (p['openPrice']['bid'] + p['openPrice']['ask']) / 2
                    hp = (p['highPrice']['bid'] + p['highPrice']['ask']) / 2
                    lp = (p['lowPrice']['bid'] + p['lowPrice']['ask']) / 2
                    cp = (p['closePrice']['bid'] + p['closePrice']['ask']) / 2
                    
                    all_rows.append({
                        "timestamp": pd.to_datetime(p['snapshotTimeUTC'], utc=True),
                        "open": op,
                        "high": hp,
                        "low": lp,
                        "close": cp,
                        "volume": 0.0,
                        "symbol": epic
                    })
        except Exception as e:
            logger.log(f"   ⚠️ Capital Error for {epic} at {from_str}: {e}")
        
        chunk_start = chunk_end

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    # Return as UTC aware (standard)
    return df.sort_values("timestamp")
