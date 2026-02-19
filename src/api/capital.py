"""
Capital.com API implementation for pre/post-market data.
"""
import requests
import pandas as pd
import threading
import time
from datetime import datetime
from src.infisical_manager import InfisicalManager
from src.config import SCHEMA_COLS, UTC
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

def fetch_capital_data(epic: str, start_utc: datetime, end_utc: datetime, logger) -> tuple[pd.DataFrame, str]:
    """
    Fetches 1-minute historical candles from Capital.com.
    Returns: (DataFrame, error_message)
    """
    session_data = _get_session()
    if not session_data:
        return pd.DataFrame(), "Session Failed"

    from datetime import timedelta
    session = get_retry_session()  # Reuse a single session for all chunks
    chunk_start = start_utc
    all_rows = []
    first_error = ""

    # CLAMP: Ensure we don't request future data (causes 400 Bad Request)
    now_utc = datetime.now(UTC)

    while chunk_start < end_utc:
        if chunk_start >= now_utc:
            break
            
        chunk_end = min(end_utc, chunk_start + timedelta(hours=12))
        if chunk_end > now_utc:
            chunk_end = now_utc
            
        # Capital.com historical prices endpoint
        # Format: yyyy-MM-dd'T'HH:mm:ss
        from_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S")
        to_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
        
        url = f"https://api-capital.backend-capital.com/api/v1/prices/{epic}"
        params = {
            "resolution": "MINUTE",
            "from": from_str,
            "to": to_str,
            "max": 1000 # Verified limit (1500 fails)
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
        # -------------------------------

        try:
            resp = session.get(url, params=params, headers=headers, timeout=15)
            if resp.status_code == 401:
                # Token expired mid-chunking. Clear cache, re-auth, retry this chunk ONCE.
                global _CAPITAL_SESSION
                with _SESSION_LOCK:
                    _CAPITAL_SESSION = None
                refreshed = _get_session()
                if refreshed:
                    headers = {
                        "X-CAP-API-KEY": refreshed["api_key"],
                        "CST": refreshed["CST"],
                        "X-SECURITY-TOKEN": refreshed["X-SECURITY-TOKEN"]
                    }
                    resp = session.get(url, params=params, headers=headers, timeout=15)
                    if resp.status_code != 200:
                        first_error = first_error or f"401 Retry Failed ({resp.status_code})"
                        break
                else:
                    first_error = first_error or "401 Re-auth Failed"
                    break

                
            resp.raise_for_status()
            data = resp.json()
            
            prices = data.get("prices", [])
            if prices:
                for p in prices:
                    # We use Mid prices if available, otherwise Bid
                    op = (p['openPrice']['bid'] + p['openPrice']['ask']) / 2 if 'bid' in p['openPrice'] else p['openPrice']
                    hp = (p['highPrice']['bid'] + p['highPrice']['ask']) / 2 if 'bid' in p['highPrice'] else p['highPrice']
                    lp = (p['lowPrice']['bid'] + p['lowPrice']['ask']) / 2 if 'bid' in p['lowPrice'] else p['lowPrice']
                    cp = (p['closePrice']['bid'] + p['closePrice']['ask']) / 2 if 'bid' in p['closePrice'] else p['closePrice']
                    
                    all_rows.append({
                        "timestamp": pd.to_datetime(p['snapshotTimeUTC'], utc=True),
                        "open": op,
                        "high": hp,
                        "low": lp,
                        "close": cp,
                        "volume": 0.0 # Capital volume is unreliable/broker-specific
                    })
        except Exception as e:
            first_error = first_error or str(e)
            # Continue to next chunk or fail? 
            # If one chunk fails, data is incomplete. But partial data is better than nothing?
            logger.log(f"⚠️ Error fetching Capital chunk {from_str}: {e}")
        
        chunk_start = chunk_end

    if not all_rows:
        return pd.DataFrame(), first_error or "No Data"

    df = pd.DataFrame(all_rows)
    # Filter to requested range (strict)
    mask = (df["timestamp"] >= start_utc) & (df["timestamp"] < end_utc)
    return df[mask].copy().sort_values("timestamp"), ""
