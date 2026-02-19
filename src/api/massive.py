"""
Massive (formerly Polygon.io) API data fetching.
"""
import requests
import pandas as pd
from datetime import datetime
from src.infisical_manager import InfisicalManager
from src.config import UTC, BAHRAIN_TZ
from src.api.retry import get_retry_session

import threading
import time

# Key Rotation State (Module-level for CLI/Streamlit persistence)
_MASSIVE_KEY_IDX = 0
_KEY_LOCK = threading.Lock()

def fetch_massive_data(ticker: str, start_utc: datetime, end_utc: datetime, logger) -> tuple[pd.DataFrame, str]:
    """
    Fetches 1-minute aggregates from Massive (Polygon.io).
    Returns: (DataFrame, error_message)
    """
    global _MASSIVE_KEY_IDX
    mgr = InfisicalManager()
    keys = mgr.get_massive_api_keys()
    
    if not keys:
        msg = "Missing API Key"
        logger.log(f"   ❌ {msg}")
        return pd.DataFrame(), msg

    total_keys = len(keys)
    with _KEY_LOCK:
        start_idx = _MASSIVE_KEY_IDX % total_keys
    
    # Prepare params
    timespan = "minute"
    multiplier = 1
    from_str = start_utc.strftime("%Y-%m-%d")
    to_str = end_utc.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_str}/{to_str}"
    
    last_err = ""
    
    for i in range(total_keys):
        # Calculate current key to use
        current_idx = (start_idx + i) % total_keys
        api_key = keys[current_idx]
        
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": api_key
        }
        
        session = get_retry_session()
        try:
            response = session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            # --- SUCCESS ---
            # Update rotation to prefer next key next time
            with _KEY_LOCK:
                _MASSIVE_KEY_IDX = (current_idx + 1) % total_keys
            
            results = data.get("results", [])
            if not results:
                if data.get("status") == "OK":
                    return pd.DataFrame(), "No Data (OK)"
                return pd.DataFrame(), f"Empty: {data.get('status')}"
                
            df = pd.DataFrame(results)
            df.rename(columns={
                "t": "SnapshotTime", "o": "Open", "h": "High", 
                "l": "Low", "c": "Close", "v": "Volume"
            }, inplace=True)
            
            df["SnapshotTime"] = pd.to_datetime(df["SnapshotTime"], unit='ms').dt.tz_localize(UTC)
            mask = (df["SnapshotTime"] >= start_utc) & (df["SnapshotTime"] < end_utc)
            return df[mask].copy(), ""

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 429:
                logger.log(f"   ⚠️ Rate Limit (Key #{current_idx+1}). Switching...")
                last_err = "Rate Limit (429)"
                time.sleep(0.2) # Brief pause before retry
                continue # Try next key
            elif status == 401 or status == 403:
                logger.log(f"   ⚠️ Invalid Key #{current_idx+1}. Switching...")
                last_err = f"Auth Error (Key {current_idx+1})"
                continue
            else:
                last_err = f"HTTP {status}"
                logger.log(f"   ❌ Massive Error {ticker}: {last_err}")
                continue # Try next key instead of giving up
                
        except Exception as e:
            last_err = f"Error: {str(e)[:20]}..."
            logger.log(f"   ❌ Error fetching Massive data for {ticker}: {e}")
            return pd.DataFrame(), last_err

    # If we exited loop, all keys failed
    logger.log(f"   ❌ All {total_keys} keys exhausted/failed.")
    return pd.DataFrame(), f"Failed (All Keys): {last_err}"
