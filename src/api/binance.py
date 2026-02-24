"""
Data fetching via Binance Public API.
Handles Crypto (BTCUSDT) and Forex Proxies (EURUSDT) directly.
"""
import requests
import pandas as pd
from datetime import datetime, timezone
from src.config import SCHEMA_COLS, BINANCE_DOMAINS

# Global cache for the first successful endpoint to prevent repetitive 451 geo-block testing
WORKING_BINANCE_DOMAIN = None

def fetch_binance_range(ticker: str, start_dt: datetime, end_dt: datetime, logger=None) -> pd.DataFrame:
    """
    Fetches 1-minute klines from Binance within a specific UTC datetime range.
    Uses a Smart Cache strategy to remember the working domain and silence 451 warnings.
    """
    global WORKING_BINANCE_DOMAIN
    binance_symbol = ticker.upper().strip()

    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    
    # Priority: Memory -> Configured List
    domains_to_try = []
    if WORKING_BINANCE_DOMAIN:
        domains_to_try.append(WORKING_BINANCE_DOMAIN)
        for d in BINANCE_DOMAINS:
            if d != WORKING_BINANCE_DOMAIN:
                domains_to_try.append(d)
    else:
        domains_to_try = BINANCE_DOMAINS
    
    warnings_collected = []
    
    for domain in domains_to_try:
        url = f"{domain}/api/v3/klines"
        all_klines = []
        current_start = start_ts
        success = False
        
        try:
            while current_start < end_ts:
                params = {
                    "symbol": binance_symbol,
                    "interval": "1m",
                    "startTime": current_start,
                    "endTime": end_ts,
                    "limit": 1000
                }
                
                response = requests.get(url, params=params, timeout=5)
                
                # Handle Geo-Blocking / IP Bans Silently
                if response.status_code in [403, 451]:
                    warnings_collected.append(f"{domain} (451 Restricted)")
                    break # Try next domain
                
                if response.status_code != 200:
                    warnings_collected.append(f"{domain} ({response.status_code} Error)")
                    break

                data = response.json()
                
                # Check for API errors (e.g., Invalid Symbol)
                if isinstance(data, dict) and "code" in data:
                    warnings_collected.append(f"{domain} (API Error: {data.get('msg')})")
                    break
                    
                if not data or not isinstance(data, list):
                    success = True
                    break
                    
                all_klines.extend(data)
                
                # Update start time (Close time of last candle + 1ms)
                last_close_ts = data[-1][6]
                current_start = last_close_ts + 1
                success = True
            
            if success:
                # Cache the domain that actually worked
                if WORKING_BINANCE_DOMAIN != domain:
                    WORKING_BINANCE_DOMAIN = domain
                    if logger: logger.log(f"   ✅ Binance Memory updated: Using {domain} moving forward.")
                
                if all_klines:
                    # Convert to DataFrame
                    df = pd.DataFrame(all_klines, columns=[
                        "timestamp", "open", "high", "low", "close", "volume", 
                        "close_time", "q_vol", "trades", "buy_base", "buy_quote", "ignore"
                    ])
                    
                    # Normalize Types
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                    numeric_cols = ["open", "high", "low", "close", "volume"]
                    df[numeric_cols] = df[numeric_cols].astype(float)
                    
                    # Final Schema Cleanup
                    df["symbol"] = ticker 
                    df["session"] = "REG" 
                    
                    return df[SCHEMA_COLS]
                else:
                    return pd.DataFrame()

        except Exception as e:
            warnings_collected.append(f"{domain} (Exception: {e})")
            continue

    # If we get here, ALL domains failed. Now we log the warnings.
    msg = f"❌ All Binance domains failed for {binance_symbol}. Reasons: {', '. join(warnings_collected)}"
    if logger: logger.log(f"   {msg}")
    else: print(msg)
    
    return pd.DataFrame()

# Compatibility alias for tests and legacy logic
fetch_binance_daily = fetch_binance_range