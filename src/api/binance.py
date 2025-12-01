"""
Data fetching via Binance Public API.
Handles Crypto (BTCUSDT) and Forex Proxies (EURUSDT) directly.
"""
import requests
import pandas as pd
from datetime import datetime, timezone
from src.config import SCHEMA_COLS, BINANCE_DOMAINS

def fetch_binance_daily(ticker: str, target_date_obj) -> pd.DataFrame:
    """
    Fetches full 24h 1-minute klines from Binance for a specific symbol.
    The ticker must be in Binance format (e.g., 'BTCUSDT', 'EURUSDT').
    Attempts to fetch from api.binance.com first, then api.binance.us (fallback).
    """
    # 1. Use the Ticker Directly (No Mapping)
    binance_symbol = ticker.upper().strip()

    # 2. Calculate Start/End Timestamps (UTC)
    start_dt = datetime.combine(target_date_obj, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(target_date_obj, datetime.max.time()).replace(tzinfo=timezone.utc)
    
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    
    # Domain fallback strategy
    domains = BINANCE_DOMAINS
    
    for domain in domains:
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
                
                # Handle Geo-Blocking / IP Bans
                if response.status_code in [403, 451]:
                    print(f"⚠️ {domain} restricted ({response.status_code}). Switching domain...")
                    break # Try next domain
                
                if response.status_code != 200:
                    print(f"❌ Error {response.status_code} from {domain}: {response.text}")
                    break

                data = response.json()
                
                # Check for API errors (e.g., Invalid Symbol)
                if isinstance(data, dict) and "code" in data:
                    print(f"❌ Binance Error for {binance_symbol} on {domain}: {data.get('msg')}")
                    break
                    
                if not data or not isinstance(data, list):
                    success = True
                    break
                    
                all_klines.extend(data)
                
                # Update start time (Close time of last candle + 1ms)
                last_close_ts = data[-1][6]
                current_start = last_close_ts + 1
                success = True
            
            if success and all_klines:
                # 3. Convert to DataFrame
                df = pd.DataFrame(all_klines, columns=[
                    "timestamp", "open", "high", "low", "close", "volume", 
                    "close_time", "q_vol", "trades", "buy_base", "buy_quote", "ignore"
                ])
                
                # 4. Normalize Types
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                numeric_cols = ["open", "high", "low", "close", "volume"]
                df[numeric_cols] = df[numeric_cols].astype(float)
                
                # 5. Final Schema Cleanup
                df["symbol"] = ticker 
                df["session"] = "REG" # Default label, Harvester will slice this later
                
                return df[SCHEMA_COLS]

        except Exception as e:
            print(f"❌ Exception fetching Binance data for {binance_symbol} on {domain}: {e}")
            continue

    return pd.DataFrame()