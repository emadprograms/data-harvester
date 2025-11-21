"""
Data fetching via Binance Public API.
Handles Crypto (BTCUSDT) and Forex Proxies (EURUSDT) directly.
"""
import requests
import pandas as pd
from datetime import datetime, timezone
from src.config import SCHEMA_COLS

def fetch_binance_daily(ticker: str, target_date_obj) -> pd.DataFrame:
    """
    Fetches full 24h 1-minute klines from Binance for a specific symbol.
    The ticker must be in Binance format (e.g., 'BTCUSDT', 'EURUSDT').
    """
    # 1. Use the Ticker Directly (No Mapping)
    # We just ensure it's uppercase to be safe.
    binance_symbol = ticker.upper().strip()

    # 2. Calculate Start/End Timestamps (UTC)
    # Binance API requires milliseconds
    start_dt = datetime.combine(target_date_obj, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_dt = datetime.combine(target_date_obj, datetime.max.time()).replace(tzinfo=timezone.utc)
    
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)
    
    url = "https://api.binance.com/api/v3/klines"
    all_klines = []
    current_start = start_ts
    
    try:
        while current_start < end_ts:
            params = {
                "symbol": binance_symbol,
                "interval": "1m",
                "startTime": current_start,
                "endTime": end_ts,
                "limit": 1000
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            # Check for API errors (e.g., Invalid Symbol)
            if isinstance(data, dict) and "code" in data:
                print(f"❌ Binance Error for {binance_symbol}: {data.get('msg')}")
                return pd.DataFrame()
                
            if not data or not isinstance(data, list):
                break
                
            all_klines.extend(data)
            
            # Update start time (Close time of last candle + 1ms)
            last_close_ts = data[-1][6]
            current_start = last_close_ts + 1
            
    except Exception as e:
        print(f"❌ Error fetching Binance data for {binance_symbol}: {e}")
        return pd.DataFrame()

    if not all_klines:
        return pd.DataFrame()

    # 3. Convert to DataFrame
    # Binance columns: Open time, Open, High, Low, Close, Volume, ...
    df = pd.DataFrame(all_klines, columns=[
        "timestamp", "open", "high", "low", "close", "volume", 
        "close_time", "q_vol", "trades", "buy_base", "buy_quote", "ignore"
    ])
    
    # 4. Normalize Types
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    
    # 5. Final Schema Cleanup
    # We save it exactly as the ticker provided (e.g. 'BTCUSDT')
    df["symbol"] = ticker 
    df["session"] = "REG" # Default label, Harvester will slice this later
    
    return df[SCHEMA_COLS]