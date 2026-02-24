import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from src.database.connection import get_archive_db_connection
from src.config import US_EASTERN, UTC

def visualize_density(target_date_str, symbol):
    client = get_archive_db_connection()
    if not client: return

    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    prev_date = target_date - timedelta(days=1)
    
    start_et = US_EASTERN.localize(datetime.combine(prev_date, datetime.strptime("20:01:00", "%H:%M:%S").time()))
    end_et = US_EASTERN.localize(datetime.combine(target_date, datetime.strptime("20:00:00", "%H:%M:%S").time()))
    
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = end_et.astimezone(timezone.utc)
    
    res = client.execute(
        "SELECT timestamp FROM market_data WHERE symbol = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
        [symbol, start_utc.strftime('%Y-%m-%d %H:%M:%S'), end_utc.strftime('%Y-%m-%d %H:%M:%S')]
    )
    client.close()
    
    if not res.rows:
        print(f"\n📊 {symbol} Density: [░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░] (0 rows)")
        return

    found_ts = [pd.to_datetime(r[0]).replace(tzinfo=timezone.utc) for r in res.rows]
    num_slots = 48
    mins_per_slot = 1440 // num_slots
    
    bar = ""
    for i in range(num_slots):
        slot_start = start_utc + timedelta(minutes=i * mins_per_slot)
        slot_end = slot_start + timedelta(minutes=mins_per_slot)
        has_data = any(slot_start <= ts < slot_end for ts in found_ts)
        bar += "█" if has_data else "░"

    print(f"\n📊 {symbol} Density: {target_date_str} (8:01 PM ET -> 8:00 PM ET)")
    print(f"[{bar}] {len(found_ts)}/1440 mins")

if __name__ == "__main__":
    import sys
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-02-24"
    ticker = sys.argv[2] if len(sys.argv) > 2 else "AAPL"
    visualize_density(date, ticker)
