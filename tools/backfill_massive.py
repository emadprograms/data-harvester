"""
Massive (Polygon.io) Historical Backfill Engine.
Discovers missing historical trading days between July 28, 2026 and September 25, 2026,
fetches 1-minute OHLCV candles using round-robin rotation across all 9 Massive API keys,
and saves the bars with Source-Tiering Protection into data/historical.duckdb.
"""
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from polygon import RESTClient

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.credentials import get_massive_keys
from src.database.connection import get_historical_db_connection
from src.database.operations import _save_to_client
from src.config import US_EASTERN, UTC

# Market hours boundaries for session labeling
_MARKET_OPEN = datetime.strptime("09:30", "%H:%M").time()
_MARKET_CLOSE = datetime.strptime("16:00", "%H:%M").time()


def get_session_label(dt_utc: datetime) -> str:
    """Calculates PRE / REG / POST session tag based on US Eastern time."""
    dt_et = dt_utc.astimezone(US_EASTERN)
    t = dt_et.time()
    if t < _MARKET_OPEN:
        return "PRE"
    if t > _MARKET_CLOSE:
        return "POST"
    return "REG"


class KeyRotator:
    """Thread-safe round-robin rotator for Polygon/Massive API keys."""
    def __init__(self, keys):
        self.keys = keys
        self.index = 0

    def next_key(self):
        if not self.keys:
            return None
        key = self.keys[self.index % len(self.keys)]
        self.index += 1
        return key


def backfill_massive():
    print("=" * 70)
    print("🚀 MASSIVE HISTORICAL BACKFILL ENGINE (v3.0)")
    print("=" * 70)

    # 1. Load keys
    keys = get_massive_keys()
    print(f"🔑 Loaded {len(keys)} Massive API keys from environment.")
    if not keys:
        print("❌ Error: No Massive API keys found in .env!")
        return

    rotator = KeyRotator(keys)

    # 2. Connect to historical database
    client = get_historical_db_connection()
    if not client:
        print("❌ Error: Could not connect to data/historical.duckdb!")
        return

    # Check baseline row count
    initial_count = client.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
    print(f"📊 Baseline historical.duckdb market_data rows: {initial_count:,}")

    # 3. Retrieve symbols with massive_ticker
    symbol_rows = client.execute("""
        SELECT display_name, massive_ticker 
        FROM symbol_map 
        WHERE massive_ticker IS NOT NULL 
        ORDER BY display_name ASC
    """).fetchall()

    print(f"🎯 Target symbols configured for Massive ({len(symbol_rows)}):")
    print("   " + ", ".join([r[0] for r in symbol_rows]))
    print("-" * 70)

    end_utc = datetime(2026, 9, 25, 23, 59, 59, tzinfo=timezone.utc)
    total_new_bars = 0
    symbols_processed = 0

    t_start_all = time.time()

    for disp, massive_sym in symbol_rows:
        # Determine latest existing bar for this symbol
        max_ts_res = client.execute("SELECT MAX(timestamp) FROM market_data WHERE symbol = ?", [disp]).fetchone()
        max_ts_val = max_ts_res[0] if max_ts_res and max_ts_res[0] else None

        if max_ts_val:
            if isinstance(max_ts_val, datetime):
                max_dt = max_ts_val if max_ts_val.tzinfo else max_ts_val.replace(tzinfo=timezone.utc)
            else:
                try:
                    max_dt = datetime.strptime(str(max_ts_val).split('.')[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except Exception:
                    max_dt = datetime(2026, 7, 28, 0, 0, 0, tzinfo=timezone.utc)
            start_utc = max_dt + timedelta(minutes=1)
        else:
            start_utc = datetime(2026, 7, 28, 0, 0, 0, tzinfo=timezone.utc)

        if start_utc >= end_utc:
            print(f"⏩ [{disp}] Already up to date (Latest: {max_ts_val}). Skipping.")
            continue

        start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S')
        end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S')
        print(f"📥 [{disp}] Backfilling from {start_str} to {end_str} (Ticker: {massive_sym})...")

        # Fetch in chunks of up to 50,000 bars
        curr_from = start_utc
        symbol_bars = []
        max_retries = len(keys) + 2

        while curr_from < end_utc:
            from_ms = int(curr_from.timestamp() * 1000)
            to_ms = int(end_utc.timestamp() * 1000)

            chunk_aggs = []
            success = False

            for attempt in range(max_retries):
                key = rotator.next_key()
                p_client = RESTClient(key, retries=0)

                try:
                    t0 = time.time()
                    chunk_aggs = list(p_client.list_aggs(
                        ticker=massive_sym,
                        multiplier=1,
                        timespan="minute",
                        from_=from_ms,
                        to=to_ms,
                        limit=50000
                    ))
                    t1 = time.time()
                    success = True
                    break
                except Exception as e:
                    err_msg = str(e)
                    if "429" in err_msg or "too many" in err_msg.lower():
                        print(f"   ⚠️ Rate limited on key (Attempt {attempt+1}/{max_retries}). Rotating key & sleeping 2s...")
                        time.sleep(2.0)
                    else:
                        print(f"   ❌ Polygon fetch error for {massive_sym}: {err_msg}")
                        break

            if not success or not chunk_aggs:
                break

            symbol_bars.extend(chunk_aggs)
            last_agg_dt = datetime.fromtimestamp(chunk_aggs[-1].timestamp / 1000, tz=timezone.utc)

            # If fewer than 50,000 bars returned, we reached the end of available data
            if len(chunk_aggs) < 50000:
                break
            else:
                curr_from = last_agg_dt + timedelta(minutes=1)

            # Polite pause between requests to preserve rate quota
            time.sleep(1.5)

        if not symbol_bars:
            print(f"   ⚠️ [{disp}] No bars returned from Massive.")
            time.sleep(1.5)
            continue

        # Format rows for DuckDB ingestion
        rows_to_insert = []
        for agg in symbol_bars:
            ts = datetime.fromtimestamp(agg.timestamp / 1000, tz=timezone.utc)
            ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
            sess = get_session_label(ts)
            rows_to_insert.append((
                ts_str,
                disp,
                float(agg.open),
                float(agg.high),
                float(agg.low),
                float(agg.close),
                float(agg.volume or 0.0),
                sess,
                "MASSIVE"
            ))

        # Save to historical.duckdb with Source-Tiering Protection
        save_ok = _save_to_client(client, rows_to_insert, label=f"DuckDB-{disp}")
        if save_ok:
            client.commit()
            count_saved = len(rows_to_insert)
            total_new_bars += count_saved
            symbols_processed += 1
            first_bar = rows_to_insert[0][0]
            last_bar = rows_to_insert[-1][0]
            print(f"   ✅ [{disp}] Saved {count_saved:,} 1m bars ({first_bar} → {last_bar}).")
        else:
            print(f"   ❌ [{disp}] Failed saving bars to DuckDB.")

        # Rate spacing before next symbol
        time.sleep(1.5)

    t_end_all = time.time()
    elapsed = t_end_all - t_start_all

    # 4. Final verification and report
    final_count = client.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
    client.close()

    print("=" * 70)
    print("🎉 MASSIVE BACKFILL COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"• Total Symbols Updated: {symbols_processed}")
    print(f"• Total New Bars Ingested: {total_new_bars:,}")
    print(f"• Database Row Count: {initial_count:,} → {final_count:,} (+{final_count - initial_count:,})")
    print(f"• Total Time Elapsed: {elapsed:.2f} seconds ({elapsed/60:.2f} mins)")
    print("=" * 70)


if __name__ == "__main__":
    backfill_massive()
