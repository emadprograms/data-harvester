"""
Databento Tick Backfiller for streaming.duckdb.
Iteratively backfills historical tick-by-tick TBBO (Trade and Top-of-Book Quotes)
one trading day at a time going backward from the most recent session.
Strictly targets single-stock symbols (excluding all ETFs, crypto, and commodities)
during the last 30 minutes of premarket (09:00-09:30 ET) and regular session (09:30-16:00 ET).
Tracks cost before every query to strictly respect the credit budget.
"""
import os
import time
import logging
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
from dotenv import load_dotenv

import databento as db
from src.database.connection import get_streaming_db_connection, get_historical_db_connection

logger = logging.getLogger("databento_backfill")
NY_TZ = ZoneInfo("America/New_York")

# Explicit exclusions: ETFs, Cryptos, Commodities, and FX
EXCLUDED_SYMBOLS = {
    # Broad market & Sector ETFs
    "SPY", "QQQ", "IWM", "DIA", "XLC", "XLF", "XLK", "XLE", "XLI", "XLP", "XLU", "XLV",
    "TLT", "SMH", "SOXX",
    # Crypto pairs
    "BTC", "ETH", "SOL", "BTCUSDT", "ETHUSDT", "EURUSDT", "PAXGUSDT",
    # Commodities, Rates & Volatility
    "CL=F", "GC=F", "VIX", "UUP"
}


def get_target_stock_symbols(historical_client=None) -> List[str]:
    """
    Retrieves tracked symbols from symbol_map, filtering strictly for single-stock equities.
    Filters out all ETFs, crypto pairs, commodities, and synthetic tickers.
    """
    own_client = False
    if historical_client is None:
        historical_client = get_historical_db_connection(read_only=True)
        own_client = True

    if not historical_client:
        return []

    try:
        rows = historical_client.execute(
            "SELECT display_name FROM symbol_map ORDER BY display_name"
        ).fetchall()
        stock_symbols = []
        for r in rows:
            sym = r[0].strip().upper()
            if (
                sym not in EXCLUDED_SYMBOLS
                and not sym.endswith("USDT")
                and "=" not in sym
                and "/" not in sym
            ):
                stock_symbols.append(sym)
        return stock_symbols
    finally:
        if own_client:
            historical_client.close()


def get_day_trading_bounds(trading_date: date) -> Tuple[datetime, datetime, datetime]:
    """
    Computes exact UTC start and end bounds for:
    - Last 30 minutes of premarket: 09:00:00 ET to 09:30:00 ET
    - Regular market hours: 09:30:00 ET to 16:00:00 ET
    Returns (start_utc, reg_open_utc, end_utc).
    """
    dt_pre_start = datetime(trading_date.year, trading_date.month, trading_date.day, 9, 0, 0, tzinfo=NY_TZ)
    dt_reg_open = datetime(trading_date.year, trading_date.month, trading_date.day, 9, 30, 0, tzinfo=NY_TZ)
    dt_reg_close = datetime(trading_date.year, trading_date.month, trading_date.day, 16, 0, 0, tzinfo=NY_TZ)

    return (
        dt_pre_start.astimezone(timezone.utc),
        dt_reg_open.astimezone(timezone.utc),
        dt_reg_close.astimezone(timezone.utc),
    )


def get_databento_client(api_key: Optional[str] = None) -> db.Historical:
    """Instantiates a Databento Historical client."""
    if not api_key:
        load_dotenv(".env")
        api_key = (
            os.getenv("DATABENTO_API_KEY")
            or os.getenv("DATABENTO_KEY")
            or os.getenv("DATABENTO_TOKEN")
        )
    if not api_key:
        raise ValueError("No Databento API key found in environment or .env file.")
    return db.Historical(api_key)


def estimate_day_cost(
    client: db.Historical,
    symbols: List[str],
    trading_date: date,
    schema: str = "tbbo",
    dataset: str = "DBEQ.BASIC"
) -> float:
    """
    Queries Databento metadata API to get the exact cost in USD for the day's requested slice.
    """
    start_utc, _, end_utc = get_day_trading_bounds(trading_date)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_utc.strftime("%Y-%m-%dT%H:%M:%S")

    cost = client.metadata.get_cost(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start_str,
        end=end_str
    )
    return float(cost)


def is_day_already_backfilled(trading_date: date, symbols: List[str], streaming_client=None) -> bool:
    """
    Checks if Databento tick data already exists in streaming.duckdb for the given date and symbols.
    """
    date_str = trading_date.strftime("%Y-%m-%d")
    for attempt in range(6):
        client = streaming_client or get_streaming_db_connection(read_only=True)
        if not client:
            time.sleep(0.3)
            continue
        try:
            row = client.execute(
                """
                SELECT COUNT(*) FROM ticks 
                WHERE source = 'DATABENTO' 
                  AND timestamp::DATE = ?::DATE
                """,
                [date_str]
            ).fetchone()
            count = row[0] if row else 0
            return count > 1000  # Substantial tick records already exist
        except Exception as e:
            if "Could not set lock" in str(e) or "Conflicting lock" in str(e):
                time.sleep(0.3 * (attempt + 1))
                continue
            return False
        finally:
            if streaming_client is None and client:
                client.close()
    return False


def fetch_and_normalize_day(
    client: db.Historical,
    symbols: List[str],
    trading_date: date,
    schema: str = "tbbo",
    dataset: str = "DBEQ.BASIC"
) -> pd.DataFrame:
    """
    Downloads tick TBBO data from Databento and maps it into the streaming.duckdb ticks schema:
    [timestamp, symbol, price, volume, bid, ask, source, session]
    """
    start_utc, reg_open_utc, end_utc = get_day_trading_bounds(trading_date)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_utc.strftime("%Y-%m-%dT%H:%M:%S")

    # Fetch from Databento timeseries
    data = client.timeseries.get_range(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start_str,
        end=end_str
    )

    df = data.to_df()
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "symbol", "price", "volume", "bid", "ask", "source", "session"])

    # Reset index if ts_event or ts_recv is in the index
    if "ts_event" not in df.columns:
        df = df.reset_index()

    # Determine timestamp column (prefer ts_event over ts_recv)
    ts_col = "ts_event" if "ts_event" in df.columns else "ts_recv"
    ts_series = pd.to_datetime(df[ts_col], utc=True)

    # Format timestamp string with microseconds for DuckDB
    timestamps = ts_series.dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    # Map prices: use trade execution price; if missing or 0, fallback to midpoint of bid/ask
    price = df["price"].astype(float)
    bid = df["bid_px_00"].astype(float) if "bid_px_00" in df.columns else pd.Series(None, index=df.index)
    ask = df["ask_px_00"].astype(float) if "ask_px_00" in df.columns else pd.Series(None, index=df.index)

    # Where trade price is zero or null, use midpoint
    midpoint = (bid + ask) / 2.0
    price = price.where(price > 0, midpoint)

    # Volume (trade size)
    volume = df["size"].astype(float).fillna(1.0) if "size" in df.columns else pd.Series(1.0, index=df.index)

    # Session classification: PRE vs REG
    session = ts_series.apply(lambda ts: "PRE" if ts < reg_open_utc else "REG")

    norm_df = pd.DataFrame({
        "timestamp": timestamps,
        "symbol": df["symbol"].astype(str).str.upper(),
        "price": price.round(4),
        "volume": volume.round(2),
        "bid": bid.round(4),
        "ask": ask.round(4),
        "source": "DATABENTO",
        "session": session
    })

    # Drop any records with completely null price
    norm_df = norm_df.dropna(subset=["price", "symbol", "timestamp"])
    return norm_df


def insert_ticks_to_streaming_db(df: pd.DataFrame, streaming_client=None) -> int:
    """
    Inserts normalized ticks DataFrame in bulk directly into streaming.duckdb ticks table.
    Retries gracefully if the live streaming daemon holds an intermittent lock.
    """
    if df.empty:
        return 0

    max_retries = 10
    for attempt in range(max_retries):
        client = streaming_client or get_streaming_db_connection()
        if not client:
            time.sleep(0.5 * (attempt + 1))
            continue

        try:
            # High-performance bulk registration & insertion
            client.conn.register("_batch_ticks_df", df)
            client.conn.execute("""
                INSERT INTO ticks (timestamp, symbol, price, volume, bid, ask, source, session)
                SELECT timestamp, symbol, price, volume, bid, ask, source, session
                FROM _batch_ticks_df
            """)
            client.conn.unregister("_batch_ticks_df")
            client.conn.commit()
            return len(df)
        except Exception as e:
            err_msg = str(e)
            if "Could not set lock" in err_msg or "Conflicting lock" in err_msg:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise
        finally:
            if streaming_client is None and client:
                client.close()

    raise RuntimeError("Failed to obtain write lock on streaming.duckdb after multiple attempts.")


def generate_candidate_trading_days(start_from_date: date, count: int = 60) -> List[date]:
    """Generates candidate weekday dates going backward from start_from_date."""
    days = []
    curr = start_from_date
    while len(days) < count:
        if curr.weekday() < 5:  # Monday through Friday
            days.append(curr)
        curr -= timedelta(days=1)
    return days


def run_databento_backfill(
    max_budget: float = 120.0,
    max_days: int = 40,
    start_date: Optional[date] = None,
    client: Optional[db.Historical] = None,
    progress_callback: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Orchestrates the backward day-by-day tick backfill loop:
    1. Targets single-stock symbols only.
    2. Starts from yesterday (most recent closed session) and goes backward.
    3. Checks cost before every day and stops if budget is reached.
    4. Ingests and stores into streaming.duckdb.
    """
    if client is None:
        client = get_databento_client()

    symbols = get_target_stock_symbols()
    if not symbols:
        return {"success": False, "error": "No eligible stock symbols found in symbol_map."}

    if start_date is None:
        # Default start from yesterday
        now_et = datetime.now(NY_TZ)
        start_date = (now_et - timedelta(days=1)).date()

    accumulated_cost = 0.0
    total_ticks_ingested = 0
    completed_days = []

    print(f"============================================================", flush=True)
    print(f"🚀 DATABENTO TICK BACKFILL INITIALIZED", flush=True)
    print(f"Target Symbols ({len(symbols)}): {', '.join(symbols)}", flush=True)
    print(f"Hours: 09:00 ET -> 16:00 ET (Last 30m Pre-Market + Regular)", flush=True)
    print(f"Credit Budget Cap: ${max_budget:.2f} USD", flush=True)
    print(f"Starting from: {start_date} going backward", flush=True)
    print(f"============================================================", flush=True)

    curr_day = start_date
    while len(completed_days) < max_days:
        if curr_day < date(2023, 3, 28):
            print("Reached earliest available data date in DBEQ.BASIC (2023-03-28). Stopping.", flush=True)
            break

        if curr_day.weekday() >= 5:  # Skip weekend
            curr_day -= timedelta(days=1)
            continue

        trading_day = curr_day
        curr_day -= timedelta(days=1)
        day_str = trading_day.strftime("%Y-%m-%d")

        # Check if already backfilled
        if is_day_already_backfilled(trading_day, symbols):
            print(f"⏩ [{day_str}] Already backfilled in streaming.duckdb. Skipping.", flush=True)
            continue

        # Estimate cost for this trading day
        try:
            day_cost = estimate_day_cost(client, symbols, trading_day, schema="tbbo")
        except Exception as e:
            print(f"⚠️ [{day_str}] Cost estimate failed (holiday or market closed): {e}. Skipping.", flush=True)
            continue

        if day_cost == 0.0:
            print(f"ℹ️ [{day_str}] Market holiday / zero billable size. Skipping.", flush=True)
            continue

        # Check budget limit
        if accumulated_cost + day_cost > max_budget:
            print(f"🛑 Budget limit reached! (Next day ${day_cost:.2f} would exceed budget of ${max_budget:.2f}). Stopping.", flush=True)
            break

        # Fetch and ingest
        print(f"📥 [{day_str}] Fetching TBBO ticks... (Estimated Cost: ${day_cost:.3f} USD)", flush=True)
        df_day = fetch_and_normalize_day(client, symbols, trading_day, schema="tbbo")

        if not df_day.empty:
            ticks_inserted = insert_ticks_to_streaming_db(df_day)
            accumulated_cost += day_cost
            total_ticks_ingested += ticks_inserted
            completed_days.append(day_str)
            rem_budget = max_budget - accumulated_cost
            print(f"✅ [{day_str}] Ingested {ticks_inserted:,} ticks. Spent so far: ${accumulated_cost:.3f} | Remaining budget: ${rem_budget:.3f}", flush=True)
        else:
            print(f"ℹ️ [{day_str}] No ticks returned.", flush=True)

        if progress_callback:
            progress_callback({
                "date": day_str,
                "day_cost": day_cost,
                "accumulated_cost": accumulated_cost,
                "ticks": len(df_day),
                "total_ticks": total_ticks_ingested
            })

    summary = {
        "success": True,
        "completed_days_count": len(completed_days),
        "completed_days": completed_days,
        "total_ticks_ingested": total_ticks_ingested,
        "total_cost_usd": round(accumulated_cost, 4),
        "remaining_budget_usd": round(max_budget - accumulated_cost, 4),
        "symbols_count": len(symbols),
        "symbols": symbols
    }

    print(f"\n============================================================")
    print(f"🎉 BACKFILL COMPLETED")
    print(f"Days Ingested: {len(completed_days)}")
    print(f"Total Ticks: {total_ticks_ingested:,}")
    print(f"Total Cost: ${accumulated_cost:.3f} USD")
    print(f"Remaining Budget: ${max_budget - accumulated_cost:.3f} USD")
    print(f"============================================================")
    return summary


if __name__ == "__main__":
    run_databento_backfill(max_budget=120.0, max_days=30)
