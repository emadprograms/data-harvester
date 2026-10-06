"""
Databento tick gap-filler for the Parquet tick lake.

Fills one named day at a time, going backward from the most recent session.
A day is not downloaded whole. Databento tbbo is requested only for stretches
where every active symbol is silent, inside 04:00-20:00 ET. Returned bid and
ask are appended as schema v2. Existing files are not rewritten. The publisher
lock is checked before any Databento call. Cost is still checked before each
day so the credit budget is respected.
"""
import hashlib
import os
from pathlib import Path
import time
import logging
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional, Dict, Any
import pandas as pd
from dotenv import load_dotenv

try:
    import databento as db
except ModuleNotFoundError as exc:  # Optional for offline utilities and injected-client tests.
    if exc.name != "databento":
        raise
    db = None
from src.config import APPROVED_EQUITY_SYMBOLS

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


def _is_approved_equity(symbol: str) -> bool:
    """Single-stock equities only: no ETFs, crypto pairs or futures."""
    return (
        symbol in APPROVED_EQUITY_SYMBOLS
        and symbol not in EXCLUDED_SYMBOLS
        and not symbol.endswith("USDT")
        and "=" not in symbol
        and "/" not in symbol
    )


def get_target_stock_symbols(lake_root: Optional[Any] = None) -> List[str]:
    """
    Active symbols from the selected lake's _control/registry.json, restricted
    to the approved single-stock equity scope.
    """
    from src.storage.config import resolve_tick_lake_root
    from src.storage.registry import SymbolRegistry

    try:
        root = Path(lake_root) if lake_root is not None else resolve_tick_lake_root()
        registry = SymbolRegistry(root=root)
    except Exception:
        return []
    if not registry.root.exists():
        return []

    try:
        entries = registry.get_active_symbols()
    except Exception:
        return []

    symbols = {
        (entry.display_name or entry.symbol or "").strip().upper()
        for entry in entries
    }
    return sorted(sym for sym in symbols if sym and _is_approved_equity(sym))


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


def get_databento_client(api_key: Optional[str] = None) -> Any:
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
    if db is None:
        raise RuntimeError("Databento SDK is required for live backfill; install the optional databento package")
    return db.Historical(api_key)


def estimate_interval_cost(
    client: Any,
    symbols: List[str],
    start: datetime,
    end: datetime,
    schema: str = "tbbo",
    dataset: str = "DBEQ.BASIC",
) -> float:
    """Cost of one actual Databento request window, in USD."""
    start_str = start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    cost = client.metadata.get_cost(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start_str,
        end=end_str,
    )
    return float(cost)


def estimate_day_cost(
    client: Any,
    symbols: List[str],
    trading_date: date,
    schema: str = "tbbo",
    dataset: str = "DBEQ.BASIC"
) -> float:
    """
    Queries Databento metadata API to get the exact cost in USD for the day's requested slice.
    """
    start_utc, _, end_utc = get_day_trading_bounds(trading_date)
    return estimate_interval_cost(
        client,
        symbols,
        start_utc,
        end_utc,
        schema=schema,
        dataset=dataset,
    )


def is_day_already_backfilled(trading_date: date, symbols: Optional[List[str]] = None) -> bool:
    """
    Checks whether DATABENTO ticks already exist in the lake for the trading day.

    Only gap-filled history counts: a day captured live by Capital.com must not
    stop the gap-filler from adding what it is missing.
    """
    from src.storage.config import resolve_tick_lake_root
    from src.storage.reader import TickLakeReader

    start_utc, _, end_utc = get_day_trading_bounds(trading_date)
    window = (start_utc.replace(tzinfo=None), end_utc.replace(tzinfo=None))

    try:
        reader = TickLakeReader(root=resolve_tick_lake_root())
    except Exception:
        return False

    filled = 0
    for symbol in (symbols or [None]):
        try:
            rows = reader.query_ticks(symbol=symbol, start=window[0], end=window[1], limit=1001)
        except Exception:
            return False
        filled += sum(1 for row in rows if str(row.get("source", "")).upper() == "DATABENTO")
        if filled > 1000:
            return True
    return False


QUOTE_V2_COLUMNS = ["timestamp", "symbol", "bid_price", "ask_price", "source", "session"]


def normalize_tbbo_frame(df: pd.DataFrame, trading_date: date) -> pd.DataFrame:
    """Map a Databento tbbo frame onto bid_price and ask_price.

    The trade price and the trade size are dropped. A missing bid or ask is
    dropped too — it is not replaced by the trade price or by a midpoint.
    """
    empty = pd.DataFrame(columns=QUOTE_V2_COLUMNS)
    if df is None or df.empty:
        return empty

    frame = df.copy()
    if "ts_event" not in frame.columns and "ts_recv" not in frame.columns:
        frame = frame.reset_index()
    if "symbol" not in frame.columns:
        frame = frame.reset_index()

    ts_col = "ts_event" if "ts_event" in frame.columns else "ts_recv"
    if ts_col not in frame.columns or "symbol" not in frame.columns:
        return empty

    from src.data.gap_fill import session_label

    ts_series = pd.to_datetime(frame[ts_col], utc=True)
    bid = (
        pd.to_numeric(frame["bid_px_00"], errors="coerce")
        if "bid_px_00" in frame.columns
        else pd.Series(float("nan"), index=frame.index)
    )
    ask = (
        pd.to_numeric(frame["ask_px_00"], errors="coerce")
        if "ask_px_00" in frame.columns
        else pd.Series(float("nan"), index=frame.index)
    )
    session = ts_series.map(lambda ts: session_label(ts.to_pydatetime(), trading_date))
    norm_df = pd.DataFrame(
        {
            "timestamp": ts_series.dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "symbol": frame["symbol"].astype(str).str.upper(),
            "bid_price": bid,
            "ask_price": ask,
            "source": "DATABENTO",
            "session": session,
        }
    )
    norm_df = norm_df.dropna(subset=["timestamp", "symbol", "bid_price", "ask_price"])
    norm_df = norm_df[(norm_df["bid_price"] > 0) & (norm_df["ask_price"] > 0)]
    return norm_df.reset_index(drop=True)


def fetch_and_normalize_day(
    client: Any,
    symbols: List[str],
    trading_date: date,
    schema: str = "tbbo",
    dataset: str = "DBEQ.BASIC"
) -> pd.DataFrame:
    """
    Downloads tick TBBO data from Databento and maps it into schema v2:
    [timestamp, symbol, bid_price, ask_price, source, session]
    """
    start_utc, _reg_open_utc, end_utc = get_day_trading_bounds(trading_date)
    start_str = start_utc.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_utc.strftime("%Y-%m-%dT%H:%M:%S")

    data = client.timeseries.get_range(
        dataset=dataset,
        symbols=symbols,
        schema=schema,
        start=start_str,
        end=end_str
    )
    return normalize_tbbo_frame(data.to_df(), trading_date)


def _stable_tbbo_ingest_id(record: Dict[str, Any]) -> str:
    payload = "|".join(
        [
            str(record.get("symbol", "")),
            str(record.get("timestamp", "")),
            str(record.get("bid_price", "")),
            str(record.get("ask_price", "")),
            str(record.get("source", "DATABENTO")),
        ]
    )
    return "dbtbbo_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def publish_ticks_to_lake(
    df: pd.DataFrame,
    lake_root: Optional[Any] = None,
    batch_id: Optional[str] = None,
    writer_id: str = "databento_backfill",
    sequence: int = 0,
) -> int:
    """
    Publishes normalized ticks into the Parquet tick lake — the only store.

    Goes through the product writer, so an in-progress maintenance window or a
    competing publisher makes this raise rather than write a partial day.
    A stable batch_id replays an existing receipt instead of appending a copy.
    """
    if df.empty:
        return 0

    from src.storage.config import resolve_tick_lake_root
    from src.storage.parquet_writer import TickLakeWriter
    from src.storage.publication import LakePublisher

    root = Path(lake_root) if lake_root is not None else Path(resolve_tick_lake_root())
    records = df.to_dict("records")
    for record in records:
        ingest_id = record.get("ingest_id")
        if ingest_id is None or ingest_id == "" or (isinstance(ingest_id, float) and pd.isna(ingest_id)):
            record["ingest_id"] = _stable_tbbo_ingest_id(record)

    if batch_id:
        with LakePublisher(root=root, writer_id=writer_id) as publisher:
            publisher.publish_batch(records, batch_id=batch_id, sequence=sequence)
        return len(records)

    writer = TickLakeWriter(root=root, writer_id=writer_id)
    try:
        writer.write_ticks(records)
        writer.flush(block=True)
    finally:
        writer.close()
    return len(records)


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
    client: Optional[Any] = None,
    progress_callback: Optional[Any] = None,
    lake_root: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Walks backward one named day at a time.

    The publisher lock is checked before any Databento call. Each day requests
    only all-symbol silence and appends schema v2 rows. Existing files are not
    rewritten. Cost is checked before each day so the credit budget still stops
    the walk.
    """
    from src.data.gap_fill import (
        GapFillBudgetExceeded,
        GapFillEstimateError,
        fill_named_day,
        is_full_nyse_holiday,
        refuse_if_lake_locked,
    )
    from src.storage.config import resolve_tick_lake_root

    root = Path(lake_root) if lake_root is not None else resolve_tick_lake_root()
    if client is None:
        client = get_databento_client()

    symbols = get_target_stock_symbols(root)
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
    print(f"Hours: 04:00 ET -> 20:00 ET, all-symbol silence only", flush=True)
    print(f"Credit Budget Cap: ${max_budget:.2f} USD", flush=True)
    print(f"Starting from: {start_date} going backward", flush=True)
    print(f"============================================================", flush=True)

    refuse_if_lake_locked(root)

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

        if is_full_nyse_holiday(trading_day):
            print(f"⏩ [{day_str}] Full NYSE holiday. Skipping.", flush=True)
            continue

        remaining = max_budget - accumulated_cost
        print(f"📥 [{day_str}] Filling silent stretches...", flush=True)
        try:
            result = fill_named_day(
                trading_day,
                client=client,
                lake_root=root,
                symbols=symbols,
                remaining_budget=remaining,
            )
        except GapFillBudgetExceeded as exc:
            print(f"🛑 Budget limit reached! {exc}. Stopping.", flush=True)
            break
        except GapFillEstimateError as exc:
            print(f"⚠️ [{day_str}] Cost estimate failed (holiday or market closed): {exc}. Skipping.", flush=True)
            continue

        day_cost = float(result.get("estimated_cost") or 0.0)
        ticks_inserted = int(result["rows"])
        if result["requests"] or ticks_inserted:
            accumulated_cost += day_cost
            total_ticks_ingested += ticks_inserted
            completed_days.append(day_str)
            rem_budget = max_budget - accumulated_cost
            print(
                f"✅ [{day_str}] Requested {result['requests']} stretches, ingested {ticks_inserted:,} ticks. "
                f"Spent so far: ${accumulated_cost:.3f} | Remaining budget: ${rem_budget:.3f}",
                flush=True,
            )
        else:
            print(f"ℹ️ [{day_str}] No all-symbol silence to request.", flush=True)

        if progress_callback:
            progress_callback({
                "date": day_str,
                "day_cost": day_cost,
                "accumulated_cost": accumulated_cost,
                "ticks": ticks_inserted,
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
