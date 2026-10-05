"""
Tick-Health Engine.

Reads the Parquet tick lake to detect quiet intervals and stream staleness.

v5.0 removed DuckDB as storage, so this module has no database backend. The bar-era
checks it used to provide - minute-bar gap detection, cross-store price drift
reconciliation, and database fingerprint/MD5 verification - were deleted along with
the bar subsystem they measured.
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from src.storage.config import resolve_tick_lake_root
from src.storage.reader import TickLakeReader

_TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
_TIMESTAMP_KEYS = ("timestamp", "time_str", "time")


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse a lake timestamp (string or datetime) into an aware UTC datetime."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None:
        return None
    text = str(value).strip()
    for fmt in _TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _tick_timestamp(tick: Dict[str, Any]) -> Optional[datetime]:
    for key in _TIMESTAMP_KEYS:
        if key in tick:
            parsed = _parse_timestamp(tick.get(key))
            if parsed is not None:
                return parsed
    return None


def _default_reader() -> TickLakeReader:
    return TickLakeReader(resolve_tick_lake_root())


def detect_stream_quiet_intervals(
    symbol: str,
    lookback_minutes: int = 60,
    threshold_seconds: int = 120,
    reader: Optional[TickLakeReader] = None,
) -> dict:
    """
    Scans recent lake ticks for quiet periods where no tick arrived for > threshold_seconds.
    Also returns the latest tick timestamp and stream staleness.
    """
    reader = reader or _default_reader()
    now_utc = datetime.now(timezone.utc)
    since_utc = now_utc - timedelta(minutes=lookback_minutes)

    ticks = reader.query_ticks(symbol=symbol, start=since_utc, limit=100000, direction="asc")

    if not ticks:
        latest = reader.get_latest_tick(symbol)
        last_recorded = _tick_timestamp(latest) if latest else None
        return {
            "symbol": symbol,
            "ticks_in_window": 0,
            "quiet_intervals": [],
            "last_tick_time": last_recorded.strftime("%Y-%m-%d %H:%M:%S.%f") if last_recorded else None,
            "seconds_since_last_tick": round((now_utc - last_recorded).total_seconds(), 1) if last_recorded else -1,
            "is_stalled": True,
            "passed": False,
        }

    stamped = [(_tick_timestamp(t), t) for t in ticks]
    stamped = [(ts, t) for ts, t in stamped if ts is not None]
    if not stamped:
        return {
            "symbol": symbol,
            "ticks_in_window": len(ticks),
            "quiet_intervals": [],
            "last_tick_time": None,
            "seconds_since_last_tick": -1,
            "is_stalled": True,
            "passed": False,
        }

    quiet_intervals = []
    for (t1, _), (t2, _) in zip(stamped, stamped[1:]):
        gap = (t2 - t1).total_seconds()
        if gap > threshold_seconds:
            quiet_intervals.append({
                "interval_start": t1.strftime("%Y-%m-%d %H:%M:%S"),
                "interval_end": t2.strftime("%Y-%m-%d %H:%M:%S"),
                "gap_seconds": int(gap),
            })

    last_tick_dt = stamped[-1][0]
    seconds_ago = (now_utc - last_tick_dt).total_seconds()

    return {
        "symbol": symbol,
        "ticks_in_window": len(stamped),
        "quiet_intervals": quiet_intervals,
        "last_tick_time": last_tick_dt.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "seconds_since_last_tick": round(seconds_ago, 1),
        "is_stalled": seconds_ago > threshold_seconds,
        "passed": len(quiet_intervals) == 0 and seconds_ago <= threshold_seconds,
    }
