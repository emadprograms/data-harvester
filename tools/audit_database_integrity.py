#!/usr/bin/env python3
"""
Comprehensive Data Integrity Audit Tool for DuckDB Databases.
Audits 100% of data/streaming.duckdb and data/historical.duckdb:
1. Column Null & Sanity (prices > 0, volume >= 0, timestamps valid).
2. Asset Scope (strictly 19 target stocks, zero ETFs/crypto/commodities in Databento).
3. Trading Hours & Boundaries (strictly 09:00-16:00 ET, zero weekend ticks).
4. Session Classification (PRE strictly 09:00-09:30 ET, REG strictly 09:30-16:00 ET).
5. Calendar Continuity & Market Holiday Verification across all trading sessions.
6. Daily Symbol Completeness (all 19 symbols populated every trading day).
7. Candle Resampling & Aggregation Health.
8. Historical Database overview and symbol_map verification.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Any, Optional, Union

import duckdb

# Known US market holidays between 2025 and 2026 (NYSE/NASDAQ)
KNOWN_US_MARKET_HOLIDAYS = {
    # 2025
    date(2025, 1, 1): "New Year's Day",
    date(2025, 1, 20): "Martin Luther King Jr. Day",
    date(2025, 2, 17): "Presidents' Day",
    date(2025, 4, 18): "Good Friday",
    date(2025, 5, 26): "Memorial Day",
    date(2025, 6, 19): "Juneteenth",
    date(2025, 7, 4): "Independence Day",
    date(2025, 9, 1): "Labor Day",
    date(2025, 11, 27): "Thanksgiving Day",
    date(2025, 12, 25): "Christmas Day",
    # 2026
    date(2026, 1, 1): "New Year's Day",
    date(2026, 1, 19): "Martin Luther King Jr. Day",
    date(2026, 2, 16): "Presidents' Day",
    date(2026, 4, 3): "Good Friday",
    date(2026, 5, 25): "Memorial Day",
    date(2026, 6, 19): "Juneteenth",
    date(2026, 7, 3): "Independence Day (Observed)",
    date(2026, 9, 7): "Labor Day",
    date(2026, 11, 26): "Thanksgiving Day",
    date(2026, 12, 25): "Christmas Day",
}

EXPECTED_EQUITY_SYMBOLS = {
    "AAPL", "ADBE", "AMD", "AMZN", "APP", "AVGO", "BABA", "GOOGL", "META",
    "MSFT", "MU", "NDAQ", "NVDA", "ORCL", "PANW", "QCOM", "SHOP", "TSLA", "TSM"
}

FORBIDDEN_PATTERNS = ["USDT", "=F", "SPY", "QQQ", "IWM", "DIA", "XL", "TLT", "SMH", "SOXX", "VIX", "BTC", "ETH"]


def print_header(title: str):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def print_check(name: str, passed: bool, detail: str = ""):
    badge = "[\033[92mPASS\033[0m]" if passed else "[\033[91mFAIL\033[0m]"
    detail_str = f" - {detail}" if detail else ""
    print(f"{badge} {name}{detail_str}")


EXCHANGE_TZ = "America/New_York"


def pin_session_utc(con) -> None:
    """
    Pins the DuckDB session TimeZone to UTC.

    DuckDB inherits this setting from the host OS and uses it to resolve every implicit
    TIMESTAMP <-> TIMESTAMPTZ cast, so audit results would otherwise change with the machine's
    timezone. Mirrors src.database.connection.SESSION_TIMEZONE.
    """
    try:
        con.execute("SET TimeZone = 'UTC'")
    except Exception:
        pass


def exchange_local_expr(con, table: str, column: str = "timestamp") -> str:
    """
    SQL expression returning the exchange-local (NYSE, America/New_York) wall clock of `column`.

    Mirrors src.dashboard.analytics.build_exchange_local_sql(): the conversion is built from the
    *physical* column type, because `(ts AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'` over a
    TIMESTAMPTZ column cancels itself out through the session-timezone-dependent trailing cast and
    hands back raw UTC (a 09:30 ET open rendered as 13:30).
    """
    data_type = ""
    try:
        row = con.execute(
            "SELECT data_type FROM duckdb_columns() WHERE table_name = ? AND column_name = ? LIMIT 1",
            [table, column],
        ).fetchone()
        data_type = str(row[0]).upper() if row and row[0] else ""
    except Exception:
        data_type = ""

    compact = data_type.replace(" ", "")
    if "TIMEZONE" in compact or "TIMESTAMPTZ" in compact:
        return f"timezone('{EXCHANGE_TZ}', {column}::TIMESTAMPTZ)"
    return f"timezone('{EXCHANGE_TZ}', timezone('UTC', {column}::TIMESTAMP))"


def audit_streaming_db(db_path: str = "data/streaming.duckdb") -> bool:
    print_header(f"AUDITING STREAMING DATABASE: {db_path}")
    if not os.path.exists(db_path):
        print(f"[\033[91mFAIL\033[0m] Database file not found: {db_path}")
        return False

    file_size_gb = os.path.getsize(db_path) / (1024 ** 3)
    print(f"Database File Size: {file_size_gb:.2f} GB")

    con = duckdb.connect(db_path, read_only=True)
    pin_session_utc(con)
    all_passed = True

    # 1. Total records and source distribution
    total_ticks = con.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
    sources = con.execute("SELECT source, COUNT(*) FROM ticks GROUP BY source ORDER BY COUNT(*) DESC").fetchall()
    print(f"\nTotal Ticks Ingested: {total_ticks:,d}")
    for src, count in sources:
        print(f"  • Source '{src}': {count:,d} ticks ({count/total_ticks*100:.2f}%)")

    has_ticks = total_ticks > 0
    print_check("Streaming database contains data", has_ticks, f"{total_ticks:,d} rows")
    all_passed = all_passed and has_ticks

    # 2. Null and sanity checks across 100% of rows
    null_sanity = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE timestamp IS NULL) as null_ts,
            COUNT(*) FILTER (WHERE symbol IS NULL) as null_sym,
            COUNT(*) FILTER (WHERE price IS NULL OR price <= 0) as invalid_price,
            COUNT(*) FILTER (WHERE volume IS NULL OR volume < 0) as invalid_vol,
            COUNT(*) FILTER (WHERE bid IS NOT NULL AND bid < 0) as negative_bid,
            COUNT(*) FILTER (WHERE ask IS NOT NULL AND ask < 0) as negative_ask,
            COUNT(*) FILTER (WHERE bid IS NOT NULL AND ask IS NOT NULL AND bid > ask * 1.05) as crossed_quotes
        FROM ticks
    """).fetchone()

    p_null_ts = (null_sanity[0] == 0)
    print_check("Zero NULL timestamps", p_null_ts, f"{null_sanity[0]} found")
    all_passed = all_passed and p_null_ts

    p_null_sym = (null_sanity[1] == 0)
    print_check("Zero NULL symbols", p_null_sym, f"{null_sanity[1]} found")
    all_passed = all_passed and p_null_sym

    p_inv_px = (null_sanity[2] == 0)
    print_check("Zero invalid or <= 0 prices", p_inv_px, f"{null_sanity[2]} found")
    all_passed = all_passed and p_inv_px

    p_inv_vol = (null_sanity[3] == 0)
    print_check("Zero negative volumes", p_inv_vol, f"{null_sanity[3]} found")
    all_passed = all_passed and p_inv_vol

    p_neg_quotes = (null_sanity[4] == 0 and null_sanity[5] == 0)
    print_check("Zero negative bids/asks", p_neg_quotes, f"{null_sanity[4] + null_sanity[5]} found")
    all_passed = all_passed and p_neg_quotes

    p_cross = (null_sanity[6] == 0)
    print_check("Zero severely crossed quotes (bid > ask*1.05)", p_cross, f"{null_sanity[6]} found")
    all_passed = all_passed and p_cross

    # 2b. Dedicated streaming_database_symbols check
    s_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    s_tbl = "streaming_database_symbols" if "streaming_database_symbols" in s_tables else "streaming_symbol_map"
    s_map_count = con.execute(f"SELECT COUNT(*) FROM {s_tbl}").fetchone()[0]
    p_s_map = (s_map_count == 19)
    print_check(f"Streaming database symbols table ({s_tbl})", p_s_map, f"{s_map_count} configured streaming symbols")
    all_passed = all_passed and p_s_map

    # 3. Asset Scope Check for Databento
    db_symbols = con.execute("""
        SELECT DISTINCT symbol 
        FROM ticks 
        WHERE source = 'DATABENTO' 
        ORDER BY symbol
    """).fetchall()
    distinct_symbols = {r[0] for r in db_symbols}

    p_symbol_set = (distinct_symbols == EXPECTED_EQUITY_SYMBOLS)
    missing = EXPECTED_EQUITY_SYMBOLS - distinct_symbols
    extra = distinct_symbols - EXPECTED_EQUITY_SYMBOLS
    print_check(
        "Exactly 19 target stocks present in Databento ticks",
        p_symbol_set,
        f"Found {len(distinct_symbols)} symbols (Missing: {missing or 'None'}, Extra: {extra or 'None'})"
    )
    all_passed = all_passed and p_symbol_set

    # Check for forbidden ETF / crypto / commodity tickers in Databento
    forbidden_records = con.execute("""
        SELECT COUNT(*)
        FROM ticks
        WHERE source = 'DATABENTO'
          AND (
            symbol IN ('SPY', 'QQQ', 'IWM', 'DIA', 'TLT', 'SMH', 'SOXX', 'CL=F', 'GC=F', 'VIX', 'UUP')
            OR symbol LIKE '%USDT%'
            OR symbol LIKE '%=%'
          )
    """).fetchone()[0]

    p_forbidden = (forbidden_records == 0)
    print_check("Zero forbidden assets (ETFs, Crypto, Commodities)", p_forbidden, f"{forbidden_records} found")
    all_passed = all_passed and p_forbidden

    # 4. Trading Hours & Boundaries Check (ET conversion across all Databento rows)
    ny_ts_expr = exchange_local_expr(con, "ticks")
    hours_audit = con.execute(f"""
        WITH ny_times AS (
            SELECT 
                {ny_ts_expr} as ny_ts,
                session
            FROM ticks
            WHERE source = 'DATABENTO'
        )
        SELECT 
            COUNT(*) FILTER (WHERE (EXTRACT(HOUR FROM ny_ts)*60 + EXTRACT(MINUTE FROM ny_ts)) < 540) as before_9am,
            COUNT(*) FILTER (WHERE (EXTRACT(HOUR FROM ny_ts)*60 + EXTRACT(MINUTE FROM ny_ts)) >= 960) as after_4pm,
            COUNT(*) FILTER (WHERE EXTRACT(DOW FROM ny_ts) IN (0, 6)) as weekend_ticks,
            COUNT(*) FILTER (WHERE session = 'PRE' AND (EXTRACT(HOUR FROM ny_ts)*60 + EXTRACT(MINUTE FROM ny_ts)) >= 570) as pre_past_930,
            COUNT(*) FILTER (WHERE session = 'REG' AND (EXTRACT(HOUR FROM ny_ts)*60 + EXTRACT(MINUTE FROM ny_ts)) < 570) as reg_before_930,
            MIN(EXTRACT(HOUR FROM ny_ts)*60 + EXTRACT(MINUTE FROM ny_ts)) as min_minute,
            MAX(EXTRACT(HOUR FROM ny_ts)*60 + EXTRACT(MINUTE FROM ny_ts)) as max_minute
        FROM ny_times
    """).fetchone()

    p_hours_start = (hours_audit[0] == 0)
    print_check("Zero ticks before 09:00:00 ET", p_hours_start, f"{hours_audit[0]} found (min minute: {hours_audit[5]})")
    all_passed = all_passed and p_hours_start

    p_hours_end = (hours_audit[1] == 0)
    print_check("Zero ticks after 16:00:00 ET", p_hours_end, f"{hours_audit[1]} found (max minute: {hours_audit[6]})")
    all_passed = all_passed and p_hours_end

    p_no_weekends = (hours_audit[2] == 0)
    print_check("Zero ticks on Saturday / Sunday", p_no_weekends, f"{hours_audit[2]} weekend ticks")
    all_passed = all_passed and p_no_weekends

    p_session_pre = (hours_audit[3] == 0)
    print_check("PRE session strictly within 09:00 - 09:30 ET", p_session_pre, f"{hours_audit[3]} invalid PRE ticks")
    all_passed = all_passed and p_session_pre

    p_session_reg = (hours_audit[4] == 0)
    print_check("REG session strictly within 09:30 - 16:00 ET", p_session_reg, f"{hours_audit[4]} invalid REG ticks")
    all_passed = all_passed and p_session_reg

    # 5. Calendar Continuity & Market Holiday Audit
    distinct_dates = con.execute("""
        SELECT DISTINCT timestamp::DATE as d
        FROM ticks
        WHERE source = 'DATABENTO'
        ORDER BY d
    """).fetchall()
    ingested_dates = {r[0] for r in distinct_dates}
    min_date = min(ingested_dates)
    max_date = max(ingested_dates)

    print(f"\nHistorical Span: {min_date} to {max_date} ({len(ingested_dates)} trading days)")

    # Check for missing weekdays that are NOT market holidays
    curr = min_date
    unexpected_missing_days = []
    recognized_holidays = []
    while curr <= max_date:
        if curr.weekday() < 5:  # Weekday
            if curr not in ingested_dates:
                if curr in KNOWN_US_MARKET_HOLIDAYS:
                    recognized_holidays.append((curr, KNOWN_US_MARKET_HOLIDAYS[curr]))
                else:
                    unexpected_missing_days.append(curr)
        curr += timedelta(days=1)

    p_continuity = (len(unexpected_missing_days) == 0)
    print_check(
        "Calendar continuity (no dropped trading sessions)",
        p_continuity,
        f"{len(unexpected_missing_days)} unexpected missing days (Properly skipped {len(recognized_holidays)} NYSE market holidays)"
    )
    if not p_continuity:
        print(f"  ⚠️ Unexpected missing days: {unexpected_missing_days}")
    all_passed = all_passed and p_continuity

    # 6. Daily Symbol Completeness
    daily_stats = con.execute("""
        WITH daily AS (
            SELECT 
                timestamp::DATE as d,
                COUNT(DISTINCT symbol) as sym_count,
                COUNT(*) as tick_count
            FROM ticks
            WHERE source = 'DATABENTO'
            GROUP BY timestamp::DATE
        )
        SELECT 
            MIN(sym_count),
            MAX(sym_count),
            MIN(tick_count),
            MAX(tick_count),
            ROUND(AVG(tick_count))::BIGINT
        FROM daily
    """).fetchone()

    p_daily_sym = (daily_stats[0] == 19 and daily_stats[1] == 19)
    print_check(
        "All 19 symbols active on 100% of trading days",
        p_daily_sym,
        f"Min symbols: {daily_stats[0]}, Max: {daily_stats[1]}, Avg ticks/day: {daily_stats[4]:,d}"
    )
    all_passed = all_passed and p_daily_sym

    # 7. Dynamic Resampling Aggregation Health Check
    try:
        sample_resample = con.execute("""
            SELECT 
                time_bucket(INTERVAL '5 minutes', timestamp) AS bar,
                FIRST(price ORDER BY timestamp) AS o,
                MAX(price) AS h,
                MIN(price) AS l,
                LAST(price ORDER BY timestamp) AS c,
                SUM(volume) AS v,
                COUNT(*) as ticks
            FROM ticks
            WHERE symbol = 'NVDA' 
              AND timestamp::DATE = '2025-06-16'
            GROUP BY bar
            ORDER BY bar
            LIMIT 5
        """).fetchall()
        p_resample = len(sample_resample) > 0
        print_check("Vectorized Candlestick Resampling Test", p_resample, f"Generated {len(sample_resample)} test 5m candles successfully")
    except Exception as e:
        p_resample = False
        print_check("Vectorized Candlestick Resampling Test", False, str(e))
    all_passed = all_passed and p_resample

    con.close()
    return all_passed


def audit_historical_db(db_path: str = "data/historical.duckdb") -> bool:
    print_header(f"AUDITING HISTORICAL DATABASE: {db_path}")
    if not os.path.exists(db_path):
        print(f"[\033[91mFAIL\033[0m] Database file not found: {db_path}")
        return False

    file_size_mb = os.path.getsize(db_path) / (1024 ** 2)
    print(f"Database File Size: {file_size_mb:.2f} MB")

    con = duckdb.connect(db_path, read_only=True)
    pin_session_utc(con)
    all_passed = True

    # 1. Market_data table count and ranges
    candle_stats = con.execute("""
        SELECT 
            COUNT(*),
            COUNT(DISTINCT symbol),
            MIN(timestamp),
            MAX(timestamp)
        FROM market_data
    """).fetchone()

    has_candles = candle_stats[0] > 0
    print_check(
        "Historical market_data table populated",
        has_candles,
        f"{candle_stats[0]:,d} rows across {candle_stats[1]} symbols ({candle_stats[2]} to {candle_stats[3]})"
    )
    all_passed = all_passed and has_candles

    # 2. Source distribution
    hist_sources = con.execute("""
        SELECT source, COUNT(*) 
        FROM market_data 
        GROUP BY source 
        ORDER BY COUNT(*) DESC
    """).fetchall()
    print("  Historical Sources:")
    for src, cnt in hist_sources:
        print(f"    • {src}: {cnt:,d} rows ({cnt/candle_stats[0]*100:.2f}%)")

    # 3. Market_data null & validity check
    c_nulls = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE timestamp IS NULL),
            COUNT(*) FILTER (WHERE symbol IS NULL),
            COUNT(*) FILTER (WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0),
            COUNT(*) FILTER (WHERE high < low OR high < open OR high < close OR low > open OR low > close)
        FROM market_data
    """).fetchone()

    p_candles_clean = (c_nulls[0] == 0 and c_nulls[1] == 0 and c_nulls[2] == 0 and c_nulls[3] == 0)
    print_check(
        "Historical market_data OHLC geometry sanity",
        p_candles_clean,
        f"Nulls: {c_nulls[0] + c_nulls[1]}, Invalid px: {c_nulls[2]}, Bad OHLC bars: {c_nulls[3]}"
    )
    all_passed = all_passed and p_candles_clean

    # 4. Symbol table integrity
    h_tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    h_tbl = "historical_database_symbols" if "historical_database_symbols" in h_tables else "historical_symbol_map"
    sym_map_stats = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT display_name), COUNT(DISTINCT capital_ticker)
        FROM {h_tbl}
    """).fetchone()
    p_map = sym_map_stats[0] > 0
    print_check(
        f"Historical Database Symbols table ({h_tbl})",
        p_map,
        f"{sym_map_stats[0]} mappings ({sym_map_stats[1]} unique symbols, {sym_map_stats[2]} capital tickers)"
    )
    all_passed = all_passed and p_map

    view_stats = con.execute("SELECT COUNT(*) FROM symbol_map").fetchone()
    p_view = view_stats[0] == sym_map_stats[0]
    print_check(
        "Backward-compatible symbol_map VIEW",
        p_view,
        f"{view_stats[0]} mappings accessible via symbol_map view"
    )
    all_passed = all_passed and p_view

    con.close()
    return all_passed


def audit_tick_lake(lake_root: Optional[Union[str, Path]] = None) -> bool:
    print_header("AUDITING PARTITIONED PARQUET TICK LAKE")
    all_passed = True

    # 1. Resolve root
    if lake_root is not None:
        lake_path = Path(lake_root).resolve()
    else:
        env_root = os.environ.get("TICK_LAKE_ROOT")
        if env_root:
            lake_path = Path(env_root).resolve()
        else:
            try:
                from src.storage.config import resolve_tick_lake_root
                lake_path = resolve_tick_lake_root()
            except Exception:
                repo_root = Path(__file__).resolve().parent.parent
                lake_path = (repo_root / "data" / "tick_lake").resolve()

    print(f"Lake Root: {lake_path}")
    if not lake_path.exists():
        print_check("Lake directory exists", False, f"Directory not found: {lake_path}")
        return False

    # 2. Read lake.json metadata
    lake_json_path = lake_path / "lake.json"
    if not lake_json_path.is_file():
        print_check("Lake metadata file (lake.json) exists", False, f"Not found at {lake_json_path}")
        return False

    try:
        with open(lake_json_path, "r", encoding="utf-8") as f:
            lake_meta = json.load(f)
        p_meta = lake_meta.get("format") == "tick_lake" and "lake_id" in lake_meta
        print_check(
            "Lake metadata file (lake.json) valid",
            p_meta,
            f"ID: {lake_meta.get('lake_id')}, Schema Version: {lake_meta.get('schema_version')}, Layout: {lake_meta.get('partition_layout')}"
        )
        all_passed = all_passed and p_meta
    except Exception as e:
        print_check("Lake metadata file (lake.json) readable", False, str(e))
        return False

    # 3. Scan ticks/ partitions
    ticks_dir = lake_path / "ticks"
    if not ticks_dir.exists():
        print_check("ticks/ partition directory exists", False, f"Not found at {ticks_dir}")
        return False

    parquet_files = sorted(list(ticks_dir.rglob("*.parquet")))
    partitions = [p for p in ticks_dir.glob("symbol=*/date=*") if p.is_dir()]
    total_bytes = sum(f.stat().st_size for f in parquet_files)
    total_mb = total_bytes / (1024 * 1024)

    has_files = len(parquet_files) > 0
    print_check(
        "Tick lake contains Parquet partition files",
        has_files,
        f"{len(parquet_files)} files across {len(partitions)} date partitions ({total_mb:.2f} MB)"
    )
    all_passed = all_passed and has_files

    if not has_files:
        return all_passed

    # 4. Symbol alignment with _control/registry.json
    active_reg_symbols = set()
    reg_file = lake_path / "_control" / "registry.json"
    if not reg_file.exists():
        reg_file = lake_path / "registry.json"
    if reg_file.exists():
        try:
            with open(reg_file, "r", encoding="utf-8") as f:
                reg_data = json.load(f)
            for sym, s_info in reg_data.get("symbols", {}).items():
                if s_info.get("status") == "ACTIVE" and s_info.get("active", True):
                    active_reg_symbols.add(s_info.get("symbol", sym).upper())
            print_check(
                "SymbolRegistry metadata accessible",
                True,
                f"{len(active_reg_symbols)} active symbols registered (version {reg_data.get('version', 1)})"
            )
        except Exception as e:
            print_check("SymbolRegistry metadata readable", False, str(e))
            all_passed = False
    else:
        print_check("SymbolRegistry file (_control/registry.json)", True, "No registry file found (optional)")

    # 5. In-memory DuckDB audit on Parquet rows
    con = duckdb.connect(":memory:")
    pin_session_utc(con)

    file_paths = [str(f) for f in parquet_files]
    try:
        con.execute("CREATE VIEW lake_ticks AS SELECT * FROM read_parquet(?)", [file_paths])
        total_ticks = con.execute("SELECT COUNT(*) FROM lake_ticks").fetchone()[0]
        print(f"\nTotal Ticks Ingested in Lake: {total_ticks:,d}")

        has_ticks = total_ticks > 0
        print_check("Parquet lake contains data rows", has_ticks, f"{total_ticks:,d} rows")
        all_passed = all_passed and has_ticks

        if total_ticks > 0:
            null_sanity = con.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE timestamp IS NULL) as null_ts,
                    COUNT(*) FILTER (WHERE symbol IS NULL OR symbol = '') as null_sym,
                    COUNT(*) FILTER (WHERE price IS NULL OR price <= 0 OR isnan(price) OR isinf(price)) as invalid_price,
                    COUNT(*) FILTER (WHERE volume IS NULL OR volume < 0 OR isnan(volume) OR isinf(volume)) as invalid_vol,
                    COUNT(*) FILTER (WHERE bid IS NOT NULL AND (bid < 0 OR isnan(bid) OR isinf(bid))) as negative_bid,
                    COUNT(*) FILTER (WHERE ask IS NOT NULL AND (ask < 0 OR isnan(ask) OR isinf(ask))) as negative_ask,
                    COUNT(*) FILTER (WHERE bid IS NOT NULL AND ask IS NOT NULL AND bid > ask * 1.05) as crossed_quotes
                FROM lake_ticks
            """).fetchone()

            p_null_ts = (null_sanity[0] == 0)
            print_check("Zero NULL timestamps in Parquet", p_null_ts, f"{null_sanity[0]} found")
            all_passed = all_passed and p_null_ts

            p_null_sym = (null_sanity[1] == 0)
            print_check("Zero NULL symbols in Parquet", p_null_sym, f"{null_sanity[1]} found")
            all_passed = all_passed and p_null_sym

            p_inv_px = (null_sanity[2] == 0)
            print_check("Zero invalid or <= 0 prices in Parquet", p_inv_px, f"{null_sanity[2]} found")
            all_passed = all_passed and p_inv_px

            p_inv_vol = (null_sanity[3] == 0)
            print_check("Zero negative volumes in Parquet", p_inv_vol, f"{null_sanity[3]} found")
            all_passed = all_passed and p_inv_vol

            p_neg_quotes = (null_sanity[4] == 0 and null_sanity[5] == 0)
            print_check("Zero negative bids/asks in Parquet", p_neg_quotes, f"{null_sanity[4] + null_sanity[5]} found")
            all_passed = all_passed and p_neg_quotes

            p_cross = (null_sanity[6] == 0)
            print_check("Zero severely crossed quotes (bid > ask*1.05)", p_cross, f"{null_sanity[6]} found")
            all_passed = all_passed and p_cross

            # Symbol alignment check
            lake_syms = {r[0] for r in con.execute("SELECT DISTINCT symbol FROM lake_ticks").fetchall() if r[0]}
            print(f"Symbols found in lake ({len(lake_syms)}): {sorted(list(lake_syms))[:10]}...")
            if active_reg_symbols:
                unknown_symbols = lake_syms - active_reg_symbols
                p_sym_align = (len(unknown_symbols) == 0)
                print_check(
                    "Lake symbols align with active Registry entries",
                    p_sym_align,
                    f"{len(lake_syms)} symbols present in ticks (unregistered: {unknown_symbols or 'None'})"
                )
                all_passed = all_passed and p_sym_align

            # Source distribution
            sources = con.execute("SELECT source, COUNT(*) FROM lake_ticks GROUP BY source ORDER BY COUNT(*) DESC").fetchall()
            for src, count in sources:
                print(f"  • Source '{src}': {count:,d} ticks ({count/total_ticks*100:.2f}%)")

            # Vectorized candle resampling check
            try:
                sample_resample = con.execute("""
                    SELECT 
                        time_bucket(INTERVAL '1 minute', timestamp) AS bar,
                        FIRST(price ORDER BY timestamp) AS o,
                        MAX(price) AS h,
                        MIN(price) AS l,
                        LAST(price ORDER BY timestamp) AS c,
                        SUM(volume) AS v,
                        COUNT(*) as ticks
                    FROM lake_ticks
                    GROUP BY bar
                    ORDER BY bar DESC
                    LIMIT 5
                """).fetchall()
                p_resample = len(sample_resample) > 0
                print_check("Vectorized Parquet Resampling Test", p_resample, f"Generated {len(sample_resample)} test 1m candles successfully")
                all_passed = all_passed and p_resample
            except Exception as e:
                print_check("Vectorized Parquet Resampling Test", False, str(e))
                all_passed = False

    except Exception as e:
        print_check("DuckDB Parquet queries executed cleanly", False, str(e))
        all_passed = False
    finally:
        con.close()

    return all_passed


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive Data Integrity Audit Tool")
    parser.add_argument("--lake-root", default=None, help="Root path of tick lake")
    parser.add_argument("--streaming-db", default="data/streaming.duckdb", help="Path to streaming.duckdb")
    parser.add_argument("--historical-db", default="data/historical.duckdb", help="Path to historical.duckdb")
    parser.add_argument("--lake-only", action="store_true", help="Force tick lake audit")
    args = parser.parse_args()

    use_tick_lake = bool(
        args.lake_only
        or args.lake_root
        or os.environ.get("TICK_LAKE_ROOT")
        or not os.path.exists(args.streaming_db)
    )

    if use_tick_lake:
        stream_ok = audit_tick_lake(args.lake_root)
    else:
        stream_ok = audit_streaming_db(args.streaming_db)

    historical_ok = audit_historical_db(args.historical_db)

    print_header("FINAL DATA INTEGRITY VERDICT")
    if stream_ok and historical_ok:
        print("\033[92m🎉 ALL SYSTEMS 100% HEALTHY - DATA INTEGRITY VERIFIED WITHOUT ERRORS.\033[0m\n")
        sys.exit(0)
    else:
        print("\033[91m❌ INTEGRITY FAILURES DETECTED IN ONE OR MORE DATA STORES.\033[0m\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
