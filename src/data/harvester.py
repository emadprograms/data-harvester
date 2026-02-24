from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from src.config import US_EASTERN, UTC, SCHEMA_COLS
from src.api.massive import fetch_massive_data, MassiveProvider
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_range
from src.data.normalizer import normalize_yahoo_df


def _session_from_timestamp(ts):
    """Calculates market session (PRE/REG/POST) based on ET time."""
    ts_et = ts.tz_convert(US_EASTERN)
    et_time = ts_et.time()
    if et_time < datetime.strptime("09:30", "%H:%M").time():
        return "PRE"
    if et_time > datetime.strptime("16:00", "%H:%M").time():
        return "POST"
    return "REG"


def _apply_session_labels(df):
    """Applies session labels to a dataframe based on timestamps."""
    if df.empty:
        return df
    out = df.copy()
    if out['timestamp'].dt.tz is None:
        out['timestamp'] = out['timestamp'].dt.tz_localize(UTC)
    out['session'] = out['timestamp'].apply(_session_from_timestamp)
    return out

def _validate_date_match(df, target_date, source_name, logger, ticker):
    """ Helper to ensure API didn't return rogue historical/future data. """
    if df.empty:
        return df, "Empty"
        
    temp_df = df.copy()
    if temp_df['timestamp'].dt.tz is None:
        temp_df['timestamp'] = temp_df['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
    else:
        temp_df['timestamp'] = temp_df['timestamp'].dt.tz_convert(US_EASTERN)
        
    target_date_str = target_date.strftime('%Y-%m-%d')
    valid_mask = temp_df['timestamp'].dt.strftime('%Y-%m-%d') == target_date_str
    
    # Check for rows that don't match the target date
    rogue_count = len(df) - valid_mask.sum()
    
    if rogue_count > 0:
        # Special handling for Binance: Keep the rows (likely previous day data due to UTC/ET shift)
        if source_name == "Binance":
            logger.log(f"   ℹ️ {ticker} [Binance]: Found {rogue_count} rows from previous day. Adding them to dataset.")
            return df, f"✅ {source_name}"
        else:
            # For others (Yahoo/Massive), strictly enforce the date boundary
            valid_df = df[valid_mask].copy()
            logger.log(f"   ⚠️ {ticker} [{source_name}]: Dropped {rogue_count} rogue rows not matching target date {target_date}.")
            
            if valid_df.empty:
                return valid_df, f"❌ {source_name} Empty (Wrong date)"
            return valid_df, f"✅ {source_name}"
        
    return df, f"✅ {source_name}"
    

def fetch_from_source(source_name, specific_ticker, start_dt, end_dt, logger, massive_provider=None):
    """
    Generic fetcher that routes to the correct API using a datetime range.
    """
    if not source_name or source_name == "NONE":
        return pd.DataFrame(), "No Source"

    try:
        if source_name == "YAHOO":
            from src.api.yahoo import fetch_yahoo_market_data
            raw = fetch_yahoo_market_data(specific_ticker, start_dt, end_dt, logger)
            if not raw.empty:
                return normalize_yahoo_df(raw, specific_ticker), f"✅ Yahoo"
            return pd.DataFrame(), "❌ Yahoo Empty"
            
        elif source_name == "MASSIVE":
            if massive_provider:
                raw = massive_provider.fetch_data(specific_ticker, start_dt, end_dt)
            else:
                from src.api.massive import fetch_massive_data
                raw = fetch_massive_data(specific_ticker, start_dt, end_dt, logger)
            
            if not raw.empty:
                return raw, f"✅ Massive"
            return pd.DataFrame(), "❌ Massive Empty"
            
        elif source_name == "BINANCE":
            from src.api.binance import fetch_binance_range
            raw = fetch_binance_range(specific_ticker, start_dt, end_dt, logger)
            if not raw.empty:
                return raw, f"✅ Binance"
            return pd.DataFrame(), "❌ Binance Empty"

        elif source_name == "CAPITAL":
            from src.api.capital import fetch_capital_data
            raw = fetch_capital_data(specific_ticker, start_dt, end_dt, logger)
            if not raw.empty:
                return raw, f"✅ Capital"
            return pd.DataFrame(), "❌ Capital Empty"

    except Exception as e:
        logger.log(f"   ⚠️ Error fetching {source_name}: {e}")
        return pd.DataFrame(), f"❌ Error {source_name}"
        
    return pd.DataFrame(), f"Unknown Source {source_name}"

def run_harvest_logic(tickers_to_harvest, start_dt, end_dt, db_map, logger, harvest_mode="🚀 Full Day", progress_callback=None, massive_provider=None):
    """
    Executes the simplified concurrent harvesting engine using UTC ranges.
    """
    def update_ui(ticker, col, val):
        if progress_callback:
            progress_callback(ticker, col, val)

    total_tickers = len(tickers_to_harvest)
    completed_count = 0
    
    if not massive_provider:
        massive_provider = MassiveProvider(logger)

    def harvest_single_ticker(ticker):
        nonlocal completed_count
        update_ui(ticker, "Status", "🔄 Harvesting...")
        
        if ticker not in db_map:
            return ticker, pd.DataFrame(), "⚠️ Not in Inventory", "NONE", 0, {}
        
        rules = db_map[ticker]
        t_y = rules.get('yahoo_ticker')
        t_b = rules.get('binance_ticker')
        t_m = rules.get('massive_ticker')
        t_c = rules.get('capital_ticker')

        # ---------------------------------------------------------------------
        # PRIORITY LOGIC
        # ---------------------------------------------------------------------
        
        if t_c:
            primary_src = "MASSIVE" if t_m else "CAPITAL"
            primary_ticker = t_m or t_c
            fallback_src = "CAPITAL" if primary_src == "MASSIVE" else "NONE"
            fallback_ticker = t_c if fallback_src == "CAPITAL" else None
        elif ticker.endswith("USDT") or t_b:
            primary_src = "BINANCE"
            primary_ticker = t_b or ticker
            fallback_src = "YAHOO"
            fallback_ticker = t_y or ticker
        else:
            primary_src = "YAHOO"
            primary_ticker = t_y or ticker
            fallback_src = "NONE"
            fallback_ticker = None

        # ---------------------------------------------------------------------
        # EXECUTION
        # ---------------------------------------------------------------------
        
        logger.log(f"   🔍 {ticker} - Trying {primary_src} ({primary_ticker})...")
        df, msg = fetch_from_source(primary_src, primary_ticker, start_dt, end_dt, logger, massive_provider)
        
        if not df.empty:
            logger.log(f"   ✅ {ticker} - {primary_src} Success ({len(df)} rows).")
            source_label = primary_src
        else:
            logger.log(f"   ⚠️ {ticker} - {primary_src} Failed ({msg}).")
            if fallback_src != "NONE":
                logger.log(f"   🔄 {ticker} - Falling back to {fallback_src} ({fallback_ticker})...")
                df, msg = fetch_from_source(fallback_src, fallback_ticker, start_dt, end_dt, logger, massive_provider)
                if not df.empty:
                    logger.log(f"   ✅ {ticker} - {fallback_src} Success ({len(df)} rows).")
                    source_label = f"FB-{fallback_src}"
                else:
                    logger.log(f"   ❌ {ticker} - {fallback_src} also failed ({msg}).")
                    return ticker, pd.DataFrame(), f"❌ Failed ({msg})", "FAILED", 0, {}
            else:
                return ticker, pd.DataFrame(), f"❌ Failed ({msg})", "FAILED", 0, {}

        # ---------------------------------------------------------------------
        # POST-PROCESS
        # ---------------------------------------------------------------------
        df = df.copy()
        df['symbol'] = ticker
        df = _apply_session_labels(df)
        
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(US_EASTERN)

        session_counts = df['session'].value_counts().to_dict()
        return ticker, df, f"✅ {source_label}", source_label, len(df), session_counts

    # Execute in Parallel
    all_data = []
    report_cards = []
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_ticker = {executor.submit(harvest_single_ticker, t): t for t in tickers_to_harvest}
        
        for future in as_completed(future_to_ticker):
            try:
                ticker, df, status, source, total_rows, s_counts = future.result()
                completed_count += 1
                
                if progress_callback:
                    progress_callback(None, "PROG", (completed_count, total_tickers, f"Processed {ticker}"))
                
                update_ui(ticker, "Status", status)
                if not df.empty:
                    update_ui(ticker, "Total Rows", total_rows)
                    all_data.append(df)
                
                card = {
                    "Ticker": ticker, "Source": source, 
                    "Total": total_rows, "Status": status,
                    "Pre": s_counts.get("PRE", 0),
                    "Reg": s_counts.get("REG", 0),
                    "Post": s_counts.get("POST", 0)
                }
                report_cards.append(card)
                
            except Exception as e:
                ticker_err = future_to_ticker[future]
                logger.log(f"   ❌ Thread Error for {ticker_err}: {e}")
                report_cards.append({
                    "Ticker": ticker_err, "Source": "FAILED", 
                    "Total": 0, "Status": f"❌ Error",
                    "Pre": 0, "Reg": 0, "Post": 0
                })

    if not all_data:
        report_df = pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()
        if not report_df.empty:
            logger.log("\n" + "="*50 + "\nHARVEST SUMMARY (NO DATA)\n" + "="*50 + "\n" + report_df.to_string(index=False) + "\n" + "="*50)
        return pd.DataFrame(), report_df
    
    final_df = pd.DataFrame()
    report_df = pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()

    # Log the summary table
    if not report_df.empty:
        logger.log("\n" + "="*50 + "\nHARVEST SUMMARY\n" + "="*50 + "\n" + report_df.to_string(index=False) + "\n" + "="*50)

    try:
        cleaned = []
        for df in all_data:
            if not df.empty:
                df = df.copy()
                df.columns = [str(c).lower() for c in df.columns]
                df = df.loc[:, ~df.columns.duplicated()]
                df = df.reset_index(drop=True)
                
                for col in SCHEMA_COLS:
                    if col not in df.columns:
                        df[col] = 0.0 if col in ['open', 'high', 'low', 'close', 'volume'] else None
                
                df = df[SCHEMA_COLS].copy()
                cleaned.append(df)
        
        if cleaned:
            final_df = pd.concat(cleaned, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=['timestamp', 'symbol'])
    except Exception as e:
        logger.log(f"❌ Error during final data merging: {e}")

    return final_df, report_df
