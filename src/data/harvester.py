import os
import pandas as pd
from datetime import datetime, time as dt_time, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import US_EASTERN, UTC
from src.api.capital import fetch_capital_data
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_daily
from src.data.normalizer import normalize_yahoo_df

def fetch_from_source(source_name, specific_ticker, target_date, logger):
    """
    Generic fetcher that routes to the correct API.
    Returns: DataFrame (Normalized), status_msg
    """
    if not source_name or source_name == "NONE":
        return pd.DataFrame(), "No Source"

    try:
        # --- YAHOO ---
        if source_name == "YAHOO":
            raw = fetch_yahoo_market_data(specific_ticker, target_date, logger)
            if not raw.empty:
                norm = normalize_yahoo_df(raw, specific_ticker) 
                return norm, "✅ Yahoo"
            return pd.DataFrame(), "❌ Yahoo Empty"
            
        # --- CAPITAL ---
        elif source_name == "CAPITAL":
            start_utc = US_EASTERN.localize(datetime.combine(target_date, dt_time(0, 0))).astimezone(UTC)
            # Use next day 00:00 as exclusive end boundary (captures full 24h)
            from datetime import timedelta
            next_day = target_date + timedelta(days=1)
            end_utc = US_EASTERN.localize(datetime.combine(next_day, dt_time(0, 0))).astimezone(UTC)
            
            raw, err_msg = fetch_capital_data(specific_ticker, start_utc, end_utc, logger)
            if not raw.empty:
                # Capital implementation already normalizes to SCHEMA_COLS
                raw['symbol'] = specific_ticker # Ensure symbol is set correctly
                return raw, "✅ Capital"
            
            return pd.DataFrame(), f"❌ {err_msg}" if err_msg else "❌ Capital Empty"
            
        # --- BINANCE ---
        elif source_name == "BINANCE":
            raw = fetch_binance_daily(specific_ticker, target_date, logger)
            if not raw.empty:
                return raw, "✅ Binance"
            return pd.DataFrame(), "❌ Binance Empty"

    except Exception as e:
        logger.log(f"   ⚠️ Error fetching {source_name}: {e}")
        return pd.DataFrame(), f"❌ Error {source_name}"
        
    return pd.DataFrame(), f"Unknown Source {source_name}"


def run_harvest_logic(tickers_to_harvest, target_date, db_map, logger, harvest_mode="🚀 Full Day", progress_callback=None):
    """
    Parallel Harvester:
    Spliced Hybrid Logic: 
    - PRE-Market: Capital.com
    - REG-Market: Yahoo Finance (for Volume)
    - POST-Market: Capital.com
    """
    def update_ui(ticker, col, val):
        if progress_callback:
            progress_callback(ticker, col, val)

    t_930am = dt_time(9,30)
    t_4pm = dt_time(16,0)

    total_tickers = len(tickers_to_harvest)
    completed_count = 0

    def harvest_single_ticker(ticker):
        nonlocal completed_count
        update_ui(ticker, "Status", "🔄 Splicing Hybrid...")
        
        if ticker not in db_map:
            return ticker, pd.DataFrame(), "⚠️ Not in Inventory", "NONE", 0, 0, 0

        rules = db_map[ticker]
        t_y = rules.get('yahoo_ticker') or ticker
        t_c = rules.get('capital_epic') or ticker
        t_b = rules.get('binance_ticker') or ticker
        p1 = rules.get('p1')
        p2 = rules.get('p2')

        # --- BINANCE (Crypto/Forex Proxy) ---
        if p1 == "BINANCE":
            df_b, msg_b = fetch_from_source("BINANCE", t_b, target_date, logger)
            if not df_b.empty:
                # Successfully fetched from Binance
                df_b['timestamp'] = df_b['timestamp'].dt.tz_convert(US_EASTERN)
                df_b['symbol'] = ticker # Map back to display name
                c_pre = df_b[df_b['timestamp'].dt.time < t_930am].copy(); c_pre['session'] = 'PRE'
                c_reg = df_b[(df_b['timestamp'].dt.time >= t_930am) & (df_b['timestamp'].dt.time < t_4pm)].copy(); c_reg['session'] = 'REG'
                c_post = df_b[df_b['timestamp'].dt.time >= t_4pm].copy(); c_post['session'] = 'POST'
                final = pd.concat([c_pre, c_reg, c_post]).sort_values('timestamp')
                return ticker, final, "✅ Binance Hub", "BINANCE", len(c_pre), len(c_reg), len(c_post)
            else:
                # Binance failed, try fallback (Yahoo/Capital)
                logger.log(f"   ⚠️ Binance failed for {ticker}. Attempting fallback...")
                p1 = p2 # Promote p2 to p1 for the hybrid logic below
                # p2 remains as it was, but we effectively skip binance
        
        # 1. Fetch in PRIORITY ORDER: P1 first, P2 only as fallback
        df_primary = pd.DataFrame(); msg_primary = "Skipped"
        df_fallback = pd.DataFrame(); msg_fallback = "Skipped"
        primary_source = p1   # e.g. "YAHOO" or "CAPITAL"
        fallback_source = p2  # e.g. "CAPITAL" or "YAHOO" or "NONE"

        # Map source name -> ticker
        source_ticker = {"YAHOO": t_y, "CAPITAL": t_c, "BINANCE": t_b}

        # Fetch P1
        if primary_source and primary_source != "NONE":
            df_primary, msg_primary = fetch_from_source(
                primary_source, source_ticker.get(primary_source, ticker), target_date, logger
            )

        # Fetch P2 ONLY if P1 failed or returned empty
        if df_primary.empty and fallback_source and fallback_source != "NONE":
            df_fallback, msg_fallback = fetch_from_source(
                fallback_source, source_ticker.get(fallback_source, ticker), target_date, logger
            )

        if df_primary.empty and df_fallback.empty:
            return ticker, pd.DataFrame(), f"❌ Both Failed ({msg_primary}/{msg_fallback})", "FAILED", 0, 0, 0

        # Localize/Convert to ET for splicing
        for d in [df_primary, df_fallback]:
            if not d.empty:
                if d['timestamp'].dt.tz is None:
                    d['timestamp'] = d['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
                else:
                    d['timestamp'] = d['timestamp'].dt.tz_convert(US_EASTERN)

        # 2. Slice and Dice — Primary source gets preference for ALL sessions
        c_pre = pd.DataFrame(); c_reg = pd.DataFrame(); c_post = pd.DataFrame()

        # Use primary for each session, fallback only if primary is empty for that session
        for df_first, df_second in [(df_primary, df_fallback)]:
            if not df_first.empty:
                c_pre = df_first[df_first['timestamp'].dt.time < t_930am].copy()
            if c_pre.empty and not df_second.empty:
                c_pre = df_second[df_second['timestamp'].dt.time < t_930am].copy()

            if not df_first.empty:
                c_reg = df_first[(df_first['timestamp'].dt.time >= t_930am) & (df_first['timestamp'].dt.time < t_4pm)].copy()
            if c_reg.empty and not df_second.empty:
                c_reg = df_second[(df_second['timestamp'].dt.time >= t_930am) & (df_second['timestamp'].dt.time < t_4pm)].copy()

            if not df_first.empty:
                c_post = df_first[df_first['timestamp'].dt.time >= t_4pm].copy()
            if c_post.empty and not df_second.empty:
                c_post = df_second[df_second['timestamp'].dt.time >= t_4pm].copy()

        # Assign session labels
        if not c_pre.empty: c_pre['session'] = 'PRE' 
        if not c_reg.empty: c_reg['session'] = 'REG'
        if not c_post.empty: c_post['session'] = 'POST'
        
        # Common cleanup
        final_stack = pd.concat([c_pre, c_reg, c_post]).sort_values('timestamp')
        final_stack['symbol'] = ticker # Map back to display name
        
        source_label = "HYBRID"
        if df_fallback.empty: 
            # Only primary was used
            source_label = f"{primary_source}-ONLY"
        if df_primary.empty:
            # Only fallback was used
            source_label = f"{fallback_source}-ONLY"

        status_label = "✅ Spliced"
        if source_label == "CAPITAL-ONLY": status_label = "✅ Capital"
        if source_label == "YAHOO-ONLY": status_label = "✅ Yahoo"

        return ticker, final_stack, status_label, source_label, len(c_pre), len(c_reg), len(c_post)

    # Execute in Parallel
    # 9 workers for 9 keys
    all_data = []
    report_cards = []
    
    with ThreadPoolExecutor(max_workers=9) as executor:
        future_to_ticker = {executor.submit(harvest_single_ticker, t): t for t in tickers_to_harvest}
        
        for future in as_completed(future_to_ticker):
            try:
                ticker, df, status, source, pre, reg, post = future.result()
                completed_count += 1
                
                if progress_callback:
                    progress_callback(None, "PROG", (completed_count, total_tickers, f"Processed {ticker}"))
                
                update_ui(ticker, "Status", status)
                if not df.empty:
                    update_ui(ticker, "Pre-Market", f"✅ {source} ({pre})")
                    update_ui(ticker, "Regular Session", f"✅ {source} ({reg})")
                    update_ui(ticker, "Post-Market", f"✅ {source} ({post})")
                    update_ui(ticker, "Total Rows", len(df))
                    all_data.append(df)
                
                report_cards.append({
                    "Ticker": ticker, "Mode": source, 
                    "Pre": pre, "Reg": reg, "Post": post, 
                    "Total": len(df), "Status": status
                })
            except Exception as e:
                ticker_err = future_to_ticker[future]
                logger.log(f"   ❌ Thread Error for {ticker_err}: {e}")
                report_cards.append({
                    "Ticker": ticker_err, "Mode": "FAILED", 
                    "Pre": 0, "Reg": 0, "Post": 0, 
                    "Total": 0, "Status": f"❌ Error: {str(e)[:30]}"
                })

    if not all_data:
        logger.log("⚠️ No data was collected for any ticker in this run.")
        return pd.DataFrame(), pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()
    
    final_df = pd.DataFrame()
    report_df = pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()

    try:
        # Aggressive cleanup to prevent "Reindexing only valid with uniquely valued Index objects"
        cleaned = []
        for df in all_data:
            if not df.empty:
                # 1. Normalize columns to lowercase and unique
                df.columns = [str(c).lower() for c in df.columns]
                df = df.loc[:, ~df.columns.duplicated()]
                
                # 2. Reset index to ensure no index collisions
                df = df.reset_index(drop=True)
                
                # 3. Ensure all SCHEMA_COLS exist (fill missing with None/0)
                for col in SCHEMA_COLS:
                    if col not in df.columns:
                        df[col] = 0.0 if col in ['open', 'high', 'low', 'close', 'volume'] else None
                
                # 4. Filter to exact schema columns
                df = df[SCHEMA_COLS].copy()
                cleaned.append(df)
        
        if cleaned:
            logger.log(f"   🔗 Merging {len(cleaned)} sanitized dataframes...")
            final_df = pd.concat(cleaned, ignore_index=True)
            
            # Final deduplication by timestamp/symbol pair
            dups = final_df.duplicated(subset=['timestamp', 'symbol']).sum()
            if dups > 0:
                logger.log(f"   ⚠️ Found {dups} duplicate timestamp entries. Dropping.")
                final_df = final_df.drop_duplicates(subset=['timestamp', 'symbol'])
    except Exception as e:
        logger.log(f"❌ Error during final data merging: {e}")
        import traceback
        logger.log(traceback.format_exc())

    # --- PERSISTENT LOGGING ---
    try:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"harvest_{timestamp_str}.log")
        
        with open(log_file, "w") as f:
            f.write(f"HARVEST RUN REPORT\n")
            f.write(f"==================\n")
            f.write(f"Run Time: {datetime.now(timezone.utc)} (UTC)\n")
            f.write(f"Target Date: {target_date}\n\n")
            
            if not report_df.empty:
                f.write(report_df.to_string())
            else:
                f.write("No report details available.")
                
            f.write(f"\n\nTotal Rows Harvested: {len(final_df)}\n")
            
        logger.log(f"📄 Full story logged to: {log_file}")
    except Exception as log_err:
        logger.log(f"⚠️ Failed to write log file: {log_err}")

    return final_df, report_df