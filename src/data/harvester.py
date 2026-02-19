import pandas as pd
import time
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import US_EASTERN, UTC
from src.api.massive import fetch_massive_data
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_daily
from src.data.normalizer import normalize_massive_df, normalize_yahoo_df

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
            
        # --- MASSIVE ---
        elif source_name == "MASSIVE":
            start_utc = US_EASTERN.localize(datetime.combine(target_date, dt_time(0, 0))).astimezone(UTC)
            end_utc = US_EASTERN.localize(datetime.combine(target_date, dt_time(23, 59))).astimezone(UTC)
            
            raw, err_msg = fetch_massive_data(specific_ticker, start_utc, end_utc, logger)
            if not raw.empty:
                norm = normalize_massive_df(raw, specific_ticker)
                return norm, "✅ Massive"
            
            return pd.DataFrame(), f"❌ {err_msg}" if err_msg else "❌ Massive Empty"
            
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
    Uses ThreadPoolExecutor to process symbols concurrently using all 9 Massive keys.
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
        update_ui(ticker, "Status", "🔄 Processing...")
        
        if ticker not in db_map:
            return ticker, pd.DataFrame(), "⚠️ Not in Inventory", "NONE", 0, 0, 0

        rules = db_map[ticker]
        pipeline = []
        p1, p2, p3 = rules.get('p1'), rules.get('p2'), rules.get('p3')
        
        if p1 and p1 != "NONE": pipeline.append(p1)
        if p2 and p2 != "NONE": pipeline.append(p2)
        if p3 and p3 != "NONE": pipeline.append(p3)
        if "YAHOO" not in pipeline: pipeline.append("YAHOO")

        t_y = rules.get('yahoo_ticker') or ticker
        t_m = rules.get('massive_ticker') or ticker
        t_b = rules.get('binance_ticker') or ticker

        def get_tick(src):
            if src == "YAHOO": return t_y
            elif src == "MASSIVE": return t_m
            elif src == "BINANCE": return t_b
            return ticker

        df_final = pd.DataFrame()
        used_source = "NONE"
        status_msg = "Pending"

        for source in pipeline:
            src_ticker = get_tick(source)
            df_temp, msg = fetch_from_source(source, src_ticker, target_date, logger)
            if not df_temp.empty:
                df_final = df_temp
                used_source = source
                status_msg = msg
                break
            status_msg = msg

        if df_final.empty:
            return ticker, pd.DataFrame(), status_msg, "FAILED", 0, 0, 0

        # Processing & Normalization
        df_final['symbol'] = ticker
        if df_final['timestamp'].dt.tz is None:
            df_final['timestamp'] = df_final['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
        else:
            df_final['timestamp'] = df_final['timestamp'].dt.tz_convert(US_EASTERN)
            
        mask_pre = df_final['timestamp'].dt.time < t_930am
        mask_reg = (df_final['timestamp'].dt.time >= t_930am) & (df_final['timestamp'].dt.time < t_4pm)
        mask_post = df_final['timestamp'].dt.time >= t_4pm

        c_pre = df_final[mask_pre].copy(); c_pre['session'] = 'PRE'
        c_reg = df_final[mask_reg].copy(); c_reg['session'] = 'REG'
        c_post = df_final[mask_post].copy(); c_post['session'] = 'POST'
        
        counts = (len(c_pre), len(c_reg), len(c_post))
        final_stack = pd.concat([c_pre, c_reg, c_post]).sort_values('timestamp')
        
        return ticker, final_stack, "✅ Complete", used_source, counts[0], counts[1], counts[2]

    # Execute in Parallel
    # 9 workers for 9 keys
    all_data = []
    report_cards = []
    
    with ThreadPoolExecutor(max_workers=9) as executor:
        future_to_ticker = {executor.submit(harvest_single_ticker, t): t for t in tickers_to_harvest}
        
        for future in as_completed(future_to_ticker):
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

    if not all_data:
        logger.log("⚠️ No data was collected for any ticker in this run.")
        return pd.DataFrame(), pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()
    
    try:
        # Reset index on each DF to prevent "Reindexing only valid with uniquely valued Index objects"
        cleaned = [df.reset_index(drop=True) for df in all_data]
        final_df = pd.concat(cleaned, ignore_index=True)
        report_df = pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()
        return final_df, report_df
    except Exception as e:
        logger.log(f"❌ Error during final data merging: {e}")
        return pd.DataFrame(), pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()