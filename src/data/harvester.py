"""
Core harvesting logic that orchestrates API calls, normalization, and data collection.
"""
import pandas as pd
import time
from datetime import datetime, time as dt_time
from src.config import US_EASTERN, UTC
from src.api.massive import fetch_massive_data
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_daily
from src.api.twelve_data import fetch_twelve_data
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
                norm = normalize_yahoo_df(raw, specific_ticker) # Symbol will be overwritten later
                return norm, "✅ Yahoo"
            return pd.DataFrame(), "❌ Yahoo Empty"
            
        # --- MASSIVE ---
        elif source_name == "MASSIVE":
            # Massive needs start/end range in UTC
            # We fetch full day (00:00 to 23:59 UTC) and the UI slicing logic handles logic
            start_utc = US_EASTERN.localize(datetime.combine(target_date, dt_time(0, 0))).astimezone(UTC)
            end_utc = US_EASTERN.localize(datetime.combine(target_date, dt_time(23, 59))).astimezone(UTC)
            
            raw, err_msg = fetch_massive_data(specific_ticker, start_utc, end_utc, logger)
            if not raw.empty:
                norm = normalize_massive_df(raw, specific_ticker)
                return norm, "✅ Massive"
            
            # Return specific error (e.g. 429) if available
            return pd.DataFrame(), f"❌ {err_msg}" if err_msg else "❌ Massive Empty"
            
        # --- BINANCE ---
        elif source_name == "BINANCE":
            raw = fetch_binance_daily(specific_ticker, target_date)
            if not raw.empty:
                # Binance returns schema-ready DF mostly, just ensure columns
                return raw, "✅ Binance"
            return pd.DataFrame(), "❌ Binance Empty"
            
        # --- TWELVE DATA ---
        elif source_name == "TWELVE_DATA":
             # Twelve Data needs start/end string. 
             # We pass datetime objects, the client handles formatting
             raw, err = fetch_twelve_data(specific_ticker, target_date, target_date + pd.Timedelta(days=1), logger)
             if not raw.empty:
                 # It's already mostly normalized by our client, ensure columns
                 # We can piggyback off massive normalizer or just return if perfect.
                 # Let's verify columns in client. Yes, lowercase 'open' etc.
                 return raw, "✅ TwelveData"
             return pd.DataFrame(), f"❌ {err}" if err else "❌ Twelve Empty"

    except Exception as e:
        logger.log(f"   ⚠️ Error fetching {source_name}: {e}")
        return pd.DataFrame(), f"❌ Error {source_name}"
        
    return pd.DataFrame(), f"Unknown Source {source_name}"


def run_harvest_logic(tickers_to_harvest, target_date, db_map, logger, harvest_mode="🚀 Full Day", progress_callback=None):
    """
    New Dynamic Harvester:
    1. Look up P1 and P2 from db_map.
    2. Try P1.
    3. If P1 empty/fails, Try P2.
    4. Slice and Dice (Pre/Reg/Post).
    """

    def update_ui(ticker, col, val):
        if progress_callback:
            progress_callback(ticker, col, val)

    # Define time objects for Slicing
    t_930am = dt_time(9,30)
    t_4pm = dt_time(16,0)

    all_data = []
    report_cards = [] 
    total_tickers = len(tickers_to_harvest)

    for idx, ticker in enumerate(tickers_to_harvest):
        update_ui(ticker, "Status", "🔄 Processing...")
        if progress_callback:
            progress_callback(None, "PROG", (idx, total_tickers, f"Processing {ticker}..."))

        if ticker not in db_map:
            update_ui(ticker, "Status", "⚠️ Not in Inventory")
            continue
            
        rules = db_map[ticker]
        
        # Unpack Rules
        p1_source = rules.get('p1', 'YAHOO')
        p2_source = rules.get('p2', 'NONE')
        p3_source = rules.get('p3', 'NONE')
        
        # Get Source-Specific Tickers (fallback to display name if missing)
        t_y = rules.get('yahoo_ticker') or ticker
        t_m = rules.get('massive_ticker') or ticker
        t_b = rules.get('binance_ticker') or ticker
        t_td = rules.get('twelve_data_ticker') or ticker
        
        # Map source name to its specific ticker
        def get_ticker_for_source(src):
            if src == "YAHOO": return t_y
            elif src == "MASSIVE": return t_m
            elif src == "BINANCE": return t_b
            elif src == "TWELVE_DATA": return t_td
            return ticker

        # --- Construct Fetch Pipeline ---
        pipeline = []
        if p1_source and p1_source != "NONE": pipeline.append(p1_source)
        if p2_source and p2_source != "NONE" and p2_source not in pipeline: pipeline.append(p2_source)
        if p3_source and p3_source != "NONE" and p3_source not in pipeline: pipeline.append(p3_source)
        
        # enforce_global_fallback_rule (Implicit Safety Net):
        # Even if user didn't configure P3, if YAHOO isn't in pipeline, add it as last resort
        if "YAHOO" not in pipeline:
             pipeline.append("YAHOO")
             
        # Execute Pipeline
        df_final = pd.DataFrame()
        status_msg = "Pending"
        used_source = "NONE"
        
        for source in pipeline:
            update_ui(ticker, "Status", f"🔄 Trying {source}...")
            
            # Map source to ticker
            src_ticker = get_ticker_for_source(source)
            if not src_ticker: src_ticker = ticker
            
            df_temp, msg = fetch_from_source(source, src_ticker, target_date, logger)
            
            if not df_temp.empty:
                df_final = df_temp
                used_source = source
                status_msg = msg
                break # Success!
            else:
                # Failed, continue to next
                status_msg = msg # Keep last error
                update_ui(ticker, "Status", f"⚠️ {source} Failed: {msg}")
        
        if df_final.empty:
             status_msg = "❌ All Sources Failed"
        
        # --- PROCESSING ---
        if not df_final.empty:
            # Overwrite symbol to match Display Name (User Ticker)
            df_final['symbol'] = ticker
            
            # timezone normalization (Force US/Eastern for slicing)
            if df_final['timestamp'].dt.tz is None:
                df_final['timestamp'] = df_final['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
            else:
                df_final['timestamp'] = df_final['timestamp'].dt.tz_convert(US_EASTERN)
                
            # Visual Slicing
            mask_pre = df_final['timestamp'].dt.time < t_930am
            mask_reg = (df_final['timestamp'].dt.time >= t_930am) & (df_final['timestamp'].dt.time < t_4pm)
            mask_post = df_final['timestamp'].dt.time >= t_4pm

            c_pre = df_final[mask_pre].copy(); c_pre['session'] = 'PRE'
            c_reg = df_final[mask_reg].copy(); c_reg['session'] = 'REG'
            c_post = df_final[mask_post].copy(); c_post['session'] = 'POST'
            
            # Reporting
            count_pre = len(c_pre)
            count_reg = len(c_reg)
            count_post = len(c_post)
            
            update_ui(ticker, "Pre-Market", f"✅ {used_source} ({count_pre})")
            update_ui(ticker, "Regular Session", f"✅ {used_source} ({count_reg})")
            update_ui(ticker, "Post-Market", f"✅ {used_source} ({count_post})")
            
            total_rows = len(df_final)
            update_ui(ticker, "Total Rows", total_rows)
            update_ui(ticker, "Status", "✅ Complete")
            
            # Re-assemble sorted
            final_stack = pd.concat([c_pre, c_reg, c_post]).sort_values('timestamp')
            all_data.append(final_stack)
            
            report_cards.append({
                "Ticker": ticker, "Mode": used_source, 
                "Pre": count_pre, "Reg": count_reg, "Post": count_post, 
                "Total": total_rows, "Status": "✅ Complete"
            })
            
        else:
            # FAILED
            update_ui(ticker, "Pre-Market", "-")
            update_ui(ticker, "Regular Session", "-")
            update_ui(ticker, "Post-Market", "-")
            update_ui(ticker, "Total Rows", 0)
            
            # Use the specific status message (e.g. "❌ Rate Limit")
            update_ui(ticker, "Status", status_msg)
            
            report_cards.append({
                "Ticker": ticker, "Mode": "FAILED", 
                "Pre": 0, "Reg": 0, "Post": 0, "Total": 0, "Status": status_msg
            })

    if progress_callback:
        progress_callback(None, "PROG", (total_tickers, total_tickers, "Harvest Complete!"))

    if not all_data:
        return pd.DataFrame(), pd.DataFrame(report_cards)
    
    # Clean Merge
    clean_data = []
    for df in all_data:
        if df is not None and not df.empty:
            df = df.loc[:, ~df.columns.duplicated()]
            clean_data.append(df)
    
    if not clean_data:
         return pd.DataFrame(), pd.DataFrame(report_cards)

    final_df = pd.concat(clean_data).reset_index(drop=True)
    report_df = pd.DataFrame(report_cards)
    return final_df, report_df