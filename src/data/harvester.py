"""
Core harvesting logic that orchestrates API calls, normalization, and data collection.
"""
import pandas as pd
import time
from datetime import datetime, time as dt_time
from src.config import US_EASTERN, UTC
from src.api.capital import create_capital_session, fetch_capital_data_range
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_daily
from src.data.normalizer import normalize_capital_df, normalize_yahoo_df


def run_harvest_logic(tickers_to_harvest, target_date, db_map, logger, harvest_mode="🚀 Full Day", progress_callback=None):
    """
    Main harvesting workflow with 3 Lanes:
    1. Binance (USDT): Crypto/Forex (24h)
    2. Yahoo Futures (=F): Commodities (24h)
    3. Yahoo Stocks: Equities (Pre/Reg/Post)
    """

    def update_ui(ticker, col, val):
        if progress_callback:
            progress_callback(ticker, col, val)

    cst, xst = create_capital_session()
    
    # Define session windows for Capital (UTC)
    pm_start = US_EASTERN.localize(datetime.combine(target_date, dt_time(4, 0))).astimezone(UTC)
    pm_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(9, 30))).astimezone(UTC)
    reg_start = pm_end 
    reg_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(16, 0))).astimezone(UTC)
    post_start = reg_end
    post_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(20, 0))).astimezone(UTC)

    # Define time objects for Slicing
    t_4am = dt_time(4,0)
    t_930am = dt_time(9,30)
    t_4pm = dt_time(16,0)
    t_8pm = dt_time(20,0)

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
        epic, strategy = rules['epic'], rules['strategy']

        # ==========================================
        # LANE 1: BINANCE (24H ASSETS)
        # Criteria: Ticker ends with "USDT"
        # ==========================================
        if ticker.upper().endswith("USDT"):
            update_ui(ticker, "Status", "🔄 Fetching (Binance)...")
            df_crypto = fetch_binance_daily(ticker, target_date)
            
            # --- FALLBACK: If Binance fails, try Yahoo Finance ---
            if df_crypto.empty:
                update_ui(ticker, "Status", "⚠️ Binance Failed. Trying Yahoo...")
                
                # Heuristic Mapper: Convert Binance Ticker to Yahoo Ticker
                # Forex: EURUSDT -> EURUSD=X (if starts with standard forex)
                # Crypto: BTCUSDT -> BTC-USD
                
                t_base = ticker.replace("USDT", "")
                # Common forex currencies that might be paired with USDT in user's mind but exist as =X on Yahoo
                forex_majors = ["EUR", "GBP", "AUD", "NZD", "CHF", "CAD", "JPY"]
                
                yahoo_fallback_ticker = f"{t_base}-USD" # Default to Crypto syntax (e.g. BTC-USD)
                if t_base in forex_majors:
                    yahoo_fallback_ticker = f"{t_base}USD=X"
                
                logger.log(f"⚠️ Binance failed for {ticker}. Attempting Yahoo fallback with {yahoo_fallback_ticker}...")
                
                # Fetch full day from Yahoo (this function already handles timezone logic mostly, but returns UTC or NY)
                # fetch_yahoo_market_data returns a dataframe with index as Datetime (localized)
                raw_yahoo = fetch_yahoo_market_data(yahoo_fallback_ticker, target_date, logger)
                
                if not raw_yahoo.empty:
                    # Normalize using Yahoo Normalizer
                    # Note: normalize_yahoo_df logic expects columns like 'Open', 'High' etc. 
                    # fetch_yahoo_market_data returns columns 'Open', 'High'... 
                    # We use "REG" as a placeholder, slicing updates it later.
                    
                    df_crypto = normalize_yahoo_df(raw_yahoo, ticker, "REG")
                    
                    # Fix timezone if needed (normalize_yahoo_df usually converts to UTC)
                    # The Binance lane expects US/Eastern for slicing.
                    if df_crypto['timestamp'].dt.tz is None:
                        df_crypto['timestamp'] = df_crypto['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
                    else:
                        df_crypto['timestamp'] = df_crypto['timestamp'].dt.tz_convert(US_EASTERN)

            if not df_crypto.empty:
                # --- 24h Handling (Green Board) ---
                if df_crypto['timestamp'].dt.tz is None:
                     df_crypto['timestamp'] = df_crypto['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
                else:
                     df_crypto['timestamp'] = df_crypto['timestamp'].dt.tz_convert(US_EASTERN)

                # Visual Slicing for Dashboard consistency
                mask_pre = df_crypto['timestamp'].dt.time < t_930am
                mask_reg = (df_crypto['timestamp'].dt.time >= t_930am) & (df_crypto['timestamp'].dt.time < t_4pm)
                mask_post = df_crypto['timestamp'].dt.time >= t_4pm

                c_pre = df_crypto[mask_pre].copy(); c_pre['session'] = 'PRE'
                c_reg = df_crypto[mask_reg].copy(); c_reg['session'] = 'REG'
                c_post = df_crypto[mask_post].copy(); c_post['session'] = 'POST'

                update_ui(ticker, "Pre-Market", f"✅ Bin/Yho ({len(c_pre)})")
                update_ui(ticker, "Regular Session", f"✅ Bin/Yho ({len(c_reg)})")
                update_ui(ticker, "Post-Market", f"✅ Bin/Yho ({len(c_post)})")
                
                total_rows = len(df_crypto)
                update_ui(ticker, "Total Rows", total_rows)
                update_ui(ticker, "Status", "✅ Complete")
                
                final_crypto = pd.concat([c_pre, c_reg, c_post]).sort_values('timestamp')
                all_data.append(final_crypto)
                
                report_cards.append({
                    "Ticker": ticker, "Mode": "Binance/Yahoo", 
                    "Pre": len(c_pre), "Reg": len(c_reg), "Post": len(c_post), "Total": total_rows, "Status": "✅ Complete"
                })
            else:
                update_ui(ticker, "Status", "❌ Failed (Binance & Yahoo)")
                report_cards.append({
                    "Ticker": ticker, "Mode": "Binance", "Pre":0, "Reg":0, "Post":0, "Total":0, "Status": "❌ Failed"
                })
            continue 

        # ==========================================
        # LANE 2: FUTURES (YAHOO 24H)
        # Criteria: Ticker ends with "=F" (e.g., CL=F, GC=F)
        # ==========================================
        if ticker.upper().endswith("=F"):
            update_ui(ticker, "Status", "🔄 Fetching (Yahoo Futures)...")
            df_futures = fetch_yahoo_market_data(ticker, target_date, logger)
            
            if not df_futures.empty:
                # Normalize (Returns UTC)
                df_norm = normalize_yahoo_df(df_futures, ticker, "REG")
                
                # CRITICAL FIX: Convert to US/Eastern BEFORE slicing
                # This aligns the 09:30 check with New York time, fixing the colors.
                df_norm['timestamp'] = df_norm['timestamp'].dt.tz_convert(US_EASTERN)
                
                # Visual Slicing
                mask_pre = df_norm['timestamp'].dt.time < t_930am
                mask_reg = (df_norm['timestamp'].dt.time >= t_930am) & (df_norm['timestamp'].dt.time < t_4pm)
                mask_post = df_norm['timestamp'].dt.time >= t_4pm
                
                c_pre = df_norm[mask_pre].copy(); c_pre['session'] = 'PRE'
                c_reg = df_norm[mask_reg].copy(); c_reg['session'] = 'REG'
                c_post = df_norm[mask_post].copy(); c_post['session'] = 'POST'
                
                update_ui(ticker, "Pre-Market", f"✅ Yahoo ({len(c_pre)})")
                update_ui(ticker, "Regular Session", f"✅ Yahoo ({len(c_reg)})")
                update_ui(ticker, "Post-Market", f"✅ Yahoo ({len(c_post)})")
                
                total_rows = len(df_norm)
                update_ui(ticker, "Total Rows", total_rows)
                update_ui(ticker, "Status", "✅ Complete")

                final_futures = pd.concat([c_pre, c_reg, c_post]).sort_values('timestamp')
                all_data.append(final_futures)
                
                report_cards.append({
                    "Ticker": ticker, "Mode": "Yahoo (Futures)", 
                    "Pre": len(c_pre), "Reg": len(c_reg), "Post": len(c_post), "Total": total_rows, "Status": "✅ Complete"
                })
            else:
                update_ui(ticker, "Status", "❌ Failed (Yahoo)")
                report_cards.append({
                    "Ticker": ticker, "Mode": "Yahoo (Futures)", "Pre":0, "Reg":0, "Post":0, "Total":0, "Status": "❌ Failed"
                })
            continue

        # ==========================================
        # LANE 3: STANDARD STOCKS (Yahoo -> Capital)
        # ==========================================
        
        # --- SPECIAL VIX MAPPING ---
        # If our internal name is "VIX", we ask Yahoo for "^VIX"
        yahoo_ticker = ticker
        if ticker == "VIX":
            yahoo_ticker = "^VIX"
            
        df_pre, df_reg, df_post = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        yahoo_full_day = pd.DataFrame()
        
        if strategy != 'CAPITAL_ONLY':
            update_ui(ticker, "Status", "🔄 Fetching Yahoo...")
            # Pass the mapped ticker (e.g., ^VIX) to Yahoo
            yahoo_full_day = fetch_yahoo_market_data(yahoo_ticker, target_date, logger)
            
        # --- Process Sessions ---
        # === A. Pre-Market ===
        if harvest_mode not in ["☀️ Regular Session Only", "🌆 Post-Market Only"]:
            got_data = False
            if not yahoo_full_day.empty:
                slice_pre = yahoo_full_day.between_time(t_4am, t_930am, inclusive='left')
                if not slice_pre.empty:
                    df_pre = normalize_yahoo_df(slice_pre, ticker, "PRE")
                    update_ui(ticker, "Pre-Market", f"✅ Yahoo ({len(df_pre)})")
                    got_data = True

            if not got_data:
                update_ui(ticker, "Pre-Market", "🔄 Fetching (Cap)...")
                if cst:
                    time.sleep(0.1)
                    raw_pre = fetch_capital_data_range(epic, cst, xst, pm_start, pm_end, logger)
                    df_pre = normalize_capital_df(raw_pre, ticker, "PRE")
                    if not df_pre.empty:
                        update_ui(ticker, "Pre-Market", f"✅ Capital ({len(df_pre)})")
                    else:
                        update_ui(ticker, "Pre-Market", "⚠️ 0 Rows")
        else:
            update_ui(ticker, "Pre-Market", "-")

        # === B. Regular Session ===
        if harvest_mode not in ["🌙 Pre-Market Only", "🌆 Post-Market Only"]:
            got_data = False
            if not yahoo_full_day.empty:
                slice_reg = yahoo_full_day.between_time(t_930am, t_4pm, inclusive='left')
                if not slice_reg.empty:
                    df_reg = normalize_yahoo_df(slice_reg, ticker, "REG")
                    update_ui(ticker, "Regular Session", f"✅ Yahoo ({len(df_reg)})")
                    got_data = True
            
            if not got_data:
                update_ui(ticker, "Regular Session", "🔄 Fetching (Cap)...")
                if cst:
                    time.sleep(0.1)
                    raw_reg = fetch_capital_data_range(epic, cst, xst, reg_start, reg_end, logger)
                    df_reg = normalize_capital_df(raw_reg, ticker, "REG")
                    if not df_reg.empty:
                        update_ui(ticker, "Regular Session", f"⚠️ Cap Fallback ({len(df_reg)})")
                    else:
                         update_ui(ticker, "Regular Session", "❌ 0 Rows")
        else:
            update_ui(ticker, "Regular Session", "-")

        # === C. Post-Market ===
        if harvest_mode not in ["🌙 Pre-Market Only", "☀️ Regular Session Only"]:
            got_data = False
            if not yahoo_full_day.empty:
                slice_post = yahoo_full_day.between_time(t_4pm, t_8pm, inclusive='left')
                if not slice_post.empty:
                    df_post = normalize_yahoo_df(slice_post, ticker, "POST")
                    update_ui(ticker, "Post-Market", f"✅ Yahoo ({len(df_post)})")
                    got_data = True
            
            if not got_data:
                update_ui(ticker, "Post-Market", "🔄 Fetching (Cap)...")
                if cst:
                    time.sleep(0.1)
                    raw_post = fetch_capital_data_range(epic, cst, xst, post_start, post_end, logger)
                    df_post = normalize_capital_df(raw_post, ticker, "POST")
                    if not df_post.empty:
                         update_ui(ticker, "Post-Market", f"✅ Capital ({len(df_post)})")
                    else:
                         update_ui(ticker, "Post-Market", "⚠️ 0 Rows")
        else:
            update_ui(ticker, "Post-Market", "-")


        # --- Merge & Report ---
        dfs = [d for d in [df_pre, df_reg, df_post] if not d.empty]
        total_rows = 0
        if dfs:
            combined = pd.concat(dfs).sort_values('timestamp').drop_duplicates('timestamp', keep='last')
            all_data.append(combined)
            total_rows = len(combined)
        
        update_ui(ticker, "Total Rows", total_rows)
        
        status_icon = "✅ Complete"
        if total_rows == 0:
            status_icon = "❌ Failed"
        
        final_mode_str = strategy
        if not yahoo_full_day.empty and strategy != 'CAPITAL_ONLY':
            final_mode_str = "HYBRID (Yahoo)"
        elif strategy != 'CAPITAL_ONLY' and yahoo_full_day.empty:
            final_mode_str = "Fallback (Capital)"

        update_ui(ticker, "Status", status_icon)

        report_cards.append({
            "Ticker": ticker, 
            "Mode": final_mode_str, 
            "Pre": len(df_pre), 
            "Reg": len(df_reg), 
            "Post": len(df_post), 
            "Total": total_rows, 
            "Status": status_icon
        })

    if progress_callback:
        progress_callback(None, "PROG", (total_tickers, total_tickers, "Harvest Complete!"))

    if not all_data:
        return pd.DataFrame(), pd.DataFrame(report_cards)
    
    # --- PATCH: Sanitize DataFrames before Concat ---
    # This step is VITAL for preventing "InvalidIndexError" crashes.
    # It safely removes duplicate columns (e.g., if Yahoo returns two 'close' columns)
    # without damaging the rest of the data.
    clean_data = []
    for df in all_data:
        if df is not None and not df.empty:
            # Drop duplicate columns (keeps the first occurrence)
            df = df.loc[:, ~df.columns.duplicated()]
            clean_data.append(df)
    
    if not clean_data:
         return pd.DataFrame(), pd.DataFrame(report_cards)

    final_df = pd.concat(clean_data).reset_index(drop=True)
    report_df = pd.DataFrame(report_cards)
    return final_df, report_df