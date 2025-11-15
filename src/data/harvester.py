"""
Core harvesting logic that orchestrates API calls, normalization, and data collection.
"""
import pandas as pd
import time
from datetime import datetime, time as dt_time
from src.config import US_EASTERN, UTC
from src.api.capital import create_capital_session, fetch_capital_data_range
from src.api.yahoo import fetch_yahoo_market_data
from src.data.normalizer import normalize_capital_df, normalize_yahoo_df


def run_harvest_logic(tickers_to_harvest, target_date, db_map, logger, harvest_mode="🚀 Full Day"):
    """
    Main harvesting workflow that coordinates API calls, normalization, and reporting.
    
    Args:
        tickers_to_harvest: List of ticker symbols to harvest
        target_date: Date to harvest data for
        db_map: Dictionary mapping tickers to their epic/strategy configuration
        logger: Logger instance for status messages
        harvest_mode: Type of harvest ("🚀 Full Day", "🌙 Pre-Market Only", 
                      "☀️ Regular Session Only", "🌆 Post-Market Only")
    
    Returns:
        Tuple of (final_df, report_df) - harvested data and harvest report
    """
    cst, xst = create_capital_session()
    need_capital = True  # Always need Capital.com for pre/post market
    
    if need_capital and not cst:
        logger.log("❌ Capital.com Auth Failed. Cannot proceed.")
        return pd.DataFrame(), pd.DataFrame()

    all_data = []
    report_cards = [] 
    
    # Define all session windows
    pm_start = US_EASTERN.localize(datetime.combine(target_date, dt_time(4, 0))).astimezone(UTC)
    pm_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(9, 30))).astimezone(UTC)
    reg_start = pm_end 
    reg_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(16, 0))).astimezone(UTC)
    post_start = reg_end
    post_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(20, 0))).astimezone(UTC)

    for ticker in tickers_to_harvest:
        if ticker not in db_map:
            logger.log(f"⚠️ Skipping **{ticker}**: Not in inventory.")
            continue
            
        logger.log(f"Processing **{ticker}**...")
        rules = db_map[ticker]
        epic, strategy = rules['epic'], rules['strategy']
        
        df_pre, df_reg, df_post = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        mode_str = strategy

        # --- A. Pre-Market ---
        if harvest_mode not in ["☀️ Regular Session Only", "🌆 Post-Market Only"]:
            if cst:
                time.sleep(0.2)
                raw_pre = fetch_capital_data_range(epic, cst, xst, pm_start, pm_end, logger)
                df_pre = normalize_capital_df(raw_pre, ticker, "PRE")

        # --- B. Regular Session (with Fallback) ---
        if harvest_mode not in ["🌙 Pre-Market Only", "🌆 Post-Market Only"]:
            if strategy == 'CAPITAL_ONLY':
                mode_str = "CAPITAL_ONLY"
                if cst:
                    time.sleep(0.2)
                    raw_reg = fetch_capital_data_range(epic, cst, xst, reg_start, reg_end, logger)
                    df_reg = normalize_capital_df(raw_reg, ticker, "REG")
            else:  # HYBRID
                logger.log(f"   -> Primary Source: Yahoo Finance")
                raw_yahoo = fetch_yahoo_market_data(ticker, target_date, logger)
                
                if not raw_yahoo.empty:
                    logger.log(f"   -> Success (Yahoo): {len(raw_yahoo)} rows.")
                    df_reg = normalize_yahoo_df(raw_yahoo, ticker)
                    mode_str = "HYBRID (Yahoo)"
                else:
                    logger.log(f"   ⚠️ Yahoo failed. Trying Fallback: Capital.com ({epic})")
                    if cst:
                        time.sleep(0.2)
                        raw_capital_fallback = fetch_capital_data_range(epic, cst, xst, reg_start, reg_end, logger)
                        
                        if not raw_capital_fallback.empty:
                            logger.log(f"   -> Success (Capital Fallback): {len(raw_capital_fallback)} rows.")
                            df_reg = normalize_capital_df(raw_capital_fallback, ticker, "REG")
                            mode_str = "HYBRID (Fallback)"
                        else:
                            logger.log(f"   ❌ Fallback failed. No regular session data for {ticker}.")

        # --- C. Post-Market (NEW) ---
        if harvest_mode not in ["🌙 Pre-Market Only", "☀️ Regular Session Only"]:
            if cst:
                time.sleep(0.2)
                logger.log(f"   -> Fetching Post-Market data from Capital.com")
                raw_post = fetch_capital_data_range(epic, cst, xst, post_start, post_end, logger)
                df_post = normalize_capital_df(raw_post, ticker, "POST")
                if not df_post.empty:
                    logger.log(f"   -> Success (Post-Market): {len(df_post)} rows.")

        # --- D. Merge & Report ---
        dfs = [d for d in [df_pre, df_reg, df_post] if not d.empty]
        total_rows = 0
        if dfs:
            combined = pd.concat(dfs).sort_values('timestamp').drop_duplicates('timestamp', keep='last')
            all_data.append(combined)
            total_rows = len(combined)
        
        expected_pre = 330   # 5.5 hours * 60
        expected_reg = 390   # 6.5 hours * 60
        expected_post = 240  # 4 hours * 60
        pre_rows, reg_rows, post_rows = len(df_pre), len(df_reg), len(df_post)
        
        gaps = []
        status_icon = "✅ Complete"
        
        if harvest_mode in ["🚀 Full Day", "🌙 Pre-Market Only"]:
            if pre_rows < (expected_pre * 0.9):
                gaps.append("Pre")
        
        if harvest_mode in ["🚀 Full Day", "☀️ Regular Session Only"]:
            if reg_rows < (expected_reg * 0.9):
                gaps.append("Reg")
        
        if harvest_mode in ["🚀 Full Day", "🌆 Post-Market Only"]:
            if post_rows < (expected_post * 0.9):
                gaps.append("Post")

        if total_rows == 0:
            status_icon = "❌ Failed"
        elif gaps:
            status_icon = f"⚠️ Gappy ({', '.join(gaps)})"
        
        if "Fallback" in mode_str and status_icon == "✅ Complete":
            status_icon = "✅ (Fallback)"

        report_cards.append({
            "Ticker": ticker, 
            "Mode": mode_str, 
            "Pre": pre_rows, 
            "Reg": reg_rows,
            "Post": post_rows,  # NEW
            "Total": total_rows, 
            "Status": status_icon
        })

    if not all_data:
        return pd.DataFrame(), pd.DataFrame(report_cards)
        
    final_df = pd.concat(all_data).reset_index(drop=True)
    report_df = pd.DataFrame(report_cards)
    return final_df, report_df

