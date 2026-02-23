from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import US_EASTERN, UTC, SCHEMA_COLS
from src.api.massive import fetch_massive_data
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_daily
from src.data.normalizer import normalize_yahoo_df

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
    valid_df = df[valid_mask].copy()
    
    dropped = len(df) - len(valid_df)
    if dropped > 0:
        logger.log(f"   ⚠️ {ticker} [{source_name}]: Dropped {dropped} rogue rows not matching target date {target_date}.")
        
    if valid_df.empty:
        return valid_df, f"❌ {source_name} Empty (Wrong date)"
        
    return valid_df, f"✅ {source_name}"
    

def fetch_from_source(source_name, specific_ticker, target_date, logger):
    """
    Generic fetcher that routes to the correct API.
    """
    if not source_name or source_name == "NONE":
        return pd.DataFrame(), "No Source"

    try:
        if source_name == "YAHOO":
            raw = fetch_yahoo_market_data(specific_ticker, target_date, logger)
            if not raw.empty:
                norm = normalize_yahoo_df(raw, specific_ticker) 
                return _validate_date_match(norm, target_date, "Yahoo", logger, specific_ticker)
            return pd.DataFrame(), "❌ Yahoo Empty"
            
        elif source_name == "MASSIVE":
            raw = fetch_massive_data(specific_ticker, target_date, logger)
            if not raw.empty:
                return _validate_date_match(raw, target_date, "Massive", logger, specific_ticker)
            return pd.DataFrame(), "❌ Massive Empty"
            
        elif source_name == "BINANCE":
            raw = fetch_binance_daily(specific_ticker, target_date, logger)
            if not raw.empty:
                return _validate_date_match(raw, target_date, "Binance", logger, specific_ticker)
            return pd.DataFrame(), "❌ Binance Empty"

    except Exception as e:
        logger.log(f"   ⚠️ Error fetching {source_name}: {e}")
        return pd.DataFrame(), f"❌ Error {source_name}"
        
    return pd.DataFrame(), f"Unknown Source {source_name}"

def run_harvest_logic(tickers_to_harvest, target_date, db_map, logger, harvest_mode="🚀 Full Day", progress_callback=None):
    """
    Executes the concurrent harvesting engine.
    
    Primary source is MASSIVE (Polygon.io), fallback is YAHOO.
    Crypto assets use BINANCE.
    """
    def update_ui(ticker, col, val):
        if progress_callback:
            progress_callback(ticker, col, val)

    total_tickers = len(tickers_to_harvest)
    completed_count = 0

    def harvest_single_ticker(ticker):
        nonlocal completed_count
        update_ui(ticker, "Status", "🔄 Harvesting...")
        
        if ticker not in db_map:
            return ticker, pd.DataFrame(), "⚠️ Not in Inventory", "NONE", 0
        
        rules = db_map[ticker]
        t_y = rules.get('yahoo_ticker') or ticker
        t_b = rules.get('binance_ticker')
        t_m = rules.get('massive_ticker')

        # DEFAULT PRIORITY LOGIC (Implemented in code as requested)
        # 1. Crypto / Stablecoins
        if ticker.endswith("USDT") and ticker != "GC=F":
            primary_src = "BINANCE"
            fallback_src = "YAHOO"
            primary_ticker = t_b or ticker
            fallback_ticker = t_y

        # 2. Gold Futures (PAXGUSDT on Binance preferred over GC=F on Yahoo)
        elif ticker == "GC=F":
            primary_src = "BINANCE" if t_b else "YAHOO"
            fallback_src = "YAHOO" if t_b else "NONE"
            primary_ticker = t_b if t_b else t_y
            fallback_ticker = t_y if t_b else None

        # 3. Specialized Assets (Yahoo Only)
        elif ticker in ["CL=F", "VIX", "UUP", "XLC"]:
            primary_src = "YAHOO"
            fallback_src = "NONE"
            primary_ticker = t_y
            fallback_ticker = None

        # 4. Standard Equities / ETFs
        else:
            primary_src = "MASSIVE" if t_m else "YAHOO"
            fallback_src = "YAHOO" if t_m else "NONE"
            primary_ticker = t_m or t_y
            fallback_ticker = t_y if t_m else None

        # Try Primary
        df, msg = fetch_from_source(primary_src, primary_ticker, target_date, logger)
        source_label = primary_src

        # Fallback if Primary fails
        if df.empty and fallback_src != "NONE":
            logger.log(f"   ⚠️ {primary_src} failed for {ticker}. Attempting fallback {fallback_src}...")
            df, msg = fetch_from_source(fallback_src, fallback_ticker, target_date, logger)
            source_label = f"FB-{fallback_src}"

        if df.empty:
            return ticker, pd.DataFrame(), f"❌ Failed ({msg})", "FAILED", 0

        # Post-process
        df['symbol'] = ticker
        df['session'] = 'REG' # Simple label since splicing is removed
        
        # Ensure ET localization for consistent reporting if needed
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(US_EASTERN)

        return ticker, df, f"✅ {source_label}", source_label, len(df)

    # Execute in Parallel
    all_data = []
    report_cards = []
    
    # Using 8 workers as per 8 massive keys for optimal throughput
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_ticker = {executor.submit(harvest_single_ticker, t): t for t in tickers_to_harvest}
        
        for future in as_completed(future_to_ticker):
            try:
                ticker, df, status, source, total_rows = future.result()
                completed_count += 1
                
                if progress_callback:
                    progress_callback(None, "PROG", (completed_count, total_tickers, f"Processed {ticker}"))
                
                update_ui(ticker, "Status", status)
                if not df.empty:
                    update_ui(ticker, "Total Rows", total_rows)
                    all_data.append(df)
                
                report_cards.append({
                    "Ticker": ticker, "Source": source, 
                    "Total": total_rows, "Status": status
                })
            except Exception as e:
                ticker_err = future_to_ticker[future]
                logger.log(f"   ❌ Thread Error for {ticker_err}: {e}")
                report_cards.append({
                    "Ticker": ticker_err, "Source": "FAILED", 
                    "Total": 0, "Status": f"❌ Error"
                })

    if not all_data:
        return pd.DataFrame(), pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()
    
    final_df = pd.DataFrame()
    report_df = pd.DataFrame(report_cards).sort_values("Ticker") if report_cards else pd.DataFrame()

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