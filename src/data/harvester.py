from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from src.config import US_EASTERN, UTC, SCHEMA_COLS
from src.api.massive import fetch_massive_data
from src.api.yahoo import fetch_yahoo_market_data
from src.api.binance import fetch_binance_daily
from src.data.normalizer import normalize_yahoo_df


def _session_from_timestamp(ts):
    ts_et = ts.tz_convert(US_EASTERN)
    et_time = ts_et.time()
    if et_time < datetime.strptime("09:30", "%H:%M").time():
        return "PRE"
    if et_time > datetime.strptime("16:00", "%H:%M").time():
        return "POST"
    return "REG"


def _apply_session_labels(df):
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
        p1 = rules.get('p1')
        p2 = rules.get('p2')

        # DEFAULT PRIORITY LOGIC
        if p1 == "BINANCE" or (ticker.endswith("USDT") and ticker != "GC=F"):
            primary_src = "BINANCE"
            fallback_src = "YAHOO"
            primary_ticker = t_b or ticker
            fallback_ticker = t_y
        elif p1 == "YAHOO" and p2 == "MASSIVE":
            # Hybrid mode: PRE/POST from Massive, REG from Yahoo
            m_df, m_msg = fetch_from_source("MASSIVE", t_m or t_y, target_date, logger)
            y_df, y_msg = fetch_from_source("YAHOO", t_y, target_date, logger)

            if not m_df.empty and not y_df.empty:
                m_df = _apply_session_labels(m_df)
                y_df = _apply_session_labels(y_df)
                pre_post = m_df[m_df['session'].isin(["PRE", "POST"])].copy()
                regular = y_df[y_df['session'] == "REG"].copy()
                df = pd.concat([pre_post, regular], ignore_index=True)
                source_label = "HYBRID"
                msg = "✅ Hybrid"
                return _post_process(ticker, df, msg, source_label)
            elif not m_df.empty:
                df = _apply_session_labels(m_df)
                source_label = "MASSIVE-ONLY"
                msg = m_msg
                return _post_process(ticker, df, msg, source_label)
            elif not y_df.empty:
                df = _apply_session_labels(y_df)
                source_label = "YAHOO-ONLY"
                msg = y_msg
                return _post_process(ticker, df, msg, source_label)
            else:
                return ticker, pd.DataFrame(), f"❌ Failed ({m_msg} | {y_msg})", "FAILED", 0
        else:
            primary_src = p1 or ("MASSIVE" if t_m else "YAHOO")
            fallback_src = p2 or ("YAHOO" if primary_src == "MASSIVE" else "NONE")
            primary_ticker = t_m if primary_src == "MASSIVE" else (t_b if primary_src == "BINANCE" else t_y)
            fallback_ticker = t_y if fallback_src == "YAHOO" else (t_m if fallback_src == "MASSIVE" else None)

        # Execution
        df, msg = fetch_from_source(primary_src, primary_ticker, target_date, logger)
        source_label = primary_src
        if df.empty and fallback_src != "NONE":
            df, msg = fetch_from_source(fallback_src, fallback_ticker, target_date, logger)
            source_label = f"FB-{fallback_src}"

        if df.empty:
            return ticker, pd.DataFrame(), f"❌ Failed ({msg})", "FAILED", 0

        return _post_process(ticker, df, msg, source_label)

    def _post_process(ticker, df, msg, source_label):
        df = df.copy()
        df['symbol'] = ticker
        if 'session' not in df.columns:
            df['session'] = 'REG'
        
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(UTC).dt.tz_convert(US_EASTERN)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(US_EASTERN)

        return ticker, df, f"✅ {source_label}", source_label, len(df)

    # Execute in Parallel
    all_data = []
    report_cards = []
    
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
