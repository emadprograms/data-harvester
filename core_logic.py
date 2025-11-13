import pandas as pd
import requests
import yfinance as yf
from libsql_client import create_client_sync, Statement
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, time as dt_time, timedelta
from pytz import timezone
import time
import os

# --- Configuration & Constants ---
CAPITAL_API_URL_BASE = "https://api-capital.backend-capital.com/api/v1"
US_EASTERN = timezone('US/Eastern')
BAHRAIN_TZ = timezone('Asia/Bahrain')
UTC = timezone('UTC')

SCHEMA_COLS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session']

# --- 🛡️ PILLAR 1: RESILIENCE (Retry Logic) ---
def get_retry_session(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 504)):
    """Creates a requests session with automatic retries."""
    session = requests.Session()
    retry = Retry(
        total=retries, read=retries, connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# --- Internal Core Functions (No Decorators) ---

def get_db_connection():
    """Establishes a synchronous connection to the Turso database."""
    try:
        # Reads from os.environ (set by GitHub Secrets or local .env)
        url = os.environ.get("TURSO_DB_URL")
        token = os.environ.get("TURSO_AUTH_TOKEN")
        
        if not url or not token:
            print("Error: Missing Turso credentials. Check .env or GitHub Secrets.")
            return None
        
        http_url = url.replace("libsql://", "https://")
        config = {"url": http_url, "auth_token": token}
        return create_client_sync(**config)
    except Exception as e:
        print(f"DB Connection Error: {e}")
        return None

def create_capital_session():
    """Creates a Capital.com session and returns tokens."""
    api_key = os.environ.get("CAPITAL_X_CAP_API_KEY")
    identifier = os.environ.get("CAPITAL_IDENTIFIER")
    password = os.environ.get("CAPITAL_PASSWORD")
    
    if not api_key or not identifier or not password:
        print("Error: Missing Capital.com credentials.")
        return None, None

    session = get_retry_session()
    try:
        response = session.post(
            f"{CAPITAL_API_URL_BASE}/session", 
            headers={'X-CAP-API-KEY': api_key, 'Content-Type': 'application/json'}, 
            json={"identifier": identifier, "password": password}, timeout=15
        )
        response.raise_for_status()
        return response.headers.get('CST'), response.headers.get('X-SECURITY-TOKEN')
    except Exception as e:
        print(f"Capital.com session error: {e}")
        return None, None

# --- Database Functions ---

def init_db(client=None):
    """Initializes the database. Uses provided client or gets a new one."""
    print("Initializing database...")
    if not client:
        client = get_db_connection()
    if not client: 
        print("DB connection failed, cannot init.")
        return
    try:
        client.execute("""
            CREATE TABLE IF NOT EXISTS symbol_map (
                user_ticker TEXT PRIMARY KEY,
                capital_epic TEXT NOT NULL,
                source_strategy TEXT DEFAULT 'HYBRID' 
            )
        """)
        client.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL, volume REAL, session TEXT,
                PRIMARY KEY (symbol, timestamp)
            )
        """)
        
        res = client.execute("SELECT count(*) FROM symbol_map")
        if res.rows and res.rows[0][0] == 0:
            print("Seeding database with default symbols...")
            hybrid_tickers = [
                "AMD", "AMZN", "AAPL", "AVGO", "BABA", "GOOGL", "LRCX", "META", 
                "MSFT", "MU", "NVDA", "ORCL", "PANW", "QCOM", "SHOP", "TSLA", "TSM",
                "SPY", "QQQ", "IWM", "DIA"
            ]
            seed_data = [(t, t, "HYBRID") for t in hybrid_tickers]
            for ticker, epic, strategy in seed_data:
                client.execute(
                    "INSERT INTO symbol_map (user_ticker, capital_epic, source_strategy) VALUES (?, ?, ?)", 
                    [ticker, epic, strategy]
                )
            print("Database seeded.")
    except Exception as e:
        print(f"DB Init Error: {e}")

def get_symbol_map_from_db(client=None):
    """Fetches the complete symbol inventory from Turso."""
    if not client:
        client = get_db_connection()
    if not client: return {}
    try:
        res = client.execute("SELECT user_ticker, capital_epic, source_strategy FROM symbol_map ORDER BY user_ticker")
        return {row[0]: {'epic': row[1], 'strategy': row[2]} for row in res.rows}
    except Exception as e:
        print(f"Error fetching inventory: {e}")
        return {}

def upsert_symbol_mapping(ticker, epic, strategy):
    client = get_db_connection()
    if not client: return False
    try:
        client.execute(
            """INSERT INTO symbol_map (user_ticker, capital_epic, source_strategy) 
               VALUES (?, ?, ?) 
               ON CONFLICT(user_ticker) DO UPDATE SET 
                 capital_epic=excluded.capital_epic, 
                 source_strategy=excluded.source_strategy""",
            [ticker, epic, strategy]
        )
        return True
    except Exception as e:
        print(f"Error saving symbol: {e}")
        return False

def delete_symbol_mapping(ticker):
    client = get_db_connection()
    if not client: return False
    try:
        client.execute("DELETE FROM symbol_map WHERE user_ticker = ?", [ticker])
        return True
    except Exception as e:
        print(f"Error deleting symbol: {e}")
        return False

def save_data_to_turso(df: pd.DataFrame, logger=None, client=None):
    """Saves a DataFrame of market data to Turso using batched transactions."""
    if not client:
        client = get_db_connection()
    if not client or df.empty: return False
    
    try:
        statements = []
        for _, row in df.iterrows():
            ts_str = row['timestamp'].isoformat()
            stmt = Statement(
                """INSERT OR REPLACE INTO market_data 
                   (timestamp, symbol, open, high, low, close, volume, session) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [ts_str, row['symbol'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['session']]
            )
            statements.append(stmt)
        
        BATCH_SIZE = 100
        total_batches = (len(statements) + BATCH_SIZE - 1) // BATCH_SIZE
        
        if logger: 
            logger.log(f"   💾 Committing {len(statements)} records in {total_batches} batches...")
        
        for i in range(0, len(statements), BATCH_SIZE):
            batch = statements[i : i + BATCH_SIZE]
            client.batch(batch)
            time.sleep(0.05) 
            
        return True
    except Exception as e:
        err_msg = f"Batch Commit Failed: {e}"
        if logger: logger.log(f"   ❌ {err_msg}")
        else: print(err_msg)
        return False

def fetch_data_health_matrix(tickers: list, start_date, end_date, session_filter="Total"):
    client = get_db_connection()
    if not client: return pd.DataFrame()

    start_str = f"{start_date}T00:00:00"
    end_str = f"{end_date}T23:59:59"
    placeholders = ",".join("?" * len(tickers))

    query = f"""
        SELECT 
            symbol, 
            date(timestamp) as day, 
            COUNT(*) as candle_count
        FROM market_data 
        WHERE symbol IN ({placeholders}) 
          AND timestamp >= ? 
          AND timestamp <= ? 
    """
    params = tickers + [start_str, end_str]
    
    if session_filter != "Total":
        query += " AND session = ? "
        params.append(session_filter)
        
    query += " GROUP BY symbol, day ORDER BY symbol, day"
    
    try:
        res = client.execute(query, params)
        if not res.rows: return pd.DataFrame()
        cols = ['symbol', 'day', 'candle_count']
        df = pd.DataFrame([list(row) for row in res.rows], columns=cols)
        pivot_df = df.pivot(index='symbol', columns='day', values='candle_count')
        return pivot_df
    except Exception as e:
        print(f"Error fetching data health: {e}")
        return pd.DataFrame()

# --- Normalization Functions (Unchanged) ---
def normalize_capital_df(df: pd.DataFrame, symbol: str, session_label: str) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=SCHEMA_COLS)
    df_norm = df.copy()
    df_norm.rename(columns={'SnapshotTime': 'timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
    df_norm['symbol'] = symbol; df_norm['session'] = session_label
    return df_norm[SCHEMA_COLS]

def normalize_yahoo_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty: return pd.DataFrame(columns=SCHEMA_COLS)
    df_norm = df.copy()
    if isinstance(df_norm.columns, pd.MultiIndex): df_norm.columns = df_norm.columns.get_level_values(0)
    df_norm.reset_index(inplace=True)
    df_norm.rename(columns={'Datetime': 'timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
    if df_norm['timestamp'].dt.tz is not None: df_norm['timestamp'] = df_norm['timestamp'].dt.tz_convert('UTC')
    else: df_norm['timestamp'] = df_norm['timestamp'].dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
    df_norm['symbol'] = symbol; df_norm['session'] = 'REG'
    df_norm.columns = [c.lower() for c in df_norm.columns]
    return df_norm[SCHEMA_COLS]

# --- API Functions (Unchanged) ---
def fetch_capital_data_range(epic: str, cst: str, xst: str, start_utc, end_utc, logger) -> pd.DataFrame:
    now_utc = datetime.now(UTC)
    limit_16h_ago = now_utc - timedelta(hours=16)
    
    if start_utc < limit_16h_ago: 
        logger.log(f"   ⚠️ Start time clamped to 16h limit.")
        start_utc = limit_16h_ago + timedelta(minutes=1)
        
    if start_utc >= end_utc: return pd.DataFrame()
    if end_utc > now_utc: end_utc = now_utc
    
    price_params = {
        "resolution": "MINUTE", "max": 1000, 
        'from': start_utc.strftime('%Y-%m-%dT%H:%M:%S'), 
        'to': end_utc.strftime('%Y-%m-%dT%H:%M:%S')
    }
    session = get_retry_session()
    try:
        response = session.get(f"{CAPITAL_API_URL_BASE}/prices/{epic}", headers={'X-SECURITY-TOKEN': xst, 'CST': cst}, params=price_params, timeout=15)
        response.raise_for_status()
        prices = response.json().get('prices', [])
        if not prices: return pd.DataFrame()
        
        extracted = [{'SnapshotTime': p.get('snapshotTime'), 'Open': p.get('openPrice', {}).get('bid'), 'High': p.get('highPrice', {}).get('bid'), 'Low': p.get('lowPrice', {}).get('bid'), 'Close': p.get('closePrice', {}).get('bid'), 'Volume': p.get('lastTradedVolume')} for p in prices]
        df = pd.DataFrame(extracted)
        
        df['SnapshotTime'] = pd.to_datetime(df['SnapshotTime'])
        if df['SnapshotTime'].dt.tz is None: df['SnapshotTime'] = df['SnapshotTime'].dt.tz_localize(BAHRAIN_TZ)
        else: df['SnapshotTime'] = df['SnapshotTime'].dt.tz_convert(BAHRAIN_TZ)
        df['SnapshotTime'] = df['SnapshotTime'].dt.tz_convert(UTC)
        return df
    except Exception as e:
        logger.log(f"   ❌ Error fetching Capital data for {epic}: {e}")
        return pd.DataFrame()

def fetch_yahoo_market_data(ticker: str, target_date_et, logger) -> pd.DataFrame:
    try:
        start = target_date_et
        end = start + pd.Timedelta(days=1)
        df = yf.download(ticker, start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), interval="1m", progress=False)
        if df.empty: return pd.DataFrame()
        
        if df.index.tz is None: df.index = df.index.tz_localize('UTC')
        
        df_est = df.tz_convert(US_EASTERN)
        df_market = df_est.between_time("09:30", "16:00")
        if df_market.empty:
            logger.log(f"   ⚠️ Yahoo returned data, but none in 9:30-16:00 window.")
        return df_market
    except Exception as e:
        logger.log(f"   ❌ Error fetching Yahoo data: {e}")
        return pd.DataFrame()

# =========================================
#       CORE HARVESTING LOGIC
# =========================================
def run_harvest_logic(tickers_to_harvest, target_date, db_map, logger, harvest_mode="🚀 Full Day"):
    cst, xst = create_capital_session()
    
    need_capital = "Regular Session Only" not in harvest_mode or any(db_map[t]['strategy'] == 'CAPITAL_ONLY' for t in tickers_to_harvest if t in db_map)
    
    if need_capital and not cst:
        logger.log("❌ Capital.com Auth Failed. Cannot proceed.")
        return pd.DataFrame(), pd.DataFrame()

    all_data = []
    report_cards = [] 
    
    pm_start = US_EASTERN.localize(datetime.combine(target_date, dt_time(4, 0))).astimezone(UTC)
    pm_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(9, 30))).astimezone(UTC)
    reg_start = pm_end 
    reg_end   = US_EASTERN.localize(datetime.combine(target_date, dt_time(16, 0))).astimezone(UTC)

    for ticker in tickers_to_harvest:
        if ticker not in db_map:
            logger.log(f"⚠️ Skipping **{ticker}**: Not in inventory.")
            continue
            
        logger.log(f"Processing **{ticker}**...")
        rules = db_map[ticker]
        epic, strategy = rules['epic'], rules['strategy']
        
        df_pre, df_reg = pd.DataFrame(), pd.DataFrame()
        mode_str = strategy

        if "Regular Session Only" not in harvest_mode:
            if cst:
                time.sleep(0.2)
                raw_pre = fetch_capital_data_range(epic, cst, xst, pm_start, pm_end, logger)
                df_pre = normalize_capital_df(raw_pre, ticker, "PRE")

        if "Pre-Market Only" not in harvest_mode:
            if strategy == 'CAPITAL_ONLY':
                mode_str = "CAPITAL_ONLY"
                if cst:
                    time.sleep(0.2)
                    raw_reg = fetch_capital_data_range(epic, cst, xst, reg_start, reg_end, logger)
                    df_reg = normalize_capital_df(raw_reg, ticker, "REG")
            else: # HYBRID
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
                            df_reg = pd.DataFrame()
                            mode_str = "HYBRID (Failed)"
                    else:
                        logger.log(f"   ❌ Fallback skipped (No Capital session).")
                        df_reg = pd.DataFrame()
                        mode_str = "HYBRID (Failed)"

        dfs = [d for d in [df_pre, df_reg] if not d.empty]
        total_rows = 0
        if dfs:
            combined = pd.concat(dfs).sort_values('timestamp').drop_duplicates('timestamp', keep='last')
            all_data.append(combined)
            total_rows = len(combined)
        
        expected_pre = 330
        expected_reg = 390
        pre_rows, reg_rows = len(df_pre), len(df_reg)
        
        gaps = []
        status_icon = "✅ Complete"
        
        if harvest_mode in ["🚀 Full Day", "🌙 Pre-Market Only"]:
            if pre_rows < (expected_pre * 0.9): gaps.append("Pre")
        if harvest_mode in ["🚀 Full Day", "☀️ Regular Session Only"]:
            if reg_rows < (expected_reg * 0.9): gaps.append("Reg")

        if total_rows == 0: status_icon = "❌ Failed"
        elif gaps: status_icon = f"⚠️ Gappy ({', '.join(gaps)})"
        if "Fallback" in mode_str and status_icon == "✅ Complete":
             status_icon = "✅ (Fallback)"

        report_cards.append({"Ticker": ticker, "Mode": mode_str, "Pre": pre_rows, "Reg": reg_rows, "Total": total_rows, "Status": status_icon})

    if not all_data:
        return pd.DataFrame(), pd.DataFrame(report_cards)
        
    final_df = pd.concat(all_data).reset_index(drop=True)
    report_df = pd.DataFrame(report_cards)
    return final_df, report_df