import streamlit as st
import pandas as pd
import requests
import yfinance as yf
import altair as alt
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

# =========================================================================
# --- 🛠️ AUTOMATION REFACTOR ---
# We now have TWO sets of functions.
# 1. "_internal_...": Clean python, NO Streamlit decorators. Used by Worker.
# 2. "get_cached_...": Streamlit wrappers WITH decorators. Used by UI.
# =========================================================================

# --- Internal Core Functions (No Decorators) ---

def _internal_get_db_connection():
    """Establishes a synchronous connection to the Turso database."""
    try:
        if "turso" in st.secrets:
            url = st.secrets["turso"]["db_url"]
            token = st.secrets["turso"]["auth_token"]
        else:
            url = os.environ.get("TURSO_DB_URL")
            token = os.environ.get("TURSO_AUTH_TOKEN")
        
        if not url or not token:
            if st.runtime.exists():
                st.error("Missing Turso credentials.")
            print("Error: Missing Turso credentials.")
            return None
        
        http_url = url.replace("libsql://", "https://")
        config = {"url": http_url, "auth_token": token}
        return create_client_sync(**config)
    except Exception as e:
        if st.runtime.exists(): st.error(f"Failed to create Turso client: {e}")
        else: print(f"DB Connection Error: {e}")
        return None

def _internal_create_capital_session():
    """Creates a Capital.com session and returns tokens."""
    if "capital_com" in st.secrets:
        api_key = st.secrets["capital_com"]["X_CAP_API_KEY"]
        identifier = st.secrets["capital_com"]["identifier"]
        password = st.secrets["capital_com"]["password"]
    else:
        api_key = os.environ.get("CAPITAL_X_CAP_API_KEY")
        identifier = os.environ.get("CAPITAL_IDENTIFIER")
        password = os.environ.get("CAPITAL_PASSWORD")
    
    if not api_key or not identifier or not password:
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
    except Exception: 
        return None, None

# --- Streamlit Cached Wrappers (For UI Only) ---

@st.cache_resource
def get_cached_db_connection():
    """Cached wrapper for Streamlit UI."""
    return _internal_get_db_connection()

@st.cache_resource(ttl=600)
def get_cached_capital_session():
    """Cached wrapper for Streamlit UI."""
    return _internal_create_capital_session()

# --- Database Functions (Now use NON-CACHED versions by default) ---
# These are used by both UI and Worker, so they must be "clean".

def init_db(client=None):
    """Initializes the database. Uses provided client or gets a new one."""
    if not client:
        client = _internal_get_db_connection()
    if not client: return
    try:
        client.execute("CREATE TABLE IF NOT EXISTS symbol_map (...)") # Truncated for brevity
        client.execute("CREATE TABLE IF NOT EXISTS market_data (...)") # Truncated for brevity
        
        res = client.execute("SELECT count(*) FROM symbol_map")
        if res.rows and res.rows[0][0] == 0:
            # Seed data logic...
            if st.runtime.exists():
                st.toast("Database initialized with default symbols.", icon="💾")
    except Exception as e:
        if st.runtime.exists(): st.error(f"DB Init Error: {e}")

def get_symbol_map_from_db(client=None):
    """Fetches the complete symbol inventory from Turso."""
    if not client:
        client = _internal_get_db_connection()
    if not client: return {}
    try:
        res = client.execute("SELECT user_ticker, capital_epic, source_strategy FROM symbol_map ORDER BY user_ticker")
        return {row[0]: {'epic': row[1], 'strategy': row[2]} for row in res.rows}
    except Exception as e:
        if st.runtime.exists(): st.error(f"Error fetching inventory: {e}")
        return {}

def upsert_symbol_mapping(ticker, epic, strategy):
    client = _internal_get_db_connection()
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
        st.error(f"Error saving symbol: {e}")
        return False

def delete_symbol_mapping(ticker):
    client = _internal_get_db_connection()
    if not client: return False
    try:
        client.execute("DELETE FROM symbol_map WHERE user_ticker = ?", [ticker])
        return True
    except Exception as e:
        st.error(f"Error deleting symbol: {e}")
        return False

def save_data_to_turso(df: pd.DataFrame, logger=None, client=None):
    """Saves a DataFrame of market data to Turso using batched transactions."""
    if not client:
        client = _internal_get_db_connection()
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
        elif st.runtime.exists(): st.error(err_msg)
        return False

def fetch_data_health_matrix(tickers: list, start_date, end_date, session_filter="Total"):
    client = _internal_get_db_connection()
    if not client: return pd.DataFrame()
    # ... (rest of function is unchanged)
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
        st.error(f"Error fetching data health: {e}")
        return pd.DataFrame()

class StreamlitLogger:
    def __init__(self, container): self.container = container
    def log(self, message): self.container.write(f"🔹 {message}"); print(message) 

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
    # ... (rest of function is unchanged)
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
    # ... (rest of function is unchanged)
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
    # --- MODIFIED: Use the NON-CACHED internal function ---
    cst, xst = _internal_create_capital_session()
    
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
        # ... (rest of the loop is unchanged)
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

# =========================================
#               UI SECTIONS
# =========================================

def render_harvester_ui(inventory_list, db_map):
    st.subheader("🌱 Data Harvester")
    # ... (UI state logic unchanged)
    
    c1, c2 = st.columns([1, 2])
    with c1:
        harvest_mode = st.radio("Harvest Mode", ["🚀 Full Day", "🌙 Pre-Market Only", "☀️ Regular Session Only"])
        target_date = st.date_input("Target Date", st.session_state['harvest_target_date'])
    with c2:
        st.write("**Select Symbols to Harvest**")
        selected_tickers = st.multiselect("Tickers", options=inventory_list, default=inventory_list[:2] if inventory_list else None, label_visibility="collapsed")
        st.caption(f"Selected: {len(selected_tickers)}")
        
        if st.button("Start Harvest", type="primary", disabled=(len(selected_tickers) == 0)):
            status_container = st.status("Harvesting Data...", expanded=True)
            logger = StreamlitLogger(status_container)
            
            # --- MODIFIED: UI uses CACHED capital session via run_harvest_logic ---
            # Wait, run_harvest_logic was already fixed to use the internal one.
            # This is fine. The UI calls the same logic, which is what we want.
            final_df, report_df = run_harvest_logic(selected_tickers, target_date, db_map, logger, harvest_mode)
            
            status_container.update(label="Harvest Complete!", state="complete", expanded=False)
            
            st.session_state['harvest_report'] = report_df
            st.session_state['harvest_target_date'] = target_date 
            
            if not final_df.empty:
                st.session_state['harvested_data'] = final_df
            else:
                st.session_state['harvested_data'] = None
                st.warning("No data collected.")
            
            if not report_df.empty:
                fallback_tickers = report_df[report_df['Mode'].str.contains("Fallback")]['Ticker'].tolist()
                if fallback_tickers:
                    st.warning(f"**Fallback Alert:** {', '.join(fallback_tickers)} ...", icon="📡")

    if st.session_state.get('harvest_report') is not None:
        # ... (rest of UI unchanged)
        st.divider()
        col_report, col_viz = st.columns([1, 1])
        report_df = st.session_state['harvest_report']
        final_df = st.session_state.get('harvested_data')
        target_date_obj = st.session_state.get('harvest_target_date', datetime.now(US_EASTERN).date())
        
        with col_report:
            st.write("### 📋 Harvest Report Card")
            total_rows_collected = len(final_df) if final_df is not None else 0
            st.metric("Total Rows Collected", f"{total_rows_collected:,}")
            st.dataframe(report_df, use_container_width=True)
            
            if final_df is not None:
                csv_backup = final_df.to_csv(index=False).encode('utf-8')
                st.download_button("💾 Download Backup CSV", csv_backup, f"backup_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")
                
                btn_label = f"☁️ Commit Data for {target_date_obj}"
                if st.button(btn_label, type="primary"):
                    with st.spinner("Saving..."):
                        # --- MODIFIED: UI now uses the cached DB connection ---
                        if save_data_to_turso(final_df, client=get_cached_db_connection()):
                            st.success("Saved Successfully!"); st.balloons()
        with col_viz:
            if final_df is not None:
                st.write("### 👁️ Visual Check")
                t_sel = st.selectbox("Preview Ticker", final_df['symbol'].unique())
                if t_sel:
                    sub = final_df[final_df['symbol'] == t_sel]
                    chart = alt.Chart(sub).mark_line().encode(x='timestamp:T', y=alt.Y('close:Q', scale=alt.Scale(zero=False)), color='session:N').interactive()
                    st.altair_chart(chart, use_container_width=True)

def render_inventory_ui(db_map, inventory_list):
    st.subheader("📦 Inventory Manager")
    # ... (This UI function only calls upsert/delete, which are safe)
    # ... (No changes needed to this function)
    with st.container(border=True):
        st.write("### ➕ Add New Symbol")
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1: new_ticker = st.text_input("Ticker", placeholder="e.g. AAPL").upper()
        with c2: new_epic = st.text_input("Epic", placeholder="e.g. AAPL").upper()
        with c3: new_strat = st.selectbox("Strategy", ["HYBRID (Stock/ETF)", "CAPITAL_ONLY (Index/CFD)"], key="add_strat")
        if st.button("Save New Symbol", type="primary") and new_ticker:
            code = "CAPITAL_ONLY" if "CAPITAL" in new_strat else "HYBRID"
            epic_val = new_epic if new_epic else new_ticker
            if upsert_symbol_mapping(new_ticker, epic_val, code):
                st.success(f"Saved {new_ticker}"); time.sleep(0.5); st.rerun()

    with st.container(border=True):
        st.write("### ⚡ Edit Existing Symbol")
        if not inventory_list: st.info("No symbols in inventory yet.")
        else:
            STRAT_HYBRID = "HYBRID (Stock/ETF)"
            STRAT_CAPITAL = "CAPITAL_ONLY (Index/CFD)"
            STRAT_OPTIONS = [STRAT_HYBRID, STRAT_CAPITAL]
            
            if 'edit_select' not in st.session_state: st.session_state.edit_select = "" 
            if 'edit_ticker_val' not in st.session_state: st.session_state.edit_ticker_val = ""
            if 'edit_epic_val' not in st.session_state: st.session_state.edit_epic_val = ""
            if 'edit_strat_sel' not in st.session_state: st.session_state.edit_strat_sel = STRAT_HYBRID

            def handle_update():
                original_ticker = st.session_state.edit_select
                new_ticker_val = st.session_state.edit_ticker_val
                new_epic_val = st.session_state.edit_epic_val
                new_strategy_sel = st.session_state.edit_strat_sel
                code = "CAPITAL_ONLY" if "CAPITAL" in new_strategy_sel else "HYBRID"
                if original_ticker and new_ticker_val and original_ticker != new_ticker_val:
                    st.info(f"Renaming {original_ticker} to {new_ticker_val}...")
                    delete_symbol_mapping(original_ticker)
                if new_ticker_val:
                    if upsert_symbol_mapping(new_ticker_val, new_epic_val, code):
                        st.success(f"Updated {new_ticker_val}")
                        st.session_state.edit_select = ""
                        st.session_state.edit_ticker_val = "" 
                        st.session_state.edit_epic_val = "" 
                        st.session_state.edit_strat_sel = STRAT_HYBRID 
                    else: st.error("Failed to update symbol.")
                else: st.error("Ticker field cannot be empty.")

            c_edit1, c_edit_spacer = st.columns([1.5, 2.5])
            with c_edit1: 
                st.selectbox("Select Ticker to Edit", options=[""] + inventory_list, key="edit_select")
            
            current_selection = st.session_state.edit_select
            if current_selection != st.session_state.edit_ticker_val:
                if current_selection in db_map:
                    selected_data = db_map[current_selection]
                    st.session_state.edit_ticker_val = current_selection
                    st.session_state.edit_epic_val = selected_data['epic']
                    st.session_state.edit_strat_sel = STRAT_HYBRID if "HYBRID" in selected_data['strategy'] else STRAT_CAPITAL
                else:
                    st.session_state.edit_ticker_val = "" 
                    st.session_state.edit_epic_val = ""
                    st.session_state.edit_strat_sel = STRAT_HYBRID
            
            c_edit_fields1, c_edit_fields2, c_edit_fields3, c_edit_fields4 = st.columns([1.5, 1.5, 1.5, 1])
            with c_edit_fields1: new_ticker_val = st.text_input("Ticker (Yahoo/PK)", key="edit_ticker_val")
            with c_edit_fields2: new_epic_val = st.text_input("Epic (Capital)", key="edit_epic_val")
            with c_edit_fields3: new_strategy_sel = st.selectbox("Strategy", STRAT_OPTIONS, key="edit_strat_sel")
            with c_edit_fields4:
                st.write(""); st.write("")
                is_disabled = (st.session_state.edit_select == "")
                st.button("Update Symbol", disabled=is_disabled, on_click=handle_update)
    
    st.write("### 📋 Current Inventory")
    if db_map:
        data = [{"Ticker": k, "Epic": v['epic'], "Strategy": v['strategy']} for k, v in db_map.items()]
        st.dataframe(pd.DataFrame(data), use_container_width=True)
    st.write("#### 🗑️ Delete Symbol")
    c_del1, c_del2 = st.columns([3, 1])
    with c_del1: d_t = st.selectbox("Select Symbol to Delete", [""] + inventory_list, key="del_select")
    with c_del2:
        st.write(""); st.write("")
        if st.button("Confirm Delete", type="primary", disabled=(not d_t)):
            delete_symbol_mapping(d_t); st.success(f"Deleted {d_t}"); time.sleep(0.5); st.rerun()

def render_health_dashboard(inventory_list):
    st.subheader("🗓️ Data Health Dashboard")
    st.info("Check the completeness of your data library. Cells show the number of candles collected.")
    
    today = datetime.now(US_EASTERN).date()
    c1, c2 = st.columns(2)
    with c1:
        year = st.selectbox("Select Year", range(today.year, today.year - 3, -1), key="health_year")
    with c2:
        month_names = [datetime(2000, m, 1).strftime('%B') for m in range(1, 13)]
        month_default = today.month - 1
        month_name = st.selectbox("Select Month", month_names, index=month_default, key="health_month")
        month = month_names.index(month_name) + 1
    
    session_mode = st.radio("Select Session to Inspect", ["Full Day (Total)", "🌙 Pre-Market", "☀️ Regular Session"], horizontal=True)
    
    selected_tickers = st.multiselect("Select Symbols", inventory_list, default=inventory_list)
    
    if st.button("🔍 Generate Health Report", type="primary") and selected_tickers:
        start_date = datetime(year, month, 1).date()
        next_month = (start_date.replace(day=28) + timedelta(days=4))
        end_date = next_month - timedelta(days=next_month.day)
        
        if session_mode == "🌙 Pre-Market": session_filter = "PRE"
        elif session_mode == "☀️ Regular Session": session_filter = "REG"
        else: session_filter = "Total"
            
        with st.spinner(f"Querying {session_mode} data health..."):
            health_pivot_df = fetch_data_health_matrix(selected_tickers, start_date, end_date, session_filter)
            
            if not health_pivot_df.empty:
                def style_heatmap(val, mode="Total"):
                    if pd.isna(val): return 'background-color: #262626'
                    if mode == "Total":
                        if val > 700: return 'background-color: #285E28'
                        elif val > 330: return 'background-color: #5E5B28'
                        elif val > 300: return 'background-color: #5E4228'
                    elif mode == "PRE":
                        if val > 300: return 'background-color: #285E28'
                        elif val > 100: return 'background-color: #5E5B28'
                    elif mode == "REG":
                        if val > 350: return 'background-color: #285E28'
                        elif val > 100: return 'background-color: #5E5B28'
                    return 'background-color: #5E2828'
                
                # --- TIGHT HEIGHT FIX ---
                num_rows = len(health_pivot_df)
                dynamic_height = (num_rows + 1) * 35 # 35px per row + 1 for header
                
                st.dataframe(
                    health_pivot_df.style.apply(lambda x: x.map(lambda val: style_heatmap(val, mode=session_filter))).format("{:.0f}", na_rep=""), 
                    use_container_width=True, height=dynamic_height
                )
            else:
                st.warning("No data found for the selected symbols and date range.")

# --- Main App ---
def main():
    st.set_page_config(page_title="Market Data Harvester", layout="wide")
    
    # --- MODIFIED: UI uses CACHED connection for init ---
    init_db(client=get_cached_db_connection())
    
    with st.sidebar:
        st.title("🦁 Market Lion")
        app_mode = st.selectbox("Select App Mode", ["⚙️ Inventory Manager", "🌱 Data Harvester", "🗓️ Data Health Dashboard"])
        st.divider()
    
    # --- MODIFIED: UI uses CACHED connection for map ---
    db_map = get_symbol_map_from_db(client=get_cached_db_connection())
    inventory_list = list(db_map.keys())

    if app_mode == "⚙️ Inventory Manager":
        render_inventory_ui(db_map, inventory_list)
    elif app_mode == "🌱 Data Harvester":
        render_harvester_ui(inventory_list, db_map)
    elif app_mode == "🗓️ Data Health Dashboard":
        render_health_dashboard(inventory_list)

if __name__ == "__main__":
    main()