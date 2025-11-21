import streamlit as st
import pandas as pd
import numpy as np
from datetime import timedelta
from src.database.connection import get_db_connection
from lightweight_charts.widgets import StreamlitChart


# ----------------------------------------------------
# Analysis Logic
# ----------------------------------------------------
def check_integrity(df):
    report = {
        "gaps": [],
        "bad_ohlc": [],
        "zero_vol_rth": [],
        "score": 100
    }
    if df.empty:
        return report

    df_sorted = df.sort_values('timestamp').copy()
    df_sorted['delta'] = df_sorted['timestamp'].diff()

    # Look at both ends of the gap
    df_sorted['prev_timestamp'] = df_sorted['timestamp'].shift()
    df_sorted['prev_session'] = df_sorted['session'].shift()

    REG_LIMIT = timedelta(minutes=5)
    EXT_LIMIT = timedelta(minutes=30)
    OVERNIGHT_CUTOFF = timedelta(hours=6)

    delta = df_sorted['delta']
    prev_s = df_sorted['prev_session']
    curr_s = df_sorted['session']

    # Ignore very large overnight/weekend gaps
    is_intraday = delta < OVERNIGHT_CUTOFF

    # Gaps fully inside regular session: REG + REG on both sides
    reg_gap = (prev_s == 'REG') & (curr_s == 'REG') & (delta > REG_LIMIT)

    # Gaps fully inside extended session (PRE or POST on both sides)
    ext_gap = (
        prev_s.isin(['PRE', 'POST'])
        & curr_s.isin(['PRE', 'POST'])
        & (delta > EXT_LIMIT)
    )

    # Optional rule: gaps that cross from PRE/POST into REG
    # still treated with the relaxed EXT_LIMIT so you don't
    # get spurious alarms right at the open.
    cross_gap = (
        prev_s.isin(['PRE', 'POST'])
        & (curr_s == 'REG')
        & (delta > EXT_LIMIT)
    )

    gaps_mask = is_intraday & (reg_gap | ext_gap | cross_gap)
    gaps = df_sorted[gaps_mask]
    report['gaps'] = gaps

    # --- existing OHLC + zero-volume logic unchanged ---
    bad_ohlc = df[
        (df['high'] < df['low']) |
        (df['high'] < df['open']) |
        (df['high'] < df['close']) |
        (df['low'] > df['open']) |
        (df['low'] > df['close'])
    ]
    report['bad_ohlc'] = bad_ohlc

    if 'session' in df.columns:
        zero_vol = df[
            (df['session'] == 'REG') &
            (df['volume'] == 0)
        ]
        report['zero_vol_rth'] = zero_vol

    report['score'] -= (len(gaps) * 5)
    report['score'] -= (len(bad_ohlc) * 10)
    report['score'] = max(0, report['score'])

    return report


# ----------------------------------------------------
# Normalize DF -> lightweight-charts format (Area)
# (Kept for potential reuse; not used by StreamlitChart.set)
# ----------------------------------------------------
def normalize_for_chart(df):
    """
    Prepares dataframe for Lightweight Charts (Area/Line).
    Uses ALL data (Pre, Reg, Post) for granular inspection.
    """
    df = df.sort_values('timestamp').copy()
    chart_data = []

    for _, row in df.iterrows():
        ts = row['timestamp']
        if not isinstance(ts, pd.Timestamp):
            ts = pd.to_datetime(ts, utc=True)
        ts_unix = int(ts.timestamp())
        val = row['close']
        if pd.isna(val):
            continue
        chart_data.append({"time": ts_unix, "value": float(val)})

    return chart_data


# ----------------------------------------------------
# Normalize DF -> Daily Candles (OHLC)
# (Kept for potential reuse; not used by StreamlitChart.set)
# ----------------------------------------------------
def normalize_for_daily_candles(df):
    """
    Aggregates 1-minute data into Daily Candles (OHLC).
    Uses UTC days (00:00-23:59) for consistency.
    
    CRITICAL: Filters for 'REG' session only. 
    Pre/Post market spikes should NOT affect the Daily Candle High/Low.
    """
    df = df.sort_values('timestamp').copy()
    
    # --- FILTER: Regular Session Only ---
    if 'session' in df.columns:
        df = df[df['session'] == 'REG']
    
    df = df.set_index('timestamp')
    
    # Resample to Daily
    daily = df.resample('D').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna()
    
    chart_data = []
    for ts, row in daily.iterrows():
        if pd.isna(row['open']) or row['open'] == 0:
            continue
            
        ts_unix = int(ts.timestamp())
        chart_data.append({
            "time": ts_unix,
            "open": float(row['open']),
            "high": float(row['high']),
            "low": float(row['low']),
            "close": float(row['close'])
        })
    return chart_data


# ----------------------------------------------------
# UI Renderer
# ----------------------------------------------------
def render_inspector_ui(inventory_list):
    st.subheader("🔎 Database Inspector (The Truth)")

    # --- Controls ---
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        selected_ticker = st.selectbox("Select Ticker", inventory_list)
    with c2:
        # DEFAULT: 30‑day lookback
        days_back = st.number_input("Lookback Days", min_value=1, value=30)
    with c3:
        view_mode_options = [
            "Intraday (1m)",
            "Intraday (5m)",
            "Intraday (15m)",
            "Intraday (30m)",
            "Intraday (1H)",
            "Intraday (4H)",
            "Daily (1D)",
        ]
        # DEFAULT: 15‑minute overview
        view_mode = st.selectbox(
            "View Mode",
            view_mode_options,
            index=view_mode_options.index("Intraday (15m)"),
        )

    # Extended-hours toggle (affects intraday chart only)
    # DEFAULT: OFF (no PRE/POST)
    show_ext = st.toggle(
        "Show Extended Hours (PRE/POST)",
        value=False,
        help="Turn on to include pre/post market in the intraday chart.",
        key="inspector_show_ext",
    )

    # --- Auto-reactive body (no button) ---
    if not selected_ticker:
        st.info("Select a ticker to inspect.")
        return

    client = get_db_connection()
    if not client:
        st.error("DB Connection Failed")
        return

    limit = 1440 * days_back
    query = (
        "SELECT * FROM market_data WHERE symbol = ? "
        "ORDER BY timestamp DESC LIMIT ?"
    )

    # ----------------------------------------------------
    # Fetch from Turso
    # ----------------------------------------------------
    try:
        res = client.execute(query, [selected_ticker, limit])
        cols = [
            'timestamp', 'symbol', 'open', 'high',
            'low', 'close', 'volume', 'session'
        ]
        rows = res.rows if hasattr(res, 'rows') else res
        df = pd.DataFrame([list(row) for row in rows], columns=cols)
    except Exception as e:
        st.error(f"Query Error: {e}")
        return

    if df.empty:
        st.warning(f"No data found for {selected_ticker}.")
        return

    # ----------------------------------------------------
    # Ensure timestamp -> datetime
    # ----------------------------------------------------
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    # ----------------------------------------------------
    # Basic Stats & Integrity
    # ----------------------------------------------------
    duplicates = df.duplicated(subset=['timestamp']).sum()
    health_report = check_integrity(df)
    score = health_report['score']
    
    st.divider()
    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.metric("Total Rows", len(df))
    with h2:
        st.metric(
            "Health Score",
            f"{score}/100", 
            delta="Perfect" if score == 100 else "- Issues Found",
            delta_color="normal" if score == 100 else "inverse"
        )
    with h3:
        st.metric(
            "Gaps Detected",
            len(health_report['gaps']),
            help="REG > 5m | PRE/POST > 30m"
        )
    with h4:
        st.metric(
            "Logical Errors",
            len(health_report['bad_ohlc']),
            help="High < Low or similar impossibilities"
        )

    if duplicates > 0:
        st.error(f"❌ CRITICAL: Found {duplicates} duplicate timestamps. Data requires wiping.")

    if not health_report['bad_ohlc'].empty:
        with st.expander("⚠️ View Logical Errors (High < Low)", expanded=False):
            st.dataframe(health_report['bad_ohlc'])

    if not health_report['gaps'].empty:
        with st.expander("⚠️ View Time Gaps", expanded=False):
            st.dataframe(health_report['gaps'][['timestamp', 'delta', 'session']])

    # ----------------------------------------------------
    # Chart Rendering (Dynamic) - helper-style
    # ----------------------------------------------------
    st.write(f"### 📈 Visual Audit ({view_mode} - UTC)")

    # Map intraday view modes to resample rules
    intraday_rules = {
        "Intraday (1m)": None,   # raw 1-minute
        "Intraday (5m)": "5T",
        "Intraday (15m)": "15T",
        "Intraday (30m)": "30T",
        "Intraday (1H)": "1H",
        "Intraday (4H)": "4H",
    }

    if view_mode == "Daily (1D)":
        # Daily candles: REG session only, resampled to D
        daily_df = df.copy()
        if 'session' in daily_df.columns:
            daily_df = daily_df[daily_df['session'] == 'REG']
        daily_df = daily_df.set_index('timestamp').sort_index()
        daily_resampled = daily_df.resample('D').agg(
            {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }
        ).dropna().reset_index()
        chart_df = daily_resampled.rename(columns={'timestamp': 'time'})
    else:
        # Intraday: optionally hide extended hours, then resample if needed
        base = df.sort_values('timestamp').copy()
        if not show_ext and 'session' in base.columns:
            base = base[base['session'] == 'REG']

        rule = intraday_rules.get(view_mode, None)

        if rule is None:
            # Raw 1-minute
            chart_df = base.rename(columns={'timestamp': 'time'})
        else:
            # Resampled intraday timeframe
            base_idx = base.set_index('timestamp')
            resampled = base_idx.resample(rule).agg(
                {
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum',
                }
            ).dropna().reset_index()
            chart_df = resampled.rename(columns={'timestamp': 'time'})

    if chart_df.empty:
        chart_data = pd.DataFrame(
            columns=['time', 'open', 'high', 'low', 'close', 'volume', 'color']
        )
    else:
        # Ensure numeric OHLCV
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in chart_df.columns:
                chart_df[col] = pd.to_numeric(chart_df[col], errors='coerce')
            else:
                chart_df[col] = 0

        # Color logic copied from helper: red if open > close, else green
        chart_df['color'] = np.where(
            chart_df['open'] > chart_df['close'],
            'rgba(239, 83, 80, 0.8)',
            'rgba(38, 166, 154, 0.8)'
        )

        # Convert times to ISO8601 strings, same as helper
        chart_df['time'] = pd.to_datetime(chart_df['time'], utc=True).apply(
            lambda x: x.isoformat()
        )

        chart_data = chart_df[['time', 'open', 'high', 'low', 'close', 'volume', 'color']]

    # --- Dynamic min_bar_spacing based on timeframe ---
    spacing_map = {
        "Intraday (1m)": 5,     # tighter for long 1m history
        "Intraday (5m)": 10,
        "Intraday (15m)": 10,
        "Intraday (30m)": 10,
        "Intraday (1H)": 10,
        "Intraday (4H)": 10,
        "Daily (1D)": 10,
    }
    min_spacing = spacing_map.get(view_mode, 10)

    # --- Initialize Chart EXACTLY like the helper file, with dynamic spacing ---
    try:
        chart = StreamlitChart(width="100%", height=900)
        chart.layout(background_color="#0f111a", text_color="#ffffff")
        chart.price_scale()
        chart.time_scale(min_bar_spacing=min_spacing, right_offset=15)
        chart.volume_config()
    except Exception as e:
        st.error(f"Failed to initialize chart: {e}")
        st.stop()

    # --- Set data and render (no series methods used) ---
    chart.set(chart_data)
    chart.load()

    # ----------------------------------------------------
    # Raw Table
    # ----------------------------------------------------
    with st.expander("📄 Raw Data (UTC)", expanded=False):
        st.dataframe(
            df.sort_values('timestamp', ascending=False),
            use_container_width=True
        )
