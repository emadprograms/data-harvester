import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, time as dt_time, timedelta
from pytz import timezone
import time
import os

# --- Import all logic from the new core_logic.py file ---
import core_logic as cl

# --- Streamlit Cached Wrappers (For UI Only) ---

@st.cache_resource
def get_cached_db_connection():
    """Cached wrapper for Streamlit UI."""
    # We must read from st.secrets *first* for the UI
    if "turso" in st.secrets:
        os.environ["TURSO_DB_URL"] = st.secrets["turso"]["db_url"]
        os.environ["TURSO_AUTH_TOKEN"] = st.secrets["turso"]["auth_token"]
    return cl.get_db_connection()

@st.cache_resource(ttl=600)
def get_cached_capital_session():
    """Cached wrapper for Streamlit UI."""
    if "capital_com" in st.secrets:
        os.environ["CAPITAL_X_CAP_API_KEY"] = st.secrets["capital_com"]["X_CAP_API_KEY"]
        os.environ["CAPITAL_IDENTIFIER"] = st.secrets["capital_com"]["identifier"]
        os.environ["CAPITAL_PASSWORD"] = st.secrets["capital_com"]["password"]
    return cl.create_capital_session()

class StreamlitLogger:
    def __init__(self, container): self.container = container
    def log(self, message): self.container.write(f"🔹 {message}"); print(message) 

# =========================================
#               UI SECTIONS
# =========================================

def render_harvester_ui(inventory_list, db_map):
    st.subheader("🌱 Data Harvester")
    
    if 'harvest_report' not in st.session_state: st.session_state['harvest_report'] = None
    if 'harvested_data' not in st.session_state: st.session_state['harvested_data'] = None
    if 'harvest_target_date' not in st.session_state: st.session_state['harvest_target_date'] = datetime.now(cl.US_EASTERN).date()
    
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
            
            # --- Use cached session for UI harvest ---
            get_cached_capital_session()
            
            final_df, report_df = cl.run_harvest_logic(selected_tickers, target_date, db_map, logger, harvest_mode)
            
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
                    st.warning(f"**Fallback Alert:** {', '.join(fallback_tickers)} used Capital fallback (volume may be inaccurate).", icon="📡")

    if st.session_state.get('harvest_report') is not None:
        st.divider()
        col_report, col_viz = st.columns([1, 1])
        report_df = st.session_state['harvest_report']
        final_df = st.session_state.get('harvested_data')
        target_date_obj = st.session_state.get('harvest_target_date', datetime.now(cl.US_EASTERN).date())
        
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
                        if cl.save_data_to_turso(final_df, logger=None, client=get_cached_db_connection()):
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
    with st.container(border=True):
        st.write("### ➕ Add New Symbol")
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1: new_ticker = st.text_input("Ticker", placeholder="e.g. AAPL").upper()
        with c2: new_epic = st.text_input("Epic", placeholder="e.g. AAPL").upper()
        with c3: new_strat = st.selectbox("Strategy", ["HYBRID (Stock/ETF)", "CAPITAL_ONLY (Index/CFD)"], key="add_strat")
        if st.button("Save New Symbol", type="primary") and new_ticker:
            code = "CAPITAL_ONLY" if "CAPITAL" in new_strat else "HYBRID"
            epic_val = new_epic if new_epic else new_ticker
            if cl.upsert_symbol_mapping(new_ticker, epic_val, code):
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
                    cl.delete_symbol_mapping(original_ticker)
                if new_ticker_val:
                    if cl.upsert_symbol_mapping(new_ticker_val, new_epic_val, code):
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
            cl.delete_symbol_mapping(d_t); st.success(f"Deleted {d_t}"); time.sleep(0.5); st.rerun()

def render_health_dashboard(inventory_list):
    st.subheader("🗓️ Data Health Dashboard")
    st.info("Check the completeness of your data library. Cells show the number of candles collected.")
    
    today = datetime.now(cl.US_EASTERN).date()
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
            health_pivot_df = cl.fetch_data_health_matrix(selected_tickers, start_date, end_date, session_filter)
            
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
                
                num_rows = len(health_pivot_df)
                dynamic_height = (num_rows + 1) * 35 
                
                st.dataframe(
                    health_pivot_df.style.apply(lambda x: x.map(lambda val: style_heatmap(val, mode=session_filter))).format("{:.0f}", na_rep=""), 
                    use_container_width=True, height=dynamic_height
                )
            else:
                st.warning("No data found for the selected symbols and date range.")

# --- Main App ---
def main():
    st.set_page_config(page_title="Market Data Harvester", layout="wide")
    
    # --- UI uses CACHED connection ---
    db_conn = get_cached_db_connection()
    if db_conn:
        cl.init_db(client=db_conn)
    
    with st.sidebar:
        st.title("🦁 Market Lion")
        app_mode = st.selectbox("Select App Mode", ["⚙️ Inventory Manager", "🌱 Data Harvester", "🗓️ Data Health Dashboard"])
        st.divider()
    
    db_map = cl.get_symbol_map_from_db(client=db_conn)
    inventory_list = list(db_map.keys())

    if app_mode == "⚙️ Inventory Manager":
        render_inventory_ui(db_map, inventory_list)
    elif app_mode == "🌱 Data Harvester":
        render_harvester_ui(inventory_list, db_map)
    elif app_mode == "🗓️ Data Health Dashboard":
        render_health_dashboard(inventory_list)

if __name__ == "__main__":
    main()