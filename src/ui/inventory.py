"""
UI component for inventory management.
Auto-detects routing strategy and clarifies Fallback IDs.
"""
import streamlit as st
import pandas as pd
import time
from src.database.operations import upsert_symbol_mapping, delete_symbol_mapping






def render_inventory_ui(db_map, inventory_list):
    """Renders the inventory manager UI section."""
    st.subheader("📦 Inventory Manager")
    
    # Options
    SOURCE_OPTIONS = ["YAHOO", "MASSIVE", "BINANCE", "TWELVE_DATA"]
    
    # --- SECTION 1: ADD NEW SYMBOL ---
    with st.container(border=True):
        st.write("### ➕ Add New Symbol")
        
        display_name = st.text_input("Display Name (Unique ID)", placeholder="e.g. BTCUSDT, AAPL, Gold").upper().strip()
        
        with st.expander("🔌 Source Configuration", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                y_ticker = st.text_input("Yahoo Ticker", value=display_name, help="e.g. BTC-USD, AAPL")
            with c2:
                m_ticker = st.text_input("Massive Ticker", value=display_name, help="e.g. AAPL")
            with c3:
                b_ticker = st.text_input("Binance Ticker", value=display_name if "USDT" in display_name else "", help="e.g. BTCUSDT")
            with c4:
                td_ticker = st.text_input("TwelveData Ticker", value=display_name, help="e.g. AAPL, WTI/USD")

            st.caption("ℹ️ *Default values assume the source uses the same ticker name. Adjust if different.*")
            
        with st.expander("⚡ Priority Routing", expanded=True):
            pc1, pc2, pc3 = st.columns(3)
            with pc1:
                p1 = st.selectbox("Priority 1 (Primary)", SOURCE_OPTIONS, index=0)
            with pc2:
                # Options: NONE, YAHOO, MASSIVE, BINANCE, TWELVE_DATA
                # Index 1 = YAHOO
                p2 = st.selectbox("Priority 2 (Fallback)", ["NONE"] + SOURCE_OPTIONS, index=1)
            with pc3:
                # Index 0 = NONE
                p3 = st.selectbox("Priority 3 (Last Resort)", ["NONE"] + SOURCE_OPTIONS, index=0)
        
        if st.button("Save New Symbol", type="primary", disabled=not display_name):
            if upsert_symbol_mapping(display_name, y_ticker, m_ticker, b_ticker, p1, p2, td_ticker, p3):
                st.success(f"Saved {display_name}")
                time.sleep(0.5)
                st.rerun()

    # --- SECTION 2: EDIT EXISTING ---
    with st.container(border=True):
        st.write("### ⚡ Edit Existing Symbol")
        if not inventory_list:
            st.info("No symbols in inventory yet.")
        else:
            if 'edit_select' not in st.session_state: st.session_state.edit_select = "" 
            
            # Selection Dropdown
            selected_ticker = st.selectbox("Select Symbol to Edit", [""] + inventory_list, key="edit_select")
            
            if selected_ticker and selected_ticker in db_map:
                data = db_map[selected_ticker]
                
                st.write(f"**Editing: {selected_ticker}**")
                
                with st.expander("🔌 Source Configuration", expanded=True):
                    ec1, ec2, ec3, ec4 = st.columns(4)
                    with ec1:
                        ny_ticker = st.text_input("Yahoo Ticker", value=data['yahoo_ticker'] or "", key="e_y")
                    with ec2:
                        nm_ticker = st.text_input("Massive Ticker", value=data['massive_ticker'] or "", key="e_m")
                    with ec3:
                        nb_ticker = st.text_input("Binance Ticker", value=data['binance_ticker'] or "", key="e_b")
                    with ec4:
                        ntd_ticker = st.text_input("TwelveData Ticker", value=data.get('twelve_data_ticker') or "", key="e_td")
                
                with st.expander("⚡ Priority Routing", expanded=True):
                    epc1, epc2, epc3 = st.columns(3)
                    with epc1:
                        curr_p1 = data['p1'] if data['p1'] in SOURCE_OPTIONS else "YAHOO"
                        np1 = st.selectbox("Priority 1", SOURCE_OPTIONS, index=SOURCE_OPTIONS.index(curr_p1), key="e_p1")
                    with epc2:
                        curr_p2 = data['p2'] if data['p2'] in ["NONE"] + SOURCE_OPTIONS else "NONE"
                        np2 = st.selectbox("Priority 2", ["NONE"] + SOURCE_OPTIONS, index=(["NONE"] + SOURCE_OPTIONS).index(curr_p2), key="e_p2")
                    with epc3:
                        opts = ["NONE"] + SOURCE_OPTIONS
                        curr_p3 = data.get('p3')
                        if curr_p3 not in opts: curr_p3 = "NONE"
                        np3 = st.selectbox("Priority 3", opts, index=opts.index(curr_p3), key="e_p3")

                if st.button("Update Symbol", type="primary"):
                    if upsert_symbol_mapping(selected_ticker, ny_ticker, nm_ticker, nb_ticker, np1, np2, ntd_ticker, np3):
                        st.success(f"Updated {selected_ticker}")
                        time.sleep(0.5)
                        st.rerun()

    # --- SECTION 3: TABLE VIEW ---
    st.write("### 📋 Current Inventory")
    if db_map:
        table_data = []
        for k, v in db_map.items():
            table_data.append({
                "Display Name": k, 
                "P1": v['p1'],
                "P2": v['p2'],
                "P3": v.get('p3', 'NONE'),
                "Yahoo": v['yahoo_ticker'],
                "Massive": v['massive_ticker'],
                "Binance": v['binance_ticker'],
                "TwelveData": v.get('twelve_data_ticker')
            })
            
        st.dataframe(pd.DataFrame(table_data), use_container_width=True)
        
        st.write("#### 🗑️ Delete Symbol")
        c_del1, c_del2 = st.columns([3, 1])
        with c_del1:
            d_t = st.selectbox("Select Symbol to Delete", [""] + inventory_list, key="del_select")
        with c_del2:
            st.write("")
            st.write("")
            if st.button("Confirm Delete", type="primary", disabled=(not d_t)):
                delete_symbol_mapping(d_t)
                st.success(f"Deleted {d_t}")
                time.sleep(0.5)
                st.rerun()