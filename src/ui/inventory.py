"""
UI component for inventory management.
Auto-detects routing strategy and clarifies Fallback IDs.
"""
import streamlit as st
import pandas as pd
import time
from src.database.operations import upsert_symbol_mapping, delete_symbol_mapping


def get_route_description(ticker):
    """
    Returns a user-friendly description of the Harvester Lane based on the ticker symbol.
    """
    if not ticker:
        return "Waiting for input..."
    
    t = ticker.upper().strip()
    
    # Lane 1: Binance
    if t.endswith("USDT"):
        return "🟢 **Lane 1 (Binance):** Will fetch 24h Crypto/Forex data from Binance. (Fallback: Capital)"
    
    # Lane 2: Yahoo Futures
    if t.endswith("=F"):
        return "🟡 **Lane 2 (Futures):** Will fetch 24h Futures data from Yahoo. (Fallback: Capital)"
    
    # Lane 3: Standard Stocks/Indices (Includes VIX)
    return "🔵 **Lane 3 (Stocks/Indices):** Will fetch Pre/Reg/Post data from Yahoo. (Fallback: Capital)"


def render_inventory_ui(db_map, inventory_list):
    """Renders the inventory manager UI section."""
    st.subheader("📦 Inventory Manager")
    
    # --- SECTION 1: ADD NEW SYMBOL ---
    with st.container(border=True):
        st.write("### ➕ Add New Symbol")
        c1, c2 = st.columns(2)
        with c1:
            new_ticker = st.text_input("Ticker", placeholder="e.g. BTCUSDT, AAPL, CL=F").upper().strip()
        with c2:
            # Added explicit Label and Help Tooltip
            new_epic = st.text_input(
                "Capital.com Epic (Fallback ID)", 
                placeholder="e.g. BTCUSD, AAPL, US500", 
                help="⚠️ REQUIRED FOR SAFETY NET: If Yahoo/Binance fails, the system uses THIS specific symbol to fetch data from Capital.com."
            ).upper().strip()
        
        # Dynamic Route Display
        if new_ticker:
            route_msg = get_route_description(new_ticker)
            st.info(route_msg)
        
        # Optional override
        force_capital = st.checkbox("🚫 Skip Primary Source (Force Capital.com Only)", key="new_force_cap")
        
        if st.button("Save New Symbol", type="primary", disabled=not new_ticker):
            code = "CAPITAL_ONLY" if force_capital else "HYBRID"
            epic_val = new_epic if new_epic else new_ticker
            
            if upsert_symbol_mapping(new_ticker, epic_val, code):
                st.success(f"Saved {new_ticker}")
                time.sleep(0.5)
                st.rerun()

    # --- SECTION 2: EDIT EXISTING ---
    with st.container(border=True):
        st.write("### ⚡ Edit Existing Symbol")
        if not inventory_list:
            st.info("No symbols in inventory yet.")
        else:
            if 'edit_select' not in st.session_state:
                st.session_state.edit_select = "" 
            if 'edit_ticker_val' not in st.session_state:
                st.session_state.edit_ticker_val = ""
            if 'edit_epic_val' not in st.session_state:
                st.session_state.edit_epic_val = ""
            if 'edit_force_cap' not in st.session_state:
                st.session_state.edit_force_cap = False

            def handle_update():
                original_ticker = st.session_state.edit_select
                new_ticker_val = st.session_state.edit_ticker_val
                new_epic_val = st.session_state.edit_epic_val
                is_forced = st.session_state.edit_force_cap
                
                code = "CAPITAL_ONLY" if is_forced else "HYBRID"
                
                if original_ticker and new_ticker_val and original_ticker != new_ticker_val:
                    st.info(f"Renaming {original_ticker} to {new_ticker_val}...")
                    delete_symbol_mapping(original_ticker)
                
                if new_ticker_val:
                    if upsert_symbol_mapping(new_ticker_val, new_epic_val, code):
                        st.success(f"Updated {new_ticker_val}")
                        # Reset state
                        st.session_state.edit_select = ""
                        st.session_state.edit_ticker_val = "" 
                        st.session_state.edit_epic_val = "" 
                        st.session_state.edit_force_cap = False
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("Failed to update symbol.")
                else:
                    st.error("Ticker field cannot be empty.")

            # Selection Dropdown
            c_edit1, c_edit_spacer = st.columns([1.5, 2.5])
            with c_edit1:
                st.selectbox("Select Ticker to Edit", options=[""] + inventory_list, key="edit_select")
            
            # Populate fields on selection
            current_selection = st.session_state.edit_select
            if current_selection != st.session_state.edit_ticker_val and current_selection != "":
                if current_selection in db_map:
                    selected_data = db_map[current_selection]
                    st.session_state.edit_ticker_val = current_selection
                    st.session_state.edit_epic_val = selected_data['epic']
                    st.session_state.edit_force_cap = (selected_data['strategy'] == "CAPITAL_ONLY")
            
            # Edit Form
            if current_selection:
                c_f1, c_f2 = st.columns(2)
                with c_f1:
                    st.text_input("Ticker", key="edit_ticker_val")
                with c_f2:
                    # Added explicit Label and Help Tooltip
                    st.text_input(
                        "Capital.com Epic (Fallback ID)", 
                        key="edit_epic_val",
                        help="The specific symbol Capital.com uses. If Yahoo fails, we need THIS to fetch the data from Capital."
                    )
                
                # Dynamic Route Display for Edit Mode
                if st.session_state.edit_ticker_val:
                    st.caption(get_route_description(st.session_state.edit_ticker_val))
                
                st.checkbox("🚫 Skip Primary Source (Force Capital.com Only)", key="edit_force_cap")
                
                st.write("")
                st.button("Update Symbol", type="primary", on_click=handle_update)

    # --- SECTION 3: TABLE VIEW ---
    st.write("### 📋 Current Inventory")
    if db_map:
        table_data = []
        for k, v in db_map.items():
            strat_raw = v['strategy']
            if strat_raw == "CAPITAL_ONLY":
                display_strat = "🚫 Capital Only"
            else:
                # Quick lookup for display
                if k.endswith("USDT"): display_strat = "Binance ➔ Cap"
                elif k.endswith("=F"): display_strat = "Yahoo Fut ➔ Cap"
                else: display_strat = "Yahoo Stk ➔ Cap"
                
            table_data.append({
                "Ticker": k, 
                "Fallback ID (Capital)": v['epic'], 
                "Effective Route": display_strat
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