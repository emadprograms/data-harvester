"""
UI component for inventory management.
"""
import streamlit as st
import pandas as pd
import time
from src.database.operations import upsert_symbol_mapping, delete_symbol_mapping


def render_inventory_ui(db_map, inventory_list):
    """Renders the inventory manager UI section."""
    st.subheader("📦 Inventory Manager")
    
    with st.container(border=True):
        st.write("### ➕ Add New Symbol")
        c1, c2, c3 = st.columns([2, 2, 2])
        with c1:
            new_ticker = st.text_input("Ticker", placeholder="e.g. AAPL").upper()
        with c2:
            new_epic = st.text_input("Epic", placeholder="e.g. AAPL").upper()
        with c3:
            new_strat = st.selectbox("Strategy", ["HYBRID (Stock/ETF)", "CAPITAL_ONLY (Index/CFD)"], key="add_strat")
        
        if st.button("Save New Symbol", type="primary") and new_ticker:
            code = "CAPITAL_ONLY" if "CAPITAL" in new_strat else "HYBRID"
            epic_val = new_epic if new_epic else new_ticker
            if upsert_symbol_mapping(new_ticker, epic_val, code):
                st.success(f"Saved {new_ticker}")
                time.sleep(0.5)
                st.rerun()

    with st.container(border=True):
        st.write("### ⚡ Edit Existing Symbol")
        if not inventory_list:
            st.info("No symbols in inventory yet.")
        else:
            STRAT_HYBRID = "HYBRID (Stock/ETF)"
            STRAT_CAPITAL = "CAPITAL_ONLY (Index/CFD)"
            STRAT_OPTIONS = [STRAT_HYBRID, STRAT_CAPITAL]
            
            if 'edit_select' not in st.session_state:
                st.session_state.edit_select = "" 
            if 'edit_ticker_val' not in st.session_state:
                st.session_state.edit_ticker_val = ""
            if 'edit_epic_val' not in st.session_state:
                st.session_state.edit_epic_val = ""
            if 'edit_strat_sel' not in st.session_state:
                st.session_state.edit_strat_sel = STRAT_HYBRID

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
                    else:
                        st.error("Failed to update symbol.")
                else:
                    st.error("Ticker field cannot be empty.")

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
            with c_edit_fields1:
                st.text_input("Ticker (Yahoo/PK)", key="edit_ticker_val")
            with c_edit_fields2:
                st.text_input("Epic (Capital)", key="edit_epic_val")
            with c_edit_fields3:
                st.selectbox("Strategy", STRAT_OPTIONS, key="edit_strat_sel")
            with c_edit_fields4:
                st.write("")
                st.write("")
                is_disabled = (st.session_state.edit_select == "")
                st.button("Update Symbol", disabled=is_disabled, on_click=handle_update)
    
    st.write("### 📋 Current Inventory")
    if db_map:
        data = [{"Ticker": k, "Epic": v['epic'], "Strategy": v['strategy']} for k, v in db_map.items()]
        st.dataframe(pd.DataFrame(data), use_container_width=True)
        
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
