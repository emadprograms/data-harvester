"""
UI component for data harvesting.
"""
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from src.config import US_EASTERN
from src.data.harvester import run_harvest_logic
from src.database.operations import save_data_to_turso
from src.utils.logger import StreamlitLogger


def render_harvester_ui(inventory_list, db_map):
    """Renders the data harvester UI section."""
    st.subheader("🌱 Data Harvester")
    
    if 'harvest_report' not in st.session_state:
        st.session_state['harvest_report'] = None
    if 'harvested_data' not in st.session_state:
        st.session_state['harvested_data'] = None
    if 'harvest_target_date' not in st.session_state:
        st.session_state['harvest_target_date'] = datetime.now(US_EASTERN).date()
    
    # Settings Expander
    with st.expander("⚙️ Harvest Settings", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
             harvest_mode = st.radio(
                "Harvest Mode",
                ["🚀 Full Day", "🌙 Pre-Market Only", "☀️ Regular Session Only", "🌆 Post-Market Only"]
            )
        with c2:
             target_date = st.date_input("Target Date", st.session_state['harvest_target_date'])

    st.write("**Select Symbols to Harvest**")

    # Select All / Deselect All Helper
    col_sel1, col_sel2 = st.columns([1, 4])
    with col_sel1:
        select_all = st.checkbox("Select All", value=True)

    default_selection = inventory_list if select_all else []

    selected_tickers = st.multiselect(
        "Tickers",
        options=inventory_list,
        default=default_selection,
        label_visibility="collapsed"
    )
    st.caption(f"Selected: {len(selected_tickers)}")

    if st.button("Start Harvest", type="primary", disabled=(len(selected_tickers) == 0)):
        # --- Initialize UI Elements ---
        st.divider()
        st.write("### 🔄 Harvest Progress")
        
        if st.button("Start Harvest", type="primary", disabled=(len(selected_tickers) == 0)):
            # --- Initialize UI Elements ---
            st.divider()
            st.write("### 🔄 Harvest Progress")
            
            prog_bar = st.progress(0, text="Starting...")
            status_text = st.empty()
            
            # Create initial DataFrame for the matrix
            initial_data = {
                "Ticker": selected_tickers,
                "Pre-Market": ["⏳"] * len(selected_tickers),
                "Regular Session": ["⏳"] * len(selected_tickers),
                "Post-Market": ["⏳"] * len(selected_tickers),
                "Total Rows": [0] * len(selected_tickers),
                "Status": ["Pending"] * len(selected_tickers)
            }
            status_df = pd.DataFrame(initial_data).set_index("Ticker")
            table_placeholder = st.empty()
            table_placeholder.dataframe(status_df, use_container_width=True)

            # Callback to update UI from inside the logic
            def update_ui_callback(ticker, col, val):
                if col == "PROG":
                    # value is (current, total, message)
                    curr, total, msg = val
                    pct = min(curr / total, 1.0)
                    prog_bar.progress(pct, text=msg)
                    return

                # Update DataFrame
                if ticker in status_df.index:
                    status_df.at[ticker, col] = val
                    table_placeholder.dataframe(status_df, use_container_width=True)

                    # Also update text status
                    status_text.markdown(f"**Processing {ticker}:** {col} -> {val}")

            # Run Logic
            # We still use a dummy logger just in case, but rely on callback for UI
            # StreamlitLogger handles None container gracefully by just printing
            logger = StreamlitLogger(None)

            final_df, report_df = run_harvest_logic(
                selected_tickers,
                target_date,
                db_map,
                logger,
                harvest_mode,
                progress_callback=update_ui_callback
            )

            prog_bar.progress(1.0, text="✅ Harvest Complete!")
            status_text.markdown("✅ **All Tasks Completed.**")
            
            st.session_state['harvest_report'] = report_df
            st.session_state['harvest_target_date'] = target_date 
            
            if not final_df.empty:
                st.session_state['harvested_data'] = final_df
            else:
                st.session_state['harvested_data'] = None
                st.warning("No data collected.")
            
            # Update DataFrame
            if ticker in status_df.index:
                status_df.at[ticker, col] = val
                table_placeholder.dataframe(status_df, use_container_width=True)

                # Also update text status
                status_text.markdown(f"**Processing {ticker}:** {col} -> {val}")

        # Run Logic
        # We still use a dummy logger just in case, but rely on callback for UI
        # StreamlitLogger handles None container gracefully by just printing
        logger = StreamlitLogger(None)

        final_df, report_df = run_harvest_logic(
            selected_tickers,
            target_date,
            db_map,
            logger,
            harvest_mode,
            progress_callback=update_ui_callback
        )

        prog_bar.progress(1.0, text="✅ Harvest Complete!")
        status_text.markdown("✅ **All Tasks Completed.**")

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
                st.warning(
                    f"**Fallback Alert:** {', '.join(fallback_tickers)} failed to fetch from Yahoo Finance and used Capital.com as a fallback.",
                    icon="📡"
                )

    if st.session_state.get('harvest_report') is not None:
        st.divider()
        col_report, col_viz = st.columns([1, 1])
        
        report_df = st.session_state['harvest_report']
        final_df = st.session_state.get('harvested_data')
        
        target_date_obj = st.session_state.get('harvest_target_date')
        if not target_date_obj:
            target_date_obj = datetime.now(US_EASTERN).date()
        
        with col_report:
            st.write("### 📋 Harvest Report Card")
            
            if final_df is not None:
                total_rows_collected = len(final_df)
                st.metric("Total Rows Collected", f"{total_rows_collected:,}")
            else:
                st.metric("Total Rows Collected", "0")

            st.dataframe(report_df, use_container_width=True)
            
            if final_df is not None:
                csv_backup = final_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "💾 Download Backup CSV", 
                    csv_backup, 
                    f"backup_{datetime.now().strftime('%Y%m%d')}.csv", 
                    "text/csv"
                )
                
                btn_label = f"☁️ Commit Data for {target_date_obj}"
                if st.button(btn_label, type="primary"):
                    with st.spinner("Saving..."):
                        if save_data_to_turso(final_df):
                            st.success("Saved Successfully!")
                            st.balloons()
        
        with col_viz:
            if final_df is not None:
                st.write("### 👁️ Visual Check")
                t_sel = st.selectbox("Preview Ticker", final_df['symbol'].unique())
                if t_sel:
                    sub = final_df[final_df['symbol'] == t_sel]
                    chart = alt.Chart(sub).mark_line().encode(
                        x='timestamp:T', 
                        y=alt.Y('close:Q', scale=alt.Scale(zero=False)), 
                        color='session:N'
                    ).interactive()
                    st.altair_chart(chart, use_container_width=True)
