"""
UI component for data harvesting.
"""
import streamlit as st
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
    
    c1, c2 = st.columns([1, 2])
    with c1:
        harvest_mode = st.radio(
    "Harvest Mode", 
    ["🚀 Full Day", "🌙 Pre-Market Only", "☀️ Regular Session Only", "🌆 Post-Market Only"]
)

        target_date = st.date_input("Target Date", st.session_state['harvest_target_date'])
    with c2:
        st.write("**Select Symbols to Harvest**")
        selected_tickers = st.multiselect(
            "Tickers", 
            options=inventory_list, 
            default=inventory_list[:2] if inventory_list else None, 
            label_visibility="collapsed"
        )
        st.caption(f"Selected: {len(selected_tickers)}")
        
        if st.button("Start Harvest", type="primary", disabled=(len(selected_tickers) == 0)):
            status_container = st.status("Harvesting Data...", expanded=True)
            logger = StreamlitLogger(status_container)
            
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
