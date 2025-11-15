"""
UI component for data health dashboard.
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from src.config import US_EASTERN
from src.database.operations import fetch_data_health_matrix


def render_health_dashboard(inventory_list):
    """Renders the data health dashboard UI section."""
    st.subheader("🗓️ Data Health Dashboard")
    st.info("Check the completeness of your data library. Cells show the number of candles collected.")
    
    session_mode = st.radio(
    "Select Session to Inspect", 
    ["Full Day (Total)", "🌙 Pre-Market", "☀️ Regular Session", "🌆 Post-Market"], 
    horizontal=True
)

    # Later in the code:
    if session_mode == "🌙 Pre-Market":
        session_filter = "PRE"
    elif session_mode == "☀️ Regular Session":
        session_filter = "REG"
    elif session_mode == "🌆 Post-Market":
        session_filter = "POST"
    else:
        session_filter = "Total"


    today = datetime.now(US_EASTERN).date()
    
    col_month, col_year = st.columns(2)
    with col_month:
        month_names = ["January", "February", "March", "April", "May", "June", 
                       "July", "August", "September", "October", "November", "December"]
        selected_month = st.selectbox("Month", month_names, index=today.month - 1)
    
    with col_year:
        years = [today.year, today.year - 1]
        selected_year = st.selectbox("Year", years, index=0)

    month_idx = month_names.index(selected_month) + 1
    start_date = datetime(selected_year, month_idx, 1).date()
    
    if month_idx == 12:
        end_date = datetime(selected_year + 1, 1, 1).date() - timedelta(days=1)
    else:
        end_date = datetime(selected_year, month_idx + 1, 1).date() - timedelta(days=1)
    
    selected_tickers = st.multiselect("Select Symbols", inventory_list, default=inventory_list)
    
    if st.button("🔍 Generate Health Report", type="primary") and selected_tickers:
        if session_mode == "🌙 Pre-Market":
            session_filter = "PRE"
        elif session_mode == "☀️ Regular Session":
            session_filter = "REG"
        else:
            session_filter = "Total"
            
        with st.spinner(f"Querying {session_mode} data health for {selected_month} {selected_year}..."):
            health_pivot_df = fetch_data_health_matrix(selected_tickers, start_date, end_date, session_filter)
            
            if not health_pivot_df.empty:
                def style_heatmap(val, mode="Total"):
                    if pd.isna(val):
                        return 'background-color: #262626'
                    if mode == "Total":
                        if val > 900: return 'background-color: #285E28'      # green (over 900/960 candles)
                        elif val > 700: return 'background-color: #5E5B28'    # yellow
                        elif val > 600: return 'background-color: #5E4228'    # orange-brown
                    elif mode == "PRE":
                        if val > 300:
                            return 'background-color: #285E28'
                        elif val > 100:
                            return 'background-color: #5E5B28'
                    elif mode == "REG":
                        if val > 350:
                            return 'background-color: #285E28'
                        elif val > 100:
                            return 'background-color: #5E5B28'
                    return 'background-color: #5E2828'

                tight_height = (len(health_pivot_df) + 1) * 35 + 3

                st.dataframe(
                    health_pivot_df.style.apply(
                        lambda x: x.map(lambda val: style_heatmap(val, mode=session_filter))
                    ).format("{:.0f}", na_rep=""), 
                    use_container_width=True, 
                    height=tight_height
                )
            else:
                st.warning("No data found for the selected symbols and date range.")
