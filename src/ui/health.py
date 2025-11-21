"""
Data Health Dashboard: Visualizes Data Completeness (Green/Red Matrix).
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from src.database.operations import fetch_data_health_matrix
from src.config import US_EASTERN

def render_health_dashboard(inventory_list):
    """Renders the data health dashboard UI section."""
    st.subheader("🗓️ Data Health Dashboard")
    st.info("Check the completeness of your data library. Cells show the number of candles collected.")
    
    # Layout for session selection and weekend toggle
    c1, c2 = st.columns([3, 1])
    
    with c1:
        session_mode = st.radio(
            "Select Session to Inspect",
            ["Full Day (Total)", "🌙 Pre-Market", "☀️ Regular Session", "🌆 Post-Market"],
            horizontal=True
        )
    
    with c2:
        # Spacer to align checkbox with radio buttons visually
        st.write("") 
        st.write("")
        hide_weekends = st.checkbox(
            "Hide Weekends", 
            value=True, 
            help="Hides Saturday and Sunday columns to clean up the view for stocks."
        )

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
        with st.spinner(f"Querying {session_mode} data health for {selected_month} {selected_year}..."):
            health_pivot_df = fetch_data_health_matrix(selected_tickers, start_date, end_date, session_filter)
            
            if not health_pivot_df.empty:
                
                # --- LOGIC: Hide Weekends ---
                if hide_weekends:
                    # Filter columns where the date is Mon(0) - Fri(4)
                    # health_pivot_df columns are date objects (YYYY-MM-DD)
                    valid_cols = [
                        c for c in health_pivot_df.columns 
                        if pd.to_datetime(c).weekday() < 5
                    ]
                    health_pivot_df = health_pivot_df[valid_cols]
                    
                    if health_pivot_df.empty:
                        st.warning("No weekday data found for this period.")
                        return

                def style_heatmap(val):
                    mode = session_filter
                    if pd.isna(val):
                        return 'background-color: #262626; color: #262626' # Dark gray for empty

                    # Color Logic
                    if mode == "Total":
                        if val > 900: return 'background-color: #285E28; color: white'      # Green
                        elif val > 700: return 'background-color: #5E5B28; color: white'    # Yellow-ish
                        elif val > 600: return 'background-color: #5E4228; color: white'    # Orange-ish
                    elif mode == "PRE":
                        if val > 300: return 'background-color: #285E28; color: white'
                        elif val > 100: return 'background-color: #5E5B28; color: white'
                    elif mode == "REG":
                        if val > 350: return 'background-color: #285E28; color: white'
                        elif val > 100: return 'background-color: #5E5B28; color: white'
                    elif mode == "POST":
                         if val > 200: return 'background-color: #285E28; color: white'
                         elif val > 50: return 'background-color: #5E5B28; color: white'

                    return 'background-color: #5E2828; color: white' # Red for low data

                tight_height = (len(health_pivot_df) + 1) * 35 + 3

                st.dataframe(
                    health_pivot_df.style.map(style_heatmap).format("{:.0f}", na_rep=""),
                    use_container_width=True, 
                    height=tight_height
                )
            else:
                st.warning("No data found for the selected symbols and date range.")