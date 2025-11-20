"""
Main Streamlit application entry point for Market Data Harvester.
"""
import streamlit as st
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db
from src.ui.inventory import render_inventory_ui
from src.ui.harvester_ui import render_harvester_ui
from src.ui.health import render_health_dashboard

st.set_page_config(page_title='Market Data Harvester', layout='wide')

# Initialize database
init_db()

# Sidebar navigation
with st.sidebar:
    st.title('🦁 Market Lion')
    app_mode = st.selectbox(
        'Select App Mode',
        ['🌱 Data Harvester', '⚙️ Inventory Manager', '🗓️ Data Health Dashboard']
    )
    st.divider()

# Get inventory
db_map = get_symbol_map_from_db()
inventory_list = list(db_map.keys())

# Route to appropriate UI
if app_mode == '⚙️ Inventory Manager':
    render_inventory_ui(db_map, inventory_list)
elif app_mode == '🌱 Data Harvester':
    render_harvester_ui(inventory_list, db_map)
elif app_mode == '🗓️ Data Health Dashboard':
    render_health_dashboard(inventory_list)
