import streamlit as st
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db

# --- UI Modules ---
# FIXED: Changed from src.data.harvester to src.ui.harvester_ui
from src.ui.harvester_ui import render_harvester_ui 
from src.ui.inventory import render_inventory_ui
from src.ui.health import render_health_dashboard
from src.ui.inspector import render_inspector_ui

# Page Config
st.set_page_config(page_title="Market Data Harvester", layout="wide", page_icon="📈")

def main():
    # Initialize DB
    init_db()
    
    # Fetch Global State
    db_map = get_symbol_map_from_db()
    inventory_list = sorted(list(db_map.keys()))

    # --- Sidebar Navigation ---
    with st.sidebar:
        st.title("🚀 Harvester")
        
        app_mode = st.radio("Navigation", [
            "🏥 Data Health",        # Shows the Green/Red Matrix (using health.py)
            "🔎 DB Inspector",       # Shows raw data for debugging (using inspector.py)
            "🌾 Data Harvester",     
            "📦 Inventory Manager"   
        ])
        
        st.divider()
        st.info(f"Tracking **{len(inventory_list)}** Symbols")

    # --- Page Routing ---
    if app_mode == "🏥 Data Health":
        render_health_dashboard(inventory_list)

    elif app_mode == "🔎 DB Inspector":
        render_inspector_ui(inventory_list)

    elif app_mode == "🌾 Data Harvester":
        render_harvester_ui(db_map, inventory_list)

    elif app_mode == "📦 Inventory Manager":
        render_inventory_ui(db_map, inventory_list)

if __name__ == "__main__":
    main()