import streamlit as st
from src.database.operations import get_symbol_map_from_db
from src.ui.health import render_health_dashboard

st.set_page_config(
    page_title="Data Health | Harvester", 
    page_icon="🏥", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🏥 Data Health Dashboard")

# Initialize DB connection implicitly handled by operations if needed, 
# but mostly we just need the symbol map here.
db_map = get_symbol_map_from_db()
inventory_list = sorted(list(db_map.keys()))

render_health_dashboard(inventory_list)
