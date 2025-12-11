import streamlit as st
from src.database.operations import get_symbol_map_from_db
from src.ui.inventory import render_inventory_ui

st.set_page_config(
    page_title="Inventory Manager | Harvester", 
    page_icon="📦", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📦 Inventory Manager")

db_map = get_symbol_map_from_db()
inventory_list = sorted(list(db_map.keys()))

render_inventory_ui(db_map, inventory_list)
