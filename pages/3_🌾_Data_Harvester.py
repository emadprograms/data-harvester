import streamlit as st
from src.database.operations import get_symbol_map_from_db
# FIXED: Use the correct UI module
from src.ui.harvester_ui import render_harvester_ui
from src.ui.sidebar import render_sidebar

st.set_page_config(page_title="Data Harvester", page_icon="🌾", layout="wide")

st.title("🌾 Data Harvester")

# Render Sidebar
render_sidebar()

db_map = get_symbol_map_from_db()
inventory_list = sorted(list(db_map.keys()))

render_harvester_ui(db_map, inventory_list)
