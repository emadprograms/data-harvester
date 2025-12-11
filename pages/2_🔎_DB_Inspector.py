import streamlit as st
from src.database.operations import get_symbol_map_from_db
from src.ui.inspector import render_inspector_ui
from src.ui.sidebar import render_sidebar

st.set_page_config(page_title="DB Inspector | Harvester", page_icon="🔎", layout="wide")

st.title("🔎 Database Inspector")

# Render Sidebar
render_sidebar()

db_map = get_symbol_map_from_db()
inventory_list = sorted(list(db_map.keys()))

render_inspector_ui(inventory_list)
