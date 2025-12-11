import streamlit as st
from src.database.operations import get_symbol_map_from_db

def render_sidebar():
    """
    Renders the shared sidebar content for the application.
    Displays the count of tracked symbols.
    """
    with st.sidebar:
        st.divider()
        try:
            db_map = get_symbol_map_from_db()
            count = len(db_map)
            st.info(f"Tracking **{count}** Symbols")
        except Exception:
            st.error("DB Error")
