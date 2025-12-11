import streamlit as st
from src.database.schema import init_db

# Page Config
# This serves as the "Home" page in the Multipage App structure.
st.set_page_config(page_title="Market Data Harvester", layout="wide", page_icon="🦁")

def main():
    # Initialize DB (Ensure tables exist)
    init_db()
    
    st.title("🦁 Market Data Harvester")
    st.markdown("""
    ### Welcome!
    This application is an advanced market data collection engine designed to harvest **1-minute OHLCV candles** from multiple sources and store them in a **Turso (libSQL)** database.
    
    It solves the problem of getting reliable, gap-free data by using a **Hybrid Harvesting Strategy** split into three distinct "Lanes".
    
    ---
    
    ### 🛣️ The Three Lanes (Routing Logic)
    
    #### 🟢 Lane 1: Crypto (Binance)
    *   **Target:** Symbols ending in `USDT` (e.g., `BTCUSDT`, `ETHUSDT`).
    *   **Primary Source:** **Binance**. Fetches full 24h data.
    *   **🛡️ Fallback (New!):** If Binance fails (e.g., due to US region restrictions for pairs like `EURUSDT`), the system automatically switches to **Yahoo Finance**.
        *   *Smart Mapping:* `EURUSDT` ➔ `EURUSD=X` (Forex), `BTCUSDT` ➔ `BTC-USD` (Crypto).
        
    #### 🟡 Lane 2: Commodities (Futures)
    *   **Target:** Symbols ending in `=F` (e.g., `CL=F` for Oil, `GC=F` for Gold).
    *   **Primary Source:** **Yahoo Finance**. Fetches 24h futures data.
    *   **Fallback:** Capital.com.
    
    #### 🔵 Lane 3: Stocks & Indices (Hybrid)
    *   **Target:** Everything else (e.g., `AAPL`, `NVDA`, `SPY`, `VIX`).
    *   **Strategy:** "Frankenstein" Candle Stitching.
        *   **Pre-Market (04:00 - 09:30 ET):** Fetched from **Capital.com** (CFDs often trade 24/5).
        *   **Regular Session (09:30 - 16:00 ET):** Fetched from **Yahoo Finance** (Official exchange data).
        *   **Post-Market (16:00 - 20:00 ET):** Fetched from **Capital.com**.
        
    ---
    
    ### 🧩 App Components
    
    *   **🌾 Data Harvester:** The engine room. Select tickers and click "Start" to fetch data for a specific date. The "Glass Box" UI shows you exactly which source is being used for each session.
    *   **📦 Inventory Manager:** Your control center. Add new symbols here. You MUST map every ticker to a **Capital.com Epic** (Fallback ID) so the system knows what to search for if the primary source fails.
    *   **🏥 Data Health:** A calendar heat-map showing data completeness. Green days = 1440 mins (24h) or 960 mins (16h Stock). Red days = Missing data.
    *   **🔎 DB Inspector:** A raw data viewer to peek inside the database tables.
    
    ### 🚀 Getting Started
    1. Go to **Inventory Manager** and add your favorite tickers.
    2. Go to **Data Harvester**, select "Full Day", and hit "Run".
    3. Commit the data to the database!
    """)
    
    st.divider()
    st.info("� **Select a module from the sidebar to get started!**")

if __name__ == "__main__":
    main()