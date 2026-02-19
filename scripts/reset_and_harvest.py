"""
Refines ETF Strategy and Resets Data for Feb 19, 2025.
1. Wipes market_data and market_symbols.
2. Reseeds inventory with:
   - ETFs: Capital Only (p1=CAPITAL, p2=NONE)
   - Stocks: Hybrid (p1=YAHOO, p2=CAPITAL)
   - Crypto: Binance (p1=BINANCE, p2=YAHOO)
3. Harvests data for Feb 19, 2025.
"""
import sys
import os
import pandas as pd
from datetime import date
from src.database.connection import get_db_connection
from src.database.schema import init_db
from src.data.harvester import run_harvest_logic
from src.database.operations import save_data_to_turso, get_symbol_map_from_db

from src.utils.logger import CLILogger

def reset_and_harvest():
    print("🧨 STARTING RESET AND HARVEST (Feb 19, 2025) 🧨")
    client = get_db_connection()
    if not client:
        print("❌ DB Connection Failed")
        return

    # 1. WIPE TABLES
    print("🗑️ Wiping tables...")
    try:
        client.execute("DROP TABLE IF EXISTS market_data")
        client.execute("DROP TABLE IF EXISTS market_symbols")
        client.execute("DROP TABLE IF EXISTS symbol_map") # Ensure old table is gone too
        print("✅ Tables wiped.")
    except Exception as e:
        print(f"⚠️ Error wiping tables: {e}")

    # 2. INIT & RESEED
    print("🌱 Re-initializing and Seeding...")
    init_db()
    
    # CLEAR defaults seeded by init_db (we want strictly our new rules)
    print("🧹 Clearing default seed data...")
    client.execute("DELETE FROM market_symbols")
    
    # Define Stocks vs ETFs vs Crypto
    # ETFs (Capital Only)
    etfs = [
        "SPY", "QQQ", "IWM", "DIA", "SMH", "TLT", 
        "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", 
        "UUP", "VIX", "^VIX"
    ]
    
    # Stocks (Hybrid)
    stocks = [
        "AAPL", "ADBE", "AMD", "AMZN", "APP", "AVGO", "BABA", 
        "GOOGL", "LRCX", "META", "MSFT", "MU", "NDAQ", 
        "NVDA", "ORCL", "PANW", "QCOM", "SHOP", "TSLA", "TSM"
    ]
    
    # Crypto (Binance)
    crypto = ["BTCUSDT", "ETHUSDT", "PAXGUSDT"]
    
    # Forex/Futures (Hybrid/Capital)
    futures = ["CL=F", "GC=F", "EURUSDT"]

    # Insert Logic
    for s in stocks:
        client.execute(
            """INSERT INTO market_symbols (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            [s, s, s, None, "YAHOO", "CAPITAL"]
        )

    for e in etfs:
        if e == "^VIX": continue # Skip ^VIX, we handle VIX below
        
        y_tick = e
        c_epic = e
        # Special Handling
        if e == "VIX": 
            y_tick = "^VIX" # Yahoo ticker for VIX
            c_epic = "VIX"  # Capital Epic
        if e == "UUP": c_epic = "DXY"
        if e == "XLC": c_epic = "IUCM"
        
        # Capital Priority, Yahoo Fallback
        client.execute(
            """INSERT INTO market_symbols (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            [e, y_tick, c_epic, None, "CAPITAL", "YAHOO"]
        )

    for c in crypto:
        y_tick = c.replace("USDT", "-USD")
        c_epic = c
        if c == "PAXGUSDT": c_epic = "X:PAXGUSD"
        
        client.execute(
            """INSERT INTO market_symbols (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            [c, y_tick, c_epic, c, "BINANCE", "YAHOO"] # Crypto prefers Binance
        )

    for f in futures:
        if f == "EURUSDT":
            client.execute(
                """INSERT INTO market_symbols (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [f, "EURUSD=X", "X:EURUSD", "EURUSDT", "BINANCE", "YAHOO"]
            )
        else:
            # Future (CL=F, GC=F)
            c_epic = f
            if f == "CL=F": c_epic = "OIL_CRUDE"
            if f == "GC=F": c_epic = "GOLD"
            
            client.execute(
                """INSERT INTO market_symbols (display_name, yahoo_ticker, capital_epic, binance_ticker, priority_1, priority_2) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [f, f, c_epic, None, "YAHOO", "CAPITAL"]
            )

    print("✅ Seeded Inventory with New Rules.")
    
    # 3. HARVEST
    target_date = date(2026, 2, 19)
    print(f"🚜 Harvesting for {target_date}...")
    
    logger = CLILogger()
    db_map = get_symbol_map_from_db()
    tickers = list(db_map.keys())
    
    final_df, report = run_harvest_logic(tickers, target_date, db_map, logger)
    
    if not final_df.empty:
        print(f"💾 Saving {len(final_df)} rows...")
        save_data_to_turso(final_df, logger)
        print("✅ Data Saved!")
    else:
        print("❌ No Data Harvested.")

    if not report.empty:
        print("\n📊 Summary:")
        print(report.to_string())

if __name__ == "__main__":
    reset_and_harvest()
