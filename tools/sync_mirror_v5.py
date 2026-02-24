"""
Synchronizes symbol_map and market_data from Archive to Mirror.
Optimized for the simplified schema (no priority columns).
"""
import pandas as pd
from src.database.connection import get_archive_db_connection, get_mirror_db_connection

def sync():
    archive = get_archive_db_connection()
    mirror = get_mirror_db_connection()
    
    if not archive or not mirror:
        print("❌ Could not connect to both DBs.")
        return

    try:
        # 1. Sync symbol_map
        print("🔄 Syncing symbol_map...")
        res = archive.execute("SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker FROM symbol_map")
        df_map = pd.DataFrame([list(row) for row in res.rows], columns=['display_name', 'yahoo_ticker', 'massive_ticker', 'binance_ticker'])
        
        mirror.execute("DELETE FROM symbol_map")
        for _, row in df_map.iterrows():
            mirror.execute(
                "INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker) VALUES (?, ?, ?, ?)",
                [row.display_name, row.yahoo_ticker, row.massive_ticker, row.binance_ticker]
            )
        print(f"✅ Synced {len(df_map)} symbols.")

        # 2. Sync Recent Market Data (Optional but good for parity)
        print("🔄 Syncing last 24h market data...")
        res_data = archive.execute("SELECT * FROM market_data WHERE timestamp >= datetime('now', '-1 day')")
        if res_data.rows:
            cols = [col[0] for col in res_data.columns] if hasattr(res_data, 'columns') else ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session']
            df_data = pd.DataFrame([list(row) for row in res_data.rows], columns=cols)
            
            placeholders = ", ".join(["?"] * len(cols))
            query = f"INSERT OR REPLACE INTO market_data ({', '.join(cols)}) VALUES ({placeholders})"
            
            for _, row in df_data.iterrows():
                mirror.execute(query, list(row))
            print(f"✅ Synced {len(df_data)} data rows.")
        else:
            print("ℹ️ No recent data to sync.")

    except Exception as e:
        print(f"❌ Sync Error: {e}")
    finally:
        archive.close()
        mirror.close()

if __name__ == "__main__":
    sync()
