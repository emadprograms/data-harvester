"""
Synchronizes the Mirror database with the Archive database.
Deletes all data in Mirror and re-inserts from Archive.
"""
import pandas as pd
from src.database.connection import get_archive_db_connection, get_mirror_db_connection
from src.database.operations import _save_to_client

def sync_from_archive():
    archive = get_archive_db_connection()
    mirror = get_mirror_db_connection()
    
    if not archive or not mirror:
        print("❌ Could not connect to databases.")
        return

    try:
        print("🔄 Syncing Mirror from Archive...")
        
        # 1. Clear Mirror
        print("🗑️ Clearing Mirror database...")
        mirror.execute("DELETE FROM market_data")
        mirror.execute("DELETE FROM symbol_map")
        
        # 2. Sync Symbol Map
        print("📥 Fetching Symbol Map from Archive...")
        res_map = archive.execute("SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker FROM symbol_map")
        
        if res_map.rows:
            print(f"➡️ Inserting {len(res_map.rows)} symbols into Mirror...")
            for row in res_map.rows:
                mirror.execute(
                    "INSERT INTO symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker, capital_ticker) VALUES (?, ?, ?, ?, ?)",
                    list(row)
                )
        
        # 3. Sync Market Data
        print("📥 Fetching Market Data from Archive...")
        res_data = archive.execute("SELECT timestamp, symbol, open, high, low, close, volume, session FROM market_data")
        
        if res_data.rows:
            print(f"➡️ Inserting {len(res_data.rows)} market data rows into Mirror...")
            
            rows_to_insert = [tuple(row) for row in res_data.rows]
            
            # We need a dummy logger or None
            class DummyLogger:
                def log(self, msg): print(f"      {msg.strip()}")
            
            # Use REPLACE mode to ensure absolute parity
            _save_to_client(mirror, rows_to_insert, logger=DummyLogger(), label="Mirror-Sync", mode="REPLACE")
            
        print("✅ Sync Complete.")

    except Exception as e:
        import traceback
        print(f"❌ Sync Error: {e}")
        traceback.print_exc()
    finally:
        archive.close()
        mirror.close()

if __name__ == "__main__":
    sync_from_archive()
