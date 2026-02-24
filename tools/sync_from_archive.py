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
        res_map = archive.execute("SELECT * FROM symbol_map")
        if res_map.rows:
            cols = [c[0] for c in res_map.columns] if hasattr(res_map, 'columns') else ['display_name', 'yahoo_ticker', 'massive_ticker', 'binance_ticker', 'capital_ticker']
            placeholders = ", ".join(["?"] * len(cols))
            
            print(f"➡️ Inserting {len(res_map.rows)} symbols into Mirror...")
            for row in res_map.rows:
                mirror.execute(f"INSERT INTO symbol_map ({', '.join(cols)}) VALUES ({placeholders})", list(row))
        
        # 3. Sync Market Data
        print("📥 Fetching Market Data from Archive...")
        # Fetch in chunks if too large, but for now assuming it fits in memory or we can iterate
        # To be safe, let's just fetch all for now, or we could paginate. 
        # Given the "batch" helper, let's fetch all and insert in batches.
        res_data = archive.execute("SELECT * FROM market_data")
        
        if res_data.rows:
            print(f"➡️ Inserting {len(res_data.rows)} market data rows into Mirror...")
            # Reuse _save_to_client for batching
            # _save_to_client expects list of tuples.
            # And it expects specific order: timestamp, symbol, open, high, low, close, volume, session
            # Archive select * might return in that order, but let's be safe? 
            # The table def is: timestamp, symbol, open, high, low, close, volume, session.
            # So SELECT * should be fine.
            
            rows_to_insert = [tuple(row) for row in res_data.rows]
            
            # We need a dummy logger or None
            class DummyLogger:
                def log(self, msg): print(msg)
            
            _save_to_client(mirror, rows_to_insert, logger=DummyLogger(), label="Mirror-Sync")
            
        print("✅ Sync Complete.")

    except Exception as e:
        print(f"❌ Sync Error: {e}")
    finally:
        archive.close()
        mirror.close()

if __name__ == "__main__":
    sync_from_archive()
