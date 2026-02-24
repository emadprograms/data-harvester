"""
Standardizes the symbol_map table and drops legacy tables.
"""
from src.database.connection import get_archive_db_connection, get_mirror_db_connection

def migrate():
    archive = get_archive_db_connection()
    mirror = get_mirror_db_connection()
    
    for label, client in [("Archive", archive), ("Mirror", mirror)]:
        if not client: continue
        print(f"🛠️  Cleaning up {label}...")
        
        try:
            # 1. Drop redundant table
            client.execute("DROP TABLE IF EXISTS market_symbols")
            
            # 2. Ensure symbol_map is clean
            # We don't want priority columns here anymore as logic is in code
            # Check current columns
            res = client.execute("PRAGMA table_info(symbol_map)")
            cols = [row[1] for row in res.rows]
            
            if "priority_1" in cols:
                print(f"🧹 Removing legacy priority columns from {label}.symbol_map...")
                # SQLite (and thus LibSQL) doesn't support DROP COLUMN easily in older versions,
                # but we can recreate the table.
                client.execute("CREATE TABLE symbol_map_new (display_name TEXT PRIMARY KEY, yahoo_ticker TEXT, massive_ticker TEXT, binance_ticker TEXT)")
                client.execute("INSERT INTO symbol_map_new SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker FROM symbol_map")
                client.execute("DROP TABLE symbol_map")
                client.execute("ALTER TABLE symbol_map_new RENAME TO symbol_map")
                print(f"✅ symbol_map standardized in {label}.")
                
        except Exception as e:
            print(f"❌ Migration Error in {label}: {e}")
        finally:
            client.close()

if __name__ == "__main__":
    migrate()
