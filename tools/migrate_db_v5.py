from src.database.connection import get_archive_db_connection
import sys

def migrate_archive():
    client = get_archive_db_connection()
    if not client:
        print("❌ Failed to connect to Archive DB")
        return

    try:
        # 1. Drop existing symbol_map (empty one)
        print("🛠️  Dropping empty 'symbol_map'...")
        client.execute("DROP TABLE IF EXISTS symbol_map")
        
        # 2. Rename market_symbols to symbol_map
        print("🛠️  Renaming 'market_symbols' to 'symbol_map'...")
        # Since table renaming isn't always straightforward in all SQL dialects,
        # we check if market_symbols exists first.
        tables = [t[0] for t in client.execute("SELECT name FROM sqlite_schema WHERE type='table'").rows]
        if 'market_symbols' in tables:
            client.execute("ALTER TABLE market_symbols RENAME TO symbol_map")
        else:
            print("⚠️ 'market_symbols' not found (maybe already renamed?)")

        # 3. Rename capital_epic to massive_ticker
        print("🛠️  Renaming column 'capital_epic' to 'massive_ticker'...")
        # Check columns first
        info = client.execute("PRAGMA table_info(symbol_map)")
        cols = [row[1] for row in info.rows]
        
        if 'capital_epic' in cols:
            client.execute("ALTER TABLE symbol_map RENAME COLUMN capital_epic TO massive_ticker")
        else:
            print(f"⚠️ 'capital_epic' column not found in symbol_map. Columns: {cols}")

        # 4. Update Priorities (Capital -> Massive)
        print("🛠️  Updating Priorities: CAPITAL -> MASSIVE, YAHOO fallback...")
        client.execute("""
            UPDATE symbol_map 
            SET priority_1 = 'MASSIVE', 
                priority_2 = 'YAHOO' 
            WHERE priority_1 = 'CAPITAL'
        """)

        # 5. Empty market_data
        print("🧹 Emptying 'market_data' table...")
        client.execute("DELETE FROM market_data")
        
        print("✅ Archive DB Migration Complete!")
        
    except Exception as e:
        print(f"❌ Error during Archive Migration: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    migrate_archive()
