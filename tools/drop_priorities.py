from src.database.connection import get_archive_db_connection
import sys

def drop_priority_columns():
    print("🚀 Dropping priority columns from Archive DB...")
    
    client = get_archive_db_connection()
    if not client:
        print("❌ Failed to connect to Archive DB")
        return

    try:
        # 1. Create new table without priority columns
        print("🛠️  Creating new_symbol_map...")
        client.execute("DROP TABLE IF EXISTS new_symbol_map")
        client.execute("""
            CREATE TABLE new_symbol_map (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                massive_ticker TEXT,
                binance_ticker TEXT
            )
        """)
        
        # 2. Copy data
        print("📦 Copying data...")
        # Get existing columns to ensure we map correctly
        # We know the old table has priority_1, priority_2, priority_3
        # So we just select the ones we want
        rows = client.execute("SELECT display_name, yahoo_ticker, massive_ticker, binance_ticker FROM symbol_map").rows
        
        for row in rows:
            client.execute(
                "INSERT INTO new_symbol_map (display_name, yahoo_ticker, massive_ticker, binance_ticker) VALUES (?, ?, ?, ?)",
                list(row)
            )
            
        # 3. Drop old table
        print("🗑️  Dropping old symbol_map...")
        client.execute("DROP TABLE symbol_map")
        
        # 4. Rename new table
        print("Qw Renaming new_symbol_map to symbol_map...")
        client.execute("ALTER TABLE new_symbol_map RENAME TO symbol_map")
        
        print("✅ Priority columns dropped successfully!")
        
    except Exception as e:
        print(f"❌ Error dropping columns: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()

if __name__ == "__main__":
    drop_priority_columns()
