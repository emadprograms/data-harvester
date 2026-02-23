import sys
from src.database.connection import get_archive_db_connection, get_mirror_db_connection

def sync_mirror_db():
    print("🚀 Starting Archive -> Mirror Sync...")
    
    archive = get_archive_db_connection()
    mirror = get_mirror_db_connection()
    
    if not archive:
        print("❌ Archive connection failed")
        return
    if not mirror:
        print("❌ Mirror connection failed")
        return

    try:
        # 1. Get Schema & Data from Archive
        print("📥 Reading Schema from Archive...")
        
        # Get actual column names from archive
        sm_cols_info = archive.execute("PRAGMA table_info(symbol_map)").rows
        sm_cols = [f"{r[1]} {r[2]}" for r in sm_cols_info] # e.g. "display_name TEXT"
        sm_col_names = [r[1] for r in sm_cols_info]
        # We need to know PK
        pk_col = next((r[1] for r in sm_cols_info if r[5] > 0), "display_name")
        
        # Reconstruct CREATE TABLE
        create_sm_sql = f"CREATE TABLE IF NOT EXISTS symbol_map ({', '.join(sm_cols)}, PRIMARY KEY({pk_col}))"
        
        # market_data schema
        md_cols_info = archive.execute("PRAGMA table_info(market_data)").rows
        md_cols = [f"{r[1]} {r[2]}" for r in md_cols_info]
        create_md_sql = """
            CREATE TABLE IF NOT EXISTS market_data (
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL, 
                high REAL, 
                low REAL, 
                close REAL, 
                volume REAL, 
                session TEXT,
                PRIMARY KEY (symbol, timestamp)
            )
        """

        # Get all symbols
        symbols = archive.execute(f"SELECT {', '.join(sm_col_names)} FROM symbol_map").rows
        print(f"📦 Found {len(symbols)} symbols to sync.")

        # 2. Reset Mirror
        print("🧹 Wiping Mirror DB...")
        mirror.execute("DROP TABLE IF EXISTS market_data")
        mirror.execute("DROP TABLE IF EXISTS symbol_map")
        mirror.execute("DROP TABLE IF EXISTS market_symbols") # Just in case
        
        # 3. Recreate Tables
        print("🛠️  Recreating Tables in Mirror...")
        mirror.execute(create_sm_sql)
        mirror.execute(create_md_sql)
        
        # 4. Populate Symbol Map
        print("🌱 Seeding Symbol Map in Mirror...")
        if symbols:
            # Prepare placeholders based on column count
            placeholders = ",".join(["?"] * len(symbols[0]))
            
            # Since libsql-client (Python) might not support explicit transaction blocks via execute("BEGIN"),
            # we'll just insert in a loop. It's only ~45 rows, so it's fast enough.
            for row in symbols:
                # Convert row (tuple) to list for parameters
                mirror.execute(f"INSERT INTO symbol_map VALUES ({placeholders})", list(row))
            
        print("✅ Mirror DB Synced Successfully!")
        
    except Exception as e:
        print(f"❌ Error during sync: {e}")
        import traceback
        traceback.print_exc()
    finally:
        archive.close()
        mirror.close()

if __name__ == "__main__":
    sync_mirror_db()
