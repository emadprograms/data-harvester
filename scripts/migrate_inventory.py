from src.database.connection import get_db_connection
import sys

def migrate():
    print("🚀 Starting Force Migration...")
    client = get_db_connection()
    if not client:
        print("❌ Connect failed")
        return

    # 1. Ensure Table Exists
    print("1. Creating table if not exists...")
    try:
        client.execute("""
            CREATE TABLE IF NOT EXISTS market_symbols (
                display_name TEXT PRIMARY KEY,
                yahoo_ticker TEXT,
                massive_ticker TEXT,
                binance_ticker TEXT,
                priority_1 TEXT, 
                priority_2 TEXT
            )
        """)
        print("✅ Table created/verified.")
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        return

    # 2. Check if clean
    res = client.execute("SELECT count(*) FROM market_symbols")
    count = res.rows[0][0]
    print(f"Current market_symbols count: {count}")
    
    if count > 0:
        print("⚠️ Table not empty. Clearing it for fresh migration...")
        client.execute("DELETE FROM market_symbols")
        print("✅ Cleared.")

    # 3. Fetch Old Data
    print("3. Fetching old data from symbol_map...")
    try:
        res = client.execute("SELECT user_ticker, capital_epic, source_strategy FROM symbol_map")
        old_rows = res.rows
        print(f"Found {len(old_rows)} old records.")
    except Exception as e:
        print(f"❌ Failed to read symbol_map: {e}")
        return

    # 4. Migrate
    print("4. Migrating...")
    success_count = 0
    for row in old_rows:
        user_ticker = row[0]
        cap_epic = row[1]
        strategy = row[2]
        
        # Logic
        p1 = "YAHOO"
        p2 = "MASSIVE"
        y_ticker = user_ticker
        m_ticker = cap_epic
        b_ticker = None
        
        if user_ticker.endswith("USDT"):
             p1 = "BINANCE"
             p2 = "YAHOO"
             b_ticker = user_ticker
        elif user_ticker.endswith("=F"):
             p1 = "YAHOO"
             p2 = "MASSIVE"
             
        if strategy == "CAPITAL_ONLY":
            p1 = "MASSIVE"
            p2 = "NONE"
            
        try:
            client.execute(
                """INSERT INTO market_symbols 
                   (display_name, yahoo_ticker, massive_ticker, binance_ticker, priority_1, priority_2) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [user_ticker, y_ticker, m_ticker, b_ticker, p1, p2]
            )
            success_count += 1
        except Exception as e:
            print(f"Failed to insert {user_ticker}: {e}")

    print(f"✅ Migration Complete! {success_count} records migrated.")

if __name__ == "__main__":
    migrate()
