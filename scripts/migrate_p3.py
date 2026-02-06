from src.database.connection import get_db_connection

def add_p3_col():
    print("🚀 Migrating DB: Adding 'priority_3' column...")
    client = get_db_connection()
    try:
        client.execute("ALTER TABLE market_symbols ADD COLUMN priority_3 TEXT")
        print("✅ Column priority_3 added.")
    except Exception as e:
        print(f"ℹ️ {e} (Likely already exists)")

if __name__ == "__main__":
    add_p3_col()
