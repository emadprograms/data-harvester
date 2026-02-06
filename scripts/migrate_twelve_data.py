from src.database.connection import get_db_connection

def add_twelve_col():
    print("🚀 Migrating DB: Adding 'twelve_data_ticker' column...")
    client = get_db_connection()
    try:
        client.execute("ALTER TABLE market_symbols ADD COLUMN twelve_data_ticker TEXT")
        print("✅ Column added.")
    except Exception as e:
        print(f"ℹ️ {e} (Likely already exists)")

if __name__ == "__main__":
    add_twelve_col()
