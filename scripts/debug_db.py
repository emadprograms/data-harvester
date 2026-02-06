from src.database.connection import get_db_connection

def check_counts():
    client = get_db_connection()
    if not client:
        print("❌ Connect failed")
        return

    try:
        res = client.execute("SELECT count(*) FROM symbol_map")
        print(f"symbol_map count: {res.rows[0][0]}")
    except Exception as e:
        print(f"symbol_map error: {e}")

    try:
        res = client.execute("SELECT count(*) FROM market_symbols")
        print(f"market_symbols count: {res.rows[0][0]}")
    except Exception as e:
        print(f"market_symbols error: {e}")
        
    # Dump 5 rows from symbol_map if it exists
    try:
        res = client.execute("SELECT * FROM symbol_map LIMIT 5")
        if res.rows:
            print("\nSAMPLE symbol_map data:")
            for r in res.rows:
                print(r)
    except:
        pass

if __name__ == "__main__":
    check_counts()
