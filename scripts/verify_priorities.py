from src.database.connection import get_db_connection

def verify_prio():
    client = get_db_connection()
    # Check AAPL and NVDA
    res = client.execute("SELECT display_name, priority_1, priority_2, priority_3 FROM market_symbols WHERE display_name IN ('AAPL', 'NVDA')")
    
    print(f"{'SYM':<6} {'P1':<10} {'P2':<12} {'P3':<10}")
    print("-" * 40)
    for row in res.rows:
        print(f"{row[0]:<6} {row[1]:<10} {row[2]:<12} {row[3]:<10}")

if __name__ == "__main__":
    verify_prio()
