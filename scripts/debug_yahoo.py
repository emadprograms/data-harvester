from src.database.connection import get_db_connection

def check_yahoo_tickers():
    client = get_db_connection()
    res = client.execute("SELECT display_name, yahoo_ticker, priority_1, priority_2 FROM market_symbols LIMIT 20")
    print(f"{'DISPLAY':<15} {'YAHOO_TICKER':<15} {'P1':<10} {'P2':<10}")
    print("-" * 60)
    for row in res.rows:
        print(f"{row[0]:<15} {row[1]:<15} {row[2]:<10} {row[3]:<10}")

if __name__ == "__main__":
    check_yahoo_tickers()
