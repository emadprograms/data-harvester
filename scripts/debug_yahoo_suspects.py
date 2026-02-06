from src.database.connection import get_db_connection

def check_suspects():
    client = get_db_connection()
    # Check Crypto, Futures, and VIX
    suspects = ["BTCUSDT", "ETHUSDT", "CL=F", "GC=F", "VIX", "US30", "US500", "US100", "EURUSDT"]
    
    placeholders = ",".join("?" * len(suspects))
    res = client.execute(f"SELECT display_name, yahoo_ticker FROM market_symbols WHERE display_name IN ({placeholders})", suspects)
    
    print(f"{'DISPLAY':<15} {'YAHOO_TICKER':<15}")
    print("-" * 35)
    for row in res.rows:
        print(f"{row[0]:<15} {row[1]:<15}")

if __name__ == "__main__":
    check_suspects()
