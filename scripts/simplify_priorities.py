from src.database.connection import get_db_connection

def simplify_config():
    print("🚀 Simplifying Config: P1=MASSIVE ➔ P2=YAHOO")
    client = get_db_connection()
    if not client: return

    # Special handling for Crypto (keeps Binance as P1)
    res = client.execute("SELECT display_name FROM market_symbols")
    
    count = 0
    for row in res.rows:
        display = row[0]
        
        if "USDT" in display:
             # P1=BINANCE, P2=YAHOO
             p1, p2 = 'BINANCE', 'YAHOO'
        elif display in ["CL=F", "GC=F"]:
             # P1=YAHOO, P2=NONE (Oil/Gold)
             p1, p2 = 'YAHOO', 'NONE'
        else:
             # Standard Stocks
             p1, p2 = 'MASSIVE', 'YAHOO'
             
        try:
            client.execute("""
                UPDATE market_symbols 
                SET priority_1 = ?,
                    priority_2 = ?,
                    priority_3 = 'NONE'
                WHERE display_name = ?
            """, [p1, p2, display])
            count += 1
        except Exception as e:
            print(f"Error {display}: {e}")

    print(f"✅ Simplified {count} symbols.")

if __name__ == "__main__":
    simplify_config()
