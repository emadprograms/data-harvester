from src.database.connection import get_db_connection

def apply_p3_config():
    print("🚀 Applying 3-Stage Priority Config: MASSIVE -> TWELVE -> YAHOO")
    client = get_db_connection()
    
    # Select all except crypto or special handling
    res = client.execute("SELECT display_name FROM market_symbols")
    
    count = 0
    for row in res.rows:
        display = row[0]
        
        # logic
        if "USDT" in display: # Crypto -> Binance
            continue
            
        if display == "CL=F" or display == "GC=F": # Commodities -> Yahoo
             continue
             
        # Standard Stocks/Indices
        try:
            client.execute("""
                UPDATE market_symbols 
                SET priority_1 = 'MASSIVE',
                    priority_2 = 'TWELVE_DATA',
                    priority_3 = 'YAHOO',
                    twelve_data_ticker = ?
                WHERE display_name = ?
            """, [display, display]) # Default TD ticker = display name
            count += 1
        except Exception as e:
            print(f"Error {display}: {e}")

    print(f"✅ Updated {count} symbols to 3-stage priority.")

if __name__ == "__main__":
    apply_p3_config()
