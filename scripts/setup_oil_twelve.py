from src.database.connection import get_db_connection

def setup_oil():
    print("🚀 Configuring 'CL=F' to use Twelve Data (WTI/USD)...")
    client = get_db_connection()
    
    # Update CL=F
    # P1: TWELVE_DATA
    # Ticker: WTI/USD
    
    try:
        client.execute("""
            UPDATE market_symbols 
            SET priority_1 = 'TWELVE_DATA', 
                priority_2 = 'YAHOO', 
                twelve_data_ticker = 'WTI/USD'
            WHERE display_name = 'CL=F'
        """)
        print("✅ CL=F Updated.")
    except Exception as e:
        print(f"❌ Error CL=F: {e}")

    # Update GC=F (Gold) -> XAU/USD? (Twelve Data uses XAU/USD)
    # Let's verify XAU/USD works? 
    # For now, user asked for Oil. I will stick to Oil.
    # But checking if BRENT is needed? BRENT/USD.
    # User asked for "accurate spot data". 
    
    print("✅ Configuration Complete.")

if __name__ == "__main__":
    setup_oil()
