from src.database.connection import get_db_connection

def revert_oil_yahoo():
    print("🚀 Reverting Oil to Yahoo (Twelve Data WTI reqs Premium)...")
    client = get_db_connection()
    try:
        client.execute("""
            UPDATE market_symbols 
            SET priority_1 = 'YAHOO', 
                priority_2 = 'NONE'
            WHERE display_name = 'CL=F'
        """)
        print("✅ CL=F Reverted to P1=YAHOO.")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    revert_oil_yahoo()
