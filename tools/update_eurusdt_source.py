
from src.database.connection import get_db_connection
import sys

def update_eurusdt():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect")
        return
    
    try:
        # Switch EURUSDT to Capital.com Primary
        query = """
        UPDATE market_symbols
        SET priority_1 = 'CAPITAL',
            priority_2 = 'YAHOO',
            priority_3 = 'NONE',
            capital_epic = 'EURUSD'
        WHERE display_name = 'EURUSDT'
        """
        conn.execute(query)
        print("✅ EURUSDT updated successfully: P1=CAPITAL, P2=YAHOO, Epic=EURUSD")
    except Exception as e:
        print(f"Error updating: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    update_eurusdt()
