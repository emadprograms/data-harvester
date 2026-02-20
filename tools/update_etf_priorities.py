"""
Update priorities for specific ETFs to YAHOO -> CAPITAL.
Symbols: DIA, XLC, XLE, XLF, XLI, XLP, XLU, XLV
"""
from src.database.connection import get_db_connection, get_local_db_connection

def update_priorities():
    symbols = ['DIA', 'XLC', 'XLE', 'XLF', 'XLI', 'XLP', 'XLU', 'XLV']
    
    clients = [get_db_connection(), get_local_db_connection()]
    
    for client in clients:
        if not client:
            continue
        
        label = "Remote" if "libsql" in str(client) or "https" in str(client) else "Local"
        print(f"🔄 Updating {label} database...")
        
        for symbol in symbols:
            try:
                client.execute(
                    "UPDATE market_symbols SET priority_1 = 'YAHOO', priority_2 = 'CAPITAL' WHERE display_name = ?",
                    [symbol]
                )
                print(f"   ✅ Updated {symbol}")
            except Exception as e:
                print(f"   ❌ Failed to update {symbol}: {e}")

if __name__ == "__main__":
    update_priorities()
