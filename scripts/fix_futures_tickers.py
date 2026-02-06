from src.database.connection import get_db_connection

def fix_futures():
    print("🚀 Fixing Futures Tickers on Massive...")
    client = get_db_connection()
    
    # Map Display Name -> Correct Massive Ticker
    corrections = {
        "CL=F": "CL",
        "GC=F": "GC"
    }
    
    for display, new_massive in corrections.items():
        try:
            client.execute(
                "UPDATE market_symbols SET massive_ticker = ? WHERE display_name = ?", 
                [new_massive, display]
            )
            print(f"✅ Updated {display}: Massive Ticker -> {new_massive}")
        except Exception as e:
            print(f"❌ Error updating {display}: {e}")

if __name__ == "__main__":
    fix_futures()
