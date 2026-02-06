import sys
from src.database.connection import get_db_connection

def apply_bulk_updates():
    print("🚀 Applying Bulk Updates: Ticker Fixes + Global Priority Rule...")
    client = get_db_connection()
    if not client: return

    res = client.execute("SELECT display_name, priority_1 FROM market_symbols")
    rows = res.rows
    print(f"Processing {len(rows)} symbols...")
    
    updates_count = 0
    prio_count = 0
    
    for row in rows:
        display_name, current_p1 = row
        t = display_name.upper().strip()
        
        # 1. Determine Correct Massive Ticker
        # By default, assume Massive Ticker = Display Name (e.g. AAPL -> AAPL)
        new_massive = t
        
        # Crypto
        if t.endswith("USDT"):
            b = t.replace("USDT", "")
            new_massive = f"X:{b}USD"
            # Special case for verified ones if needed, but heuristic is solid.
        
        # Forex
        elif t == "EURUSDT":
            new_massive = "C:EURUSD"
            
        # Indices/VIX
        elif t in ["VIX", "^VIX"]:
            new_massive = "I:VIX"
        elif t == "US500": new_massive = "I:SPX"
        elif t == "US30": new_massive = "I:DJI"
        elif t == "US100": new_massive = "I:NDX"
        
        # 2. Determine Priority
        # User requested: P1=MASSIVE, P2=YAHOO
        # Exception: Crypto -> BINANCE
        
        new_p1 = "MASSIVE"
        new_p2 = "YAHOO"
        
        if t.endswith("USDT"):
            new_p1 = "BINANCE"
            new_p2 = "YAHOO" # Fallback to Yahoo for Crypto is safer than Massive usually
            # But if user wants Massive as fallback:
            # new_p2 = "MASSIVE" 
            # Sticking to previous verification plan: Binance -> Yahoo is standard.
        
        # Execute Update
        try:
            client.execute("""
                UPDATE market_symbols 
                SET massive_ticker = ?, priority_1 = ?, priority_2 = ?
                WHERE display_name = ?
            """, [new_massive, new_p1, new_p2, display_name])
            updates_count += 1
        except Exception as e:
            print(f"❌ Error updating {display_name}: {e}")

    print(f"✅ Completed! Updated {updates_count} symbols.")
    print("   - Non-Crypto: P1=MASSIVE, P2=YAHOO")
    print("   - Crypto: P1=BINANCE, P2=YAHOO")
    print("   - Fixed Massive Tickers (heuristics applied)")

if __name__ == "__main__":
    apply_bulk_updates()
