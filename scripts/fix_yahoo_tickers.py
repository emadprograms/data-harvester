from src.database.connection import get_db_connection

def fix_yahoo():
    print("🚀 Fixing Yahoo Tickers...")
    client = get_db_connection()
    if not client: return

    res = client.execute("SELECT display_name, yahoo_ticker FROM market_symbols")
    rows = res.rows
    updates = []

    for row in rows:
        display_name, current_yahoo = row
        t = display_name.upper().strip()
        new_yahoo = current_yahoo

        # 1. Crypto: BTCUSDT -> BTC-USD
        if t.endswith("USDT") and t != "EURUSDT":
            new_yahoo = t.replace("USDT", "-USD")
        
        # 2. Forex: EURUSDT -> EURUSD=X
        elif t == "EURUSDT":
            new_yahoo = "EURUSD=X"
            
        # 3. Indices
        elif t == "VIX" or t == "^VIX":
            new_yahoo = "^VIX"
        elif t == "US30" or t == "DIA": # If User used US30 (Capital style)
            # If display is DIA, Yahoo is DIA. If display is US30, likely Dow Index -> ^DJI.
            if t == "US30": new_yahoo = "^DJI"
        elif t == "US500":
            new_yahoo = "^GSPC" # S&P 500 Index
        elif t == "US100":
            # Nasdaq 100 Index
            new_yahoo = "^NDX" 

        if new_yahoo != current_yahoo:
            updates.append((new_yahoo, display_name))

    if updates:
        print(f"Applying {len(updates)} fixes...")
        for y, d in updates:
            try:
                client.execute("UPDATE market_symbols SET yahoo_ticker = ? WHERE display_name = ?", [y, d])
                print(f"   Updated {d}: {y}")
            except Exception as e:
                print(f"   ❌ Error {d}: {e}")
        print("✅ Done.")
    else:
        print("✨ No Yahoo updates needed.")

if __name__ == "__main__":
    fix_yahoo()
