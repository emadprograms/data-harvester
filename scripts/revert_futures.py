from src.database.connection import get_db_connection

def revert_futures():
    print("🚀 Reverting Futures to Yahoo (User correct: CL is Colgate)...")
    client = get_db_connection()
    
    # 1. CL=F -> P1=YAHOO, P2=NONE (or Massive? No, Massive is wrong) -> P2=NONE
    # Actually keep Massive as fallback? No, if CL is Colgate, we got wrong data.
    # So P1=YAHOO. P2=NONE for now.
    
    reverts = ["CL=F", "GC=F"]
    
    for sym in reverts:
        try:
            client.execute(
                """UPDATE market_symbols 
                   SET priority_1 = 'YAHOO', priority_2 = 'NONE', massive_ticker = ?
                   WHERE display_name = ?""",
                [sym, sym] # Reset massive ticker to display name just to ignore "CL"
            )
            print(f"✅ Reverted {sym}: P1=YAHOO")
        except Exception as e:
            print(f"❌ Error {sym}: {e}")

if __name__ == "__main__":
    revert_futures()
