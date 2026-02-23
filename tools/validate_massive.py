import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from polygon import RESTClient
from src.database.connection import get_archive_db_connection
from src.infisical_manager import InfisicalManager
from src.utils.logger import CLILogger

def validate_ticker(ticker_info, client, target_date, logger):
    display_name, m_ticker = ticker_info
    if not m_ticker:
        return display_name, m_ticker, False, "No Ticker"
    
    try:
        start_ts = int(pd.Timestamp(target_date).timestamp() * 1000)
        end_ts = int((pd.Timestamp(target_date) + pd.Timedelta(days=1)).timestamp() * 1000) - 1
        
        aggs = client.list_aggs(
            ticker=m_ticker,
            multiplier=1,
            timespan="minute",
            from_=start_ts,
            to=end_ts,
            limit=50000
        )
        
        results = list(aggs)
        if not results:
            return display_name, m_ticker, False, "Empty Response"
        
        return display_name, m_ticker, True, f"{len(results)} rows"
    except Exception as e:
        return display_name, m_ticker, False, str(e)

def run_parallel_validation():
    print("🚀 Starting Parallel Massive Validation (9 Keys, Parallel Workers)...")
    logger = CLILogger()
    mgr = InfisicalManager()
    
    # 1. Get all 9 Keys
    keys = mgr.get_massive_keys()
    if not keys:
        print("❌ No Massive keys found!")
        return
    print(f"🔑 Using {len(keys)} API keys.")
    clients = [RESTClient(k) for k in keys]

    # 2. Get tickers from DB
    db = get_archive_db_connection()
    rows = db.execute("SELECT display_name, massive_ticker FROM symbol_map").rows
    db.close()
    
    total_tickers = len(rows)
    print(f"📦 Total Tickers to check: {total_tickers}")
    
    target_date = date(2026, 2, 20)
    results = []

    # 3. Parallel Execution
    # We use a max_workers matching the number of keys. 
    # Each worker will pick a key and process tickers from the queue.
    # To ensure keys are used evenly, we'll round-robin assign them.
    
    with ThreadPoolExecutor(max_workers=len(clients)) as executor:
        futures = []
        for i, row in enumerate(rows):
            client = clients[i % len(clients)] # Round-robin assignment
            futures.append(executor.submit(validate_ticker, row, client, target_date, logger))
        
        for future in as_completed(futures):
            results.append(future.result())

    # 4. Process Results
    failed = []
    working = []
    
    for display_name, m_ticker, is_working, msg in results:
        if is_working:
            print(f"✅ {display_name} ({m_ticker}): {msg}")
            working.append(display_name)
        else:
            print(f"❌ {display_name} ({m_ticker}): {msg}")
            failed.append((display_name, m_ticker))

    # 5. Update DB
    if failed:
        print(f"\n🗑️  Removing {len(failed)} failed massive_tickers from DB...")
        db = get_archive_db_connection()
        for display_name, _ in failed:
            db.execute("UPDATE symbol_map SET massive_ticker = NULL WHERE display_name = ?", [display_name])
        db.close()
        
    print("\n📊 Summary:")
    print(f"Working: {len(working)}")
    print(f"Failed:  {len(failed)}")
    
    if failed:
        print("\n❌ Failed Tickers:")
        for d, m in failed:
            print(f"- {d} ({m})")

if __name__ == "__main__":
    run_parallel_validation()
