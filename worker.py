import os
import sys
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv 

# Load .env for local testing
load_dotenv()

# --- MODIFIED: Import from the new, clean core_logic.py ---
try:
    import core_logic as cl
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import 'core_logic.py'. {e}")
    sys.exit(1)
# --- END MODIFICATION ---

class ConsoleLogger:
    def log(self, message):
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")

def run_automation():
    logger = ConsoleLogger()
    logger.log("--- 🤖 STARTING MARKET LION AUTOMATION ---")

    # 1. Validate Environment Variables
    required_vars = [
        "TURSO_DB_URL", "TURSO_AUTH_TOKEN",
        "CAPITAL_X_CAP_API_KEY", "CAPITAL_IDENTIFIER", "CAPITAL_PASSWORD"
    ]
    missing = [var for var in required_vars if not os.environ.get(var)]
    if missing:
        logger.log(f"❌ Error: Missing environment variables: {', '.join(missing)}")
        sys.exit(1)

    # 2. Determine Smart Date (Today or Yesterday in NY)
    logger.log("Determining target date...")
    now_et = datetime.now(cl.US_EASTERN)
    market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    
    if now_et < market_open_et:
        if now_et.weekday() == 0: # Monday
            target_date = now_et.date() - timedelta(days=3)
        elif now_et.weekday() == 6: # Sunday
             target_date = now_et.date() - timedelta(days=2)
        else:
            target_date = now_et.date() - timedelta(days=1)
        logger.log(f"   -> It's before 9:30 AM ET. Harvesting for previous market day: {target_date}")
    else:
        target_date = now_et.date()
        logger.log(f"   -> It's after 9:30 AM ET. Harvesting for today: {target_date}")

    # 3. Get DB connection (non-cached)
    logger.log("Establishing database connection...")
    db_client = cl.get_db_connection()
    if not db_client:
        logger.log("❌ Error: Could not establish DB connection. Exiting.")
        sys.exit(1)

    # 4. Fetch Inventory
    logger.log("📦 Fetching symbol inventory from Turso...")
    db_map = cl.get_symbol_map_from_db(client=db_client) # Pass client
    if not db_map:
        logger.log("❌ Error: No symbols found in database. Exiting.")
        sys.exit(1)
    
    tickers = list(db_map.keys())
    logger.log(f"🦁 Harvesting {len(tickers)} symbols: {tickers}")

    # 5. Run the Harvest
    try:
        final_df, report_df = cl.run_harvest_logic(
            tickers_to_harvest=tickers,
            target_date=target_date,
            db_map=db_map,
            logger=logger,
            harvest_mode="🚀 Full Day"
        )
    except Exception as e:
        logger.log(f"❌ CRITICAL ERROR during harvest: {e}")
        sys.exit(1)

    # 6. Analyze Results & Commit
    if final_df is not None and not final_df.empty:
        row_count = len(final_df)
        logger.log(f"✅ Harvest finished. Collected {row_count} total candles.")
        
        if report_df is not None and not report_df.empty():
            failures = report_df[report_df['Status'].str.contains("Failed")]
            if not failures.empty:
                logger.log(f"⚠️ WARNING: {len(failures)} symbols failed completely.")
            
            fallbacks = report_df[report_df['Mode'].str.contains("Fallback")]
            if not fallbacks.empty:
                logger.log(f"⚠️ WARNING: {len(fallbacks)} symbols used Capital fallback (No Volume).")
        else:
            logger.log("⚠️ Warning: Report card was empty or not a DataFrame.")

        logger.log("💾 Committing data to Turso...")
        success = cl.save_data_to_turso(final_df, logger, client=db_client)
        if success:
            logger.log("✅ SUCCESS: Data saved securely.")
        else:
            logger.log("❌ ERROR: Database commit failed.")
            sys.exit(1)
    else:
        logger.log("⚠️ Harvest finished but returned NO data.")

    logger.log("--- 🤖 AUTOMATION COMPLETE ---")

if __name__ == "__main__":
    run_automation()