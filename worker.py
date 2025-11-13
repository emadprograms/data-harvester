import os
import sys
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv 

# Load .env if it exists (Local testing)
load_dotenv()

# --- THIS IS THE FIX ---
# We are changing 'daily_harvester' to 'app'
try:
    from app import (
        get_db_connection,
        get_symbol_map_from_db,
        run_harvest_logic,
        save_data_to_turso,
        US_EASTERN
    )
except ImportError as e:
    # Updated error message
    print(f"CRITICAL ERROR: Could not import 'app.py'. {e}")
    sys.exit(1)
# --- END FIX ---

class ConsoleLogger:
    """A simple logger that prints to the GitHub Actions console."""
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
    
    # Get the current time in New York
    now_et = datetime.now(US_EASTERN)
    market_open_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    
    # If it's before 9:30 AM ET, harvest *yesterday's* data.
    if now_et < market_open_et:
        # If today is Monday, yesterday was Sunday. We need to get Friday.
        if now_et.weekday() == 0: # 0 = Monday
            target_date = now_et.date() - timedelta(days=3)
        # If today is Sunday, we need Friday
        elif now_et.weekday() == 6: # 6 = Sunday
             target_date = now_et.date() - timedelta(days=2)
        # Otherwise, just get yesterday
        else:
            target_date = now_et.date() - timedelta(days=1)
        logger.log(f"   -> It's before 9:30 AM ET. Harvesting for previous market day: {target_date}")
    else:
        # It's after 9:30 AM ET, harvest *today's* data.
        target_date = now_et.date()
        logger.log(f"   -> It's after 9:30 AM ET. Harvesting for today: {target_date}")


    # 3. Fetch Inventory
    logger.log("📦 Fetching symbol inventory from Turso...")
    db_map = get_symbol_map_from_db()
    if not db_map:
        logger.log("❌ Error: No symbols found in database. Exiting.")
        sys.exit(1)
    
    tickers = list(db_map.keys())
    logger.log(f"🦁 Harvesting {len(tickers)} symbols: {tickers}")

    # 4. Run the Harvest
    try:
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=tickers,
            target_date=target_date,
            db_map=db_map,
            logger=logger,
            harvest_mode="🚀 Full Day"
        )
    except Exception as e:
        logger.log(f"❌ CRITICAL ERROR during harvest: {e}")
        sys.exit(1)

    # 5. Analyze Results & Commit
    if final_df is not None and not final_df.empty:
        row_count = len(final_df)
        logger.log(f"✅ Harvest finished. Collected {row_count} total candles.")
        
        if report_df is not None:
            failures = report_df[report_df['Status'].str.contains("Failed")]
            if not failures.empty:
                logger.log(f"⚠️ WARNING: {len(failures)} symbols failed completely.")
            
            fallbacks = report_df[report_df['Mode'].str.contains("Fallback")]
            if not fallbacks.empty:
                logger.log(f"⚠️ WARNING: {len(fallbacks)} symbols used Capital fallback (No Volume).")
        else:
            logger.log("⚠️ Warning: Report card was empty.")


        # Save
        logger.log("💾 Committing data to Turso...")
        success = save_data_to_turso(final_df, logger)
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