import os
import sys
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv 

# Load .env if it exists (Local testing)
load_dotenv()

# Import logic from the main app
try:
    from daily_harvester import (
        get_db_connection,
        get_symbol_map_from_db,
        run_harvest_logic,
        save_data_to_turso,
        US_EASTERN
    )
except ImportError as e:
    print(f"CRITICAL ERROR: Could not import daily_harvester. {e}")
    sys.exit(1)

class ConsoleLogger:
    """A simple logger that prints to the GitHub Actions console."""
    def log(self, message):
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")

def determine_target_date(logger):
    """
    Smart logic to decide which date to harvest.
    - If run at 16:05 ET (Market Close), it picks Today.
    - If run at 02:00 ET (Next Morning), it picks Yesterday.
    """
    now_et = datetime.now(US_EASTERN)
    
    # Market Open is 9:30 AM ET.
    # If we are running BEFORE 9:30 AM ET, we assume we want the 
    # PREVIOUS day's data, because today's market hasn't opened yet.
    if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
        target_date = now_et.date() - timedelta(days=1)
        logger.log(f"🕰️ Time is {now_et.strftime('%H:%M')} ET (Before Market Open).")
        logger.log(f"   -> Targeting PREVIOUS trading day: {target_date}")
    else:
        target_date = now_et.date()
        logger.log(f"🕰️ Time is {now_et.strftime('%H:%M')} ET (After Market Open).")
        logger.log(f"   -> Targeting CURRENT trading day: {target_date}")
        
    # Skip weekends (Simple check: 5=Saturday, 6=Sunday)
    if target_date.weekday() >= 5:
        logger.log("⚠️ Warning: Target date is a weekend. Markets are likely closed.")
        
    return target_date

def run_automation():
    logger = ConsoleLogger()
    logger.log("--- 🤖 STARTING MARKET LION AUTOMATION ---")

    # 1. Validate Environment Variables
    required_vars = [
        "TURSO_DB_URL", "TURSO_AUTH_TOKEN",
        "CAPITAL_X_CAP_API_KEY", "CAPITAL_IDENTIFIER", "CAPITAL_PASSWORD"
    ]
    
    missing = []
    for var in required_vars:
        if not os.environ.get(var):
            missing.append(var)
    
    if missing:
        logger.log("❌ Error: Missing environment variables.")
        logger.log(f"Missing keys: {', '.join(missing)}")
        sys.exit(1)

    # 2. Smart Date Selection
    target_date = determine_target_date(logger)

    # 3. Fetch Inventory
    logger.log("📦 Fetching symbol inventory from Turso...")
    db_map = get_symbol_map_from_db()
    
    if not db_map:
        logger.log("❌ Error: No symbols found in database or DB connection failed.")
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
        
        # Check for failures
        failures = report_df[report_df['Status'].str.contains("Failed")]
        if not failures.empty:
            logger.log(f"⚠️ WARNING: {len(failures)} symbols failed completely.")
        
        # Check for fallbacks
        fallbacks = report_df[report_df['Mode'].str.contains("Fallback")]
        if not fallbacks.empty:
            logger.log(f"⚠️ WARNING: {len(fallbacks)} symbols used Capital fallback (No Volume).")

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