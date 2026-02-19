"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import logging
from datetime import datetime, timedelta
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_storage
from src.data.harvester import run_harvest_logic
from src.config import US_EASTERN
from src.utils.discord import send_discord_harvest_report

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------

from src.utils.logger import CLILogger

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        # Initialize database
        init_db()
        
        # Setup parameters
        # Schedule logic: Run at 6 AM Bahrain (10 PM ET / 11 PM ET)
        # At this time, 'today' ET is the market session that just concluded.
        now_et = datetime.now(US_EASTERN)
        target_date = now_et.date()
        
        # Weekend Check: If it's Saturday/Sunday morning ET, we don't expect new data usually, 
        # but the workflow is scheduled Tue-Sat Bahrain (Mon-Fri ET).
        print(f"🌍 Running Harvest at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (Local)")
        print(f"🗽 ET Time: {now_et.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 Target Market Date: {target_date}")

        # 2. Get Discord Webhook from Infisical
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        discord_webhook = mgr.get_secret("discord_data_harvest_cli_webhook_url")
        if discord_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = discord_webhook

        # 3. Fetch Inventory
        symbol_map = get_symbol_map_from_db()
        inventory_list = list(symbol_map.keys())
        
        # Create CLI logger
        logger = CLILogger()

        if not inventory_list:
            logger.log("⚠️ Symbol inventory is empty. Nothing to harvest.")
            # FORCE EXIT:
            sys.stdout.flush() 
            os._exit(0)
            
        # Harvest full day for all symbols
        logger.log(f"Starting harvest for {len(inventory_list)} symbols on {target_date}")
        
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            target_date=target_date,
            db_map=symbol_map,
            logger=logger,
            harvest_mode="🚀 Full Day"
        )
        
        # Save data if successful
        if not final_df.empty:
            if save_data_to_storage(final_df, logger):
                logger.log(f"✅ Data successfully harvested and saved to dual storage. Total rows: {len(final_df)}")
            else:
                logger.log("❌ Failed to save data to storage.")
        else:
            logger.log("⚠️ No data harvested.")
        
        # Print summary and send Discord notification
        if not report_df.empty:
            print("\n📊 Harvest Summary:")
            summary_str = report_df.to_string(index=False)
            print(summary_str)
            
            # Send to Discord
            if send_discord_harvest_report(report_df, target_date, len(final_df)):
                logger.log("📨 Discord notification sent.")
            else:
                # Only log if a webhook was provided but it failed
                if os.getenv("DISCORD_WEBHOOK_URL"):
                    logger.log("⚠️ Discord notification failed.")

    except KeyboardInterrupt:
        print("\n🛑 Harvest interrupted by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        # The Infisical client spins up background threads (aiohttp) that can
        # hang the script indefinitely. os._exit(0) kills them instantly.
        print("\n👋 Harvest complete. Exiting...")
        sys.stdout.flush() # Ensure all output is printed
        os._exit(0)
