"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_storage
from src.data.harvester import run_harvest_logic
from src.config import US_EASTERN
from src.utils.discord import send_discord_harvest_report, build_health_alerts

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------

from src.utils.logger import CLILogger

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        
        # 0. Set up Session Logging
        from src.utils.logger import CLILogger
        log_filename = f"logs/harvest_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        logger = CLILogger(log_path=log_filename)
        
        # 0. Pre-Harvest Cleanup & Download
        from src.utils.gdrive import upload_to_gdrive_oauth, download_from_gdrive_oauth
        client_id = mgr.get_secret("emadprograms_market_data_gdrive_client_id")
        client_secret = mgr.get_secret("emadprograms_market_data_gdrive_client_secret")
        refresh_token = mgr.get_secret("emadprograms_market_data_gdrive_refresh_token")
        gdrive_folder = mgr.get_secret("emadprograms_market_data_gdrive_folder_id")
        local_db_path = "market_data.db"
        
        # Clean slate: Remove any leftover DB from previous runs
        if os.path.exists(local_db_path):
            os.remove(local_db_path)
        
        # Always download from GDrive (Ephemeral mode)
        if all([client_id, client_secret, refresh_token, gdrive_folder]):
            download_from_gdrive_oauth(local_db_path, local_db_path, gdrive_folder, client_id, client_secret, refresh_token, logger)
        else:
            missing_keys = []
            if not client_id: missing_keys.append("client_id")
            if not client_secret: missing_keys.append("client_secret")
            if not refresh_token: missing_keys.append("refresh_token")
            if not gdrive_folder: missing_keys.append("folder_id")
            logger.log(f"⚠️ GDrive download skipped (Missing keys: {', '.join(missing_keys)})")

        # Initialize database (will create or use downloaded file)
        init_db()
        
        # Gap Filling / Repair from Turso
        try:
            from tools.migrate_historical_turso import repair_local_from_turso, get_remote_client, get_local_client
            remote = get_remote_client(mgr)
            local = get_local_client()
            repair_local_from_turso(remote, local, logger)
        except Exception as e:
            logger.log(f"⚠️ Gap repair skipped: {e}")

        # Setup parameters
        # Schedule logic: The trading day officially "rolls over" at 4 AM ET when pre-market opens.
        # If we run between Midnight and 4 AM ET, we want to fetch the previous day's finalized data.
        now_et = datetime.now(US_EASTERN)
        if now_et.hour < 4:
            target_date = (now_et - timedelta(days=1)).date()
            logger.log("🕒 Time is before 4 AM ET. Targeting previous trading day.")
        else:
            target_date = now_et.date()
        
        # Weekend Check: If it's Saturday/Sunday morning ET, we don't expect new data usually, 
        # but the workflow is scheduled Tue-Sat Bahrain (Mon-Fri ET).
        logger.log(f"🌍 Running Harvest at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        logger.log(f"🗽 ET Time: {now_et.strftime('%Y-%m-%d %H:%M:%S')} (Hour: {now_et.hour})")
        logger.log(f"🎯 Target Market Date: {target_date}")

        # 2. Get Discord Webhook from Infisical
        discord_webhook = mgr.get_secret("discord_data_harvest_cli_webhook_url")
        if discord_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = discord_webhook

        # 3. Fetch Inventory
        symbol_map = get_symbol_map_from_db()
        inventory_list = list(symbol_map.keys())
        
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
        integrity_msg = ""
        if not final_df.empty:
            if save_data_to_storage(final_df, logger):
                logger.log(f"✅ Data successfully harvested and saved to dual storage. Total rows: {len(final_df)}")
                
                # Integrity Check: Compare today's fingerprint between Turso and Local
                try:
                    from src.utils.integrity import verify_sync
                    from src.database.connection import get_db_connection, get_local_db_connection
                    turso_c = get_db_connection()
                    local_c = get_local_db_connection()
                    if turso_c and local_c:
                        _, integrity_msg = verify_sync(turso_c, local_c, str(target_date), logger)
                except Exception as e:
                    logger.log(f"⚠️ Integrity check skipped: {e}")
            else:
                logger.log("❌ Failed to save data to storage.")
        else:
            logger.log("⚠️ No data harvested.")
        
        # Print summary and send Discord notification
        if not report_df.empty:
            # Print to console/log
            logger.log("\n📊 Harvest Summary:")
            summary_str = report_df.to_string(index=False)
            print(summary_str)
            if logger.log_path:
                with open(logger.log_path, 'a', encoding='utf-8') as f:
                    f.write(f"\nSummary:\n{summary_str}\n")
            
            # Send to Discord with health alerts and integrity status
            # 6. Discord Notification
            total_rows = len(final_df)
            health_alerts = build_health_alerts(report_df, now_et.hour)
            if send_discord_harvest_report(report_df, target_date, total_rows,
                                           file_path=log_filename,
                                           health_alerts=health_alerts,
                                           integrity_status=integrity_msg):
                logger.log("📨 Discord notification sent with logs.")
            else:
                logger.log("⚠️ Discord notification skipped or failed.")

            # 7. Google Drive Sync (OAuth)
            if all([client_id, client_secret, refresh_token, gdrive_folder]):
                upload_to_gdrive_oauth(local_db_path, gdrive_folder, client_id, client_secret, refresh_token, logger)
            else:
                logger.log("⚠️ GDrive sync skipped (OAuth Secrets missing in Infisical)")

    except KeyboardInterrupt:
        print("\n🛑 Harvest interrupted by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        # Cleanup: Delete the local DB buffer before exit (Ephemeral architecture)
        local_db_path = "market_data.db"
        if os.path.exists(local_db_path):
            try:
                os.remove(local_db_path)
                # Also cleanup WAL/SHM files if they exist
                for ext in ["-wal", "-shm"]:
                    if os.path.exists(local_db_path + ext):
                        os.remove(local_db_path + ext)
                if logger: logger.log("🧹 Local DB buffer cleaned up.")
            except Exception as e:
                print(f"⚠️ Cleanup warning: {e}")

        # The Infisical client spins up background threads (aiohttp) that can
        # hang the script indefinitely. os._exit(0) kills them instantly.
        print("\n👋 Harvest complete. Exiting...")
        sys.stdout.flush() # Ensure all output is printed
        os._exit(0)
