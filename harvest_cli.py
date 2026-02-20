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

def main():
    """
    Main entry point for the stateless data harvesting engine.
    
    This function orchestrates a strictly ephemeral execution cycle designed for
    automated runs (e.g., via GitHub Actions). The cycle ensures data integrity
    by moving through the following phases:
    
    1. Pre-Harvest: Downloads the master SQLite database from Google Drive and
       establishes connections to the remote Turso database.
    2. Self-Heal: Merges any missing recent data from Turso into the local buffer
       to ensure the foundation is complete before the new harvest begins.
    3. Harvest: Fetches parallel market data across multiple sources (Capital.com, Binance, Yahoo)
       for the targeted trading day.
    4. Dual Write & Verify: Commits the new data to both the Turso remote and the
       local SQLite buffer, then performs a parity check to ensure they match.
    5. Sync: Uploads the updated local SQLite database back to Google Drive, strictly
       verifying integrity with an MD5 hash check.
    6. Notify & Clean: Dispatches a final Discord notification with the harvest summary
       and completely deletes the local database to leave no persistent state.
    """
    logger = None
    turso_client = None
    local_client = None
    
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

        # 0. Initialize Database Clients (Managed centrally for clean shutdown)
        from src.database.connection import get_db_connection, get_local_db_connection
        turso_client = get_db_connection()
        local_client = get_local_db_connection()

        if not turso_client:
            logger.log("❌ CRITICAL: Could not connect to Turso. Exiting.")
            return

        # Initialize database schema (using our managed clients)
        init_db(turso_client)
        if local_client:
            init_db(local_client)
        
        # Gap Filling / Repair from Turso (Self-Healing)
        # This merges data from previous mid-day runs into the local buffer
        if local_client:
            try:
                from tools.migrate_historical_turso import repair_local_from_turso
                logger.log("🔍 Checking for data gaps between Turso and local buffer...")
                repair_local_from_turso(turso_client, local_client, logger)
                logger.log("🔹 Local foundation verified/repaired from Turso.")
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
        discord_webhook = mgr.get_secret("discord_captain_data_webhook_url")
        if discord_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = discord_webhook

        # 2. Fetch Inventory
        symbol_map = get_symbol_map_from_db(turso_client)
        inventory_list = list(symbol_map.keys())
        
        if not inventory_list:
            logger.log("⚠️ Symbol inventory is empty. Nothing to harvest.")
            return
            
        # Harvest full day for all symbols
        logger.log(f"Starting harvest for {len(inventory_list)} symbols on {target_date}")
        
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            target_date=target_date,
            db_map=symbol_map,
            logger=logger,
            harvest_mode="🚀 Full Day"
        )
        
        # --- STRICT SYNC & NOTIFICATION WORKFLOW ---
        integrity_msg = "Unknown"
        critical_errors = ""
        harvest_successful = False

        if not final_df.empty:
            # A. Dual Write (Slowly/safely batched by save_data_to_storage)
            if save_data_to_storage(final_df, logger, turso_client=turso_client, local_client=local_client):
                logger.log(f"✅ Data written to dual storage. Rows: {len(final_df)}")
                harvest_successful = True
                
                # B. Post-Harvest Parity Check (Turso vs Local)
                from src.utils.integrity import verify_sync
                from tools.migrate_historical_turso import repair_local_from_turso
                
                logger.log("🔍 Post-Harvest Parity Check...")
                sync_ok, integrity_msg = verify_sync(turso_client, local_client, str(target_date), logger)
                
                if not sync_ok:
                    logger.log("⚠️ Sync drift detected after save. Performing targeted repair...")
                    repair_local_from_turso(turso_client, local_client, logger, force_exhaustive=True)
                    sync_ok, integrity_msg = verify_sync(turso_client, local_client, str(target_date), logger)
                
                if sync_ok:
                    logger.log("✅ Parity Verified (Turso vs Local).")
                else:
                    msg = "❌ Sync Mismatch persisted after repair. Check Turso logs."
                    logger.log(msg)
                    critical_errors += f"- {msg}\n"

                # C. Final GDrive Sync with MD5 Gate
                if all([client_id, client_secret, refresh_token, gdrive_folder]):
                    from src.utils.gdrive import upload_to_gdrive_oauth, get_local_md5, get_gdrive_md5
                    
                    max_retries = 3
                    md5_matched = False
                    
                    for attempt in range(1, max_retries + 1):
                        logger.log(f"☁️ Uploading to GDrive (Attempt {attempt}/{max_retries})...")
                        if upload_to_gdrive_oauth(local_db_path, gdrive_folder, client_id, client_secret, refresh_token, logger):
                            local_md5 = get_local_md5(local_db_path)
                            gdrive_md5 = get_gdrive_md5(local_db_path, gdrive_folder, client_id, client_secret, refresh_token, logger)
                            
                            if local_md5 == gdrive_md5:
                                logger.log(f"✅ GDrive MD5 MATCHED: {local_md5[:8]}...")
                                integrity_msg += " | GDrive MD5 OK"
                                md5_matched = True
                                break
                            else:
                                logger.log(f"⚠️ GDrive Sync MD5 Mismatch! Local: {local_md5}, Remote: {gdrive_md5}")
                                import time
                                time.sleep(2)
                        else:
                            logger.log("⚠️ Upload failed. Retrying...")
                            
                    if not md5_matched:
                        msg = "❌ CRITICAL: GDrive MD5 Mismatch persisted after maximum retries. Data may be out of sync."
                        logger.log(msg)
                        critical_errors += f"- {msg}\n"
                        integrity_msg += " | GDrive MD5 ❌"
                else:
                    logger.log("⚠️ GDrive sync skipped (OAuth Secrets missing)")
            else:
                err_msg = "❌ Failed to save data to storage (Finite Float error likely)."
                logger.log(err_msg)
                critical_errors += f"- {err_msg}\n"
        else:
            logger.log("⚠️ No data harvested.")
            
        # 3. Final Summary & SINGLE Discord Notification
        # Ensure we only send ONE ping to the user capturing EVERYTHING
        if harvest_successful or critical_errors or not report_df.empty:
            logger.log("\n📊 Harvest Summary:")
            summary_str = report_df.to_string(index=False) if not report_df.empty else "No Data"
            print(summary_str)
            if logger.log_path:
                with open(logger.log_path, 'a', encoding='utf-8') as f:
                    f.write(f"\nSummary:\n{summary_str}\n")
            
            total_rows = len(final_df) if not final_df.empty else 0
            health_alerts = build_health_alerts(report_df, now_et.hour) if not report_df.empty else ""
            
            logger.log("📨 Sending final unified Discord notification...")
            success = send_discord_harvest_report(
                report_df=report_df, 
                target_date=target_date, 
                total_rows=total_rows,
                file_path=log_filename,
                health_alerts=health_alerts,
                integrity_status=integrity_msg,
                critical_errors=critical_errors
            )
            
            if success:
                logger.log("✅ Discord notification sent successfully.")
            else:
                logger.log("⚠️ Discord notification skipped or failed.")

    except KeyboardInterrupt:
        print("\n🛑 Harvest interrupted by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        # 1. Close Database Connections
        if turso_client:
            try: turso_client.close()
            except: pass
        if local_client:
            try: local_client.close()
            except: pass

        # 2. Cleanup: Delete the local DB buffer before exit (Ephemeral architecture)
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

        print("\n👋 Harvest complete. Exiting...")
        sys.stdout.flush() # Ensure all output is printed

if __name__ == "__main__":
    main()
