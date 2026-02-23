"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import logging
import argparse
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
    Main entry point for the dual-database data harvesting engine.
    
    Cycle:
    1. Fetch paged data from Massive (Polygon.io).
    2. Commit to Turso Archive database.
    3. Commit to Turso Mirror database.
    4. Verify integrity using optimized MD5 check on both.
    5. Dispatches Discord notification and exits on "Clean MD5".
    """
    logger = None
    archive_client = None
    mirror_client = None
    
    try:
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        
        # Load Discord Webhook
        discord_webhook = mgr.get_secret("discord_captain_data_webhook_url")
        if discord_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = discord_webhook
            
        # 0. Set up Session Logging
        from src.utils.logger import CLILogger
        log_filename = f"logs/harvest_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
        logger = CLILogger(log_path=log_filename)
        
        # 0. Initialize Database Clients
        from src.database.connection import get_archive_db_connection, get_mirror_db_connection
        archive_client = get_archive_db_connection()
        mirror_client = get_mirror_db_connection()

        if not archive_client or not mirror_client:
            logger.log("❌ CRITICAL: Could not connect to Dual Turso setup. Exiting.")
            return

        # Initialize database schema
        init_db(archive_client)
        init_db(mirror_client)
        
        # Parse command line arguments
        parser = argparse.ArgumentParser(description="Data Harvester CLI (Dual Turso + Massive)")
        parser.add_argument("--date", type=str, help="Target date for harvest in YYYY-MM-DD format", default=None)
        args = parser.parse_args()

        now_et = datetime.now(US_EASTERN)

        if args.date:
            try:
                target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
                logger.log(f"📅 Using manually provided target date: {target_date}")
            except ValueError:
                logger.log(f"❌ Invalid date format provided: {args.date}. Expected YYYY-MM-DD.")
                return
        else:
            if now_et.hour < 4:
                target_date = (now_et - timedelta(days=1)).date()
                logger.log("🕒 Time is before 4 AM ET. Targeting previous trading day.")
            else:
                target_date = now_et.date()
                
            while target_date.weekday() > 4:
                target_date -= timedelta(days=1)
                logger.log(f"⚠️ Weekend detected. Rolling back Target Date to last trading day => {target_date}")
        
        logger.log(f"🌍 Running Harvest at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        logger.log(f"🗽 ET Time: {now_et.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.log(f"🎯 Target Market Date: {target_date}")

        # 1. Fetch Inventory
        symbol_map = get_symbol_map_from_db(archive_client)
        inventory_list = list(symbol_map.keys())
        
        if not inventory_list:
            logger.log("⚠️ Symbol inventory is empty. Nothing to harvest.")
            return
            
        # 2. Harvest from Massive (Polygon)
        logger.log(f"🚀 Starting Massive Paged Harvest for {len(inventory_list)} symbols...")
        
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            target_date=target_date,
            db_map=symbol_map,
            logger=logger,
            harvest_mode="🚀 Massive Paged"
        )
        
        # --- DUAL WRITE & INTEGRITY WORKFLOW ---
        integrity_msg = "Unknown"
        critical_errors = ""
        harvest_successful = False

        if not final_df.empty:
            # A. Dual Write
            if save_data_to_storage(final_df, logger, archive_client=archive_client, mirror_client=mirror_client):
                logger.log(f"✅ Data written to Archive & Mirror. Rows: {len(final_df)}")
                harvest_successful = True
                
                # B. Post-Harvest MD5 Integrity Check
                from src.utils.integrity import verify_db_md5
                
                logger.log("🔍 Optimized MD5 Integrity Check (Archive)...")
                archive_ok, archive_msg = verify_db_md5(archive_client, final_df, str(target_date), logger)
                
                logger.log("🔍 Optimized MD5 Integrity Check (Mirror)...")
                mirror_ok, mirror_msg = verify_db_md5(mirror_client, final_df, str(target_date), logger)
                
                integrity_msg = f"Archive: {archive_msg} | Mirror: {mirror_msg}"
                
                if archive_ok and mirror_ok:
                    logger.log("💎 CLEAN MD5 INTEGRITY CHECK. All systems green.")
                else:
                    msg = "❌ INTEGRITY MISMATCH DETECTED. Investigation required."
                    logger.log(msg)
                    critical_errors += f"- {msg}\n"
            else:
                err_msg = "❌ Failed to save data to dual storage."
                logger.log(err_msg)
                critical_errors += f"- {err_msg}\n"
        else:
            logger.log("⚠️ No data harvested.")
            
        # 3. Final Summary & Notification
        if harvest_successful or critical_errors or not report_df.empty:
            logger.log("\n📊 Harvest Summary:")
            summary_str = report_df.to_string(index=False) if not report_df.empty else "No Data"
            print(summary_str)
            
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
                logger.log("✅ Discord notification sent.")
            else:
                logger.log("⚠️ Discord notification failed.")

        # FINAL GATE: Only exit with code 0 if integrity is clean
        if (not final_df.empty and harvest_successful and archive_ok and mirror_ok) or (final_df.empty):
             print("\n👋 Clean Exit. Harvest complete.")
             sys.exit(0)
        else:
             print("\n⚠️ Exit with Errors.")
             sys.exit(1)

    except KeyboardInterrupt:
        print("\n🛑 Interrupted.")
    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        print(f"\n❌ Unexpected error:\n{err_msg}")
        if logger:
            logger.log(f"❌ Unexpected error:\n{err_msg}")
        
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
        if webhook_url:
            from src.utils.discord import _post
            safe_err = err_msg[-1900:]
            _post(webhook_url, f"🚨 **CRITICAL HARVEST FAILURE** 🚨\n```python\n{safe_err}\n```")
    finally:
        if archive_client:
            try: archive_client.close()
            except: pass
        if mirror_client:
            try: mirror_client.close()
            except: pass
        sys.stdout.flush()

if __name__ == "__main__":
    main()
