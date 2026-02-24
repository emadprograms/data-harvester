"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_storage
from src.data.harvester import run_harvest_logic
from src.api.massive import MassiveProvider
from src.config import US_EASTERN
from src.utils.discord import send_discord_harvest_report, build_health_alerts
from src.utils.logger import CLILogger

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------

def main():
    """
    Main entry point for the dual-database data harvesting engine.
    """
    logger = None
    archive_client = None
    mirror_client = None
    target_date = None
    final_df = None
    report_df = None
    critical_errors = ""
    integrity_msg = "Skipped"
    log_filename = f"logs/harvest_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    
    try:
        # 0. Initialize Logger and Secrets
        logger = CLILogger(log_path=log_filename)
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        
        discord_webhook = mgr.get_secret("discord_captain_data_webhook_url")
        if discord_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = discord_webhook
            
        # 0. Initialize Database Clients
        from src.database.connection import get_archive_db_connection, get_mirror_db_connection
        archive_client = get_archive_db_connection()
        mirror_client = get_mirror_db_connection()

        if not archive_client or not mirror_client:
            msg = "❌ CRITICAL: Could not connect to Dual Turso setup."
            logger.log(msg)
            critical_errors += f"- {msg}\n"
            return # Jump to finally for notification

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
                msg = f"❌ Invalid date format provided: {args.date}."
                logger.log(msg)
                critical_errors += f"- {msg}\n"
                return
        else:
            # Cut-off: If before 8 PM ET (Post-Market Close), target previous day.
            # If after 8 PM ET, target today.
            if now_et.hour < 20:
                target_date = (now_et - timedelta(days=1)).date()
                logger.log(f"🕒 Time is before 8 PM ET. Targeting previous trading day.")
            else:
                target_date = now_et.date()
                logger.log(f"🕒 Time is after 8 PM ET. Targeting today's harvest.")
                
            while target_date.weekday() > 4:
                target_date -= timedelta(days=1)
                logger.log(f"⚠️ Weekend detected. Rolling back Target Date to last trading day => {target_date}")
        
        logger.log(f"🌍 Running Harvest at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        logger.log(f"🎯 Target Market Date: {target_date}")

        # 1. Fetch Inventory
        symbol_map = get_symbol_map_from_db(archive_client)
        inventory_list = list(symbol_map.keys())
        
        if not inventory_list:
            msg = "⚠️ Symbol inventory is empty. Nothing to harvest."
            logger.log(msg)
            critical_errors += f"- {msg}\n"
            return
            
        # 2. Harvest
        logger.log(f"🚀 Starting Harvest for {len(inventory_list)} symbols...")
        massive_provider = MassiveProvider(logger)
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            target_date=target_date,
            db_map=symbol_map,
            logger=logger,
            massive_provider=massive_provider
        )
        
        # 3. Dual Write & Integrity
        if final_df is not None and not final_df.empty:
            if save_data_to_storage(final_df, logger, archive_client=archive_client, mirror_client=mirror_client):
                logger.log(f"✅ Data written to Archive & Mirror. Rows: {len(final_df)}")
                
                from src.utils.integrity import verify_db_md5
                ok_a, msg_a = verify_db_md5(archive_client, final_df, str(target_date), logger)
                ok_m, msg_m = verify_db_md5(mirror_client, final_df, str(target_date), logger)
                
                integrity_msg = f"Archive: {msg_a} | Mirror: {msg_m}"
                if not (ok_a and ok_m):
                    critical_errors += "- Integrity Check Failed.\n"
            else:
                msg = "❌ Failed to save data to dual storage."
                logger.log(msg)
                critical_errors += f"- {msg}\n"
        else:
            logger.log("⚠️ No data harvested.")

    except Exception as e:
        import traceback
        err_msg = traceback.format_exc()
        logger.log(f"❌ Unexpected error:\n{err_msg}") if logger else print(err_msg)
        critical_errors += f"- Unexpected System Error.\n"
        
    finally:
        # Final summary and notification (ALWAYS RUNS)
        if target_date and (report_df is not None or critical_errors):
            rows = len(final_df) if final_df is not None else 0
            # report_df might be None if inventory was empty
            if report_df is None:
                import pandas as pd
                report_df = pd.DataFrame()
                
            now_et = datetime.now(US_EASTERN)
            health_alerts = build_health_alerts(report_df, now_et.hour)
            
            send_discord_harvest_report(
                report_df=report_df, 
                target_date=target_date, 
                total_rows=rows,
                file_path=log_filename,
                health_alerts=health_alerts,
                integrity_status=integrity_msg,
                critical_errors=critical_errors
            )

        if archive_client:
            try: archive_client.close()
            except: pass
        if mirror_client:
            try: mirror_client.close()
            except: pass
        
        # Exit with error if any critical errors happened
        if critical_errors:
            sys.exit(1)
        sys.exit(0)

if __name__ == "__main__":
    main()
