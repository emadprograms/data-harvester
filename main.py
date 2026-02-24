"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta, timezone
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_storage, clear_market_data_for_dates
from src.data.harvester import run_harvest_logic
from src.api.massive import MassiveProvider
from src.config import US_EASTERN
from src.utils.discord import send_discord_harvest_report, build_health_alerts
from src.utils.logger import CLILogger
from pandas.tseries.holiday import USFederalHolidayCalendar

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
    integrity_pre_msg = "Skipped"
    integrity_post_msg = "Skipped"
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
                
            # Rollback for Weekends and US Market Holidays
            cal = USFederalHolidayCalendar()
            holidays = cal.holidays(start=f"{target_date.year-1}-01-01", end=f"{target_date.year+1}-12-31").date
            
            while target_date.weekday() > 4 or target_date in holidays:
                reason = "Weekend" if target_date.weekday() > 4 else "Market Holiday"
                logger.log(f"⚠️ {reason} detected ({target_date}). Rolling back Target Date...")
                target_date -= timedelta(days=1)
            
            logger.log(f"🎯 Final Target Market Date: {target_date}")
        
        logger.log(f"🌍 Running Harvest at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
        logger.log(f"🎯 Target Market Date: {target_date}")

        # 1. Pre-Harvest Parity Check & Repair
        from src.utils.integrity import ensure_database_parity
        logger.log(f"🔍 Pre-Harvest Parity Check for {target_date}...")
        ok_pre, msg_pre = ensure_database_parity(archive_client, mirror_client, str(target_date), logger)
        integrity_pre_msg = msg_pre
        if not ok_pre:
            critical_errors += f"- Pre-Harvest Parity Failure: {msg_pre}\n"

        # 2. Fetch Inventory
        symbol_map = get_symbol_map_from_db(archive_client)
        inventory_list = list(symbol_map.keys())
        
        if not inventory_list:
            msg = "⚠️ Symbol inventory is empty. Nothing to harvest."
            logger.log(msg)
            critical_errors += f"- {msg}\n"
            return
            
        # 3. Harvest
        logger.log(f"🚀 Starting Harvest for {len(inventory_list)} symbols...")
        massive_provider = MassiveProvider(logger)
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            target_date=target_date,
            db_map=symbol_map,
            logger=logger,
            massive_provider=massive_provider
        )
        
        # 4. Dual Write & Post-Harvest Parity
        if final_df is not None and not final_df.empty:
            # Split data into Target Date vs Rogue Rows (Previous/Other days)
            if not pd.api.types.is_datetime64_any_dtype(final_df['timestamp']):
                final_df['timestamp'] = pd.to_datetime(final_df['timestamp'], utc=True)
            
            target_mask = final_df['timestamp'].dt.date == target_date
            target_df = final_df[target_mask].copy()
            rogue_df = final_df[~target_mask].copy()

            # A. Clean & Replace for TARGET DATE
            if not target_df.empty:
                logger.log(f"🧹 Clearing existing data for {target_date} before commit...")
                clear_market_data_for_dates(archive_client, [target_date], logger, "Archive")
                clear_market_data_for_dates(mirror_client, [target_date], logger, "Mirror")
                
                if save_data_to_storage(target_df, logger, archive_client=archive_client, mirror_client=mirror_client, mode="REPLACE"):
                    logger.log(f"✅ Target data ({target_date}) written to Archive & Mirror. Rows: {len(target_df)}")
                else:
                    msg = f"❌ Failed to save target data for {target_date}."
                    logger.log(msg)
                    critical_errors += f"- {msg}\n"

            # B. Fill Gaps for ROGUE ROWS (Previous days) - Use mode="IGNORE"
            if not rogue_df.empty:
                logger.log(f"📥 Filling gaps for {len(rogue_df)} rogue rows from other dates using IGNORE mode...")
                save_data_to_storage(rogue_df, logger, archive_client=archive_client, mirror_client=mirror_client, mode="IGNORE")

            # C. Verification (Parity check targets the explicitly requested date)
            logger.log(f"🔍 Post-Harvest Parity Check for {target_date}...")
            from src.utils.integrity import ensure_database_parity
            ok_post, msg_post = ensure_database_parity(archive_client, mirror_client, str(target_date), logger)
            integrity_post_msg = msg_post
            
            if not ok_post:
                critical_errors += f"- Post-Harvest Parity Failure: {msg_post}\n"
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
                report_df = pd.DataFrame()
                
            now_et = datetime.now(US_EASTERN)
            health_alerts = build_health_alerts(report_df, now_et.hour)
            
            if os.getenv("SKIP_DISCORD") == "true":
                logger.log("📢 SKIP_DISCORD is true. Skipping Discord notification.")
            else:
                send_discord_harvest_report(
                    report_df=report_df, 
                    target_date=target_date, 
                    total_rows=rows,
                    file_path=log_filename,
                    health_alerts=health_alerts,
                    integrity_pre=integrity_pre_msg,
                    integrity_post=integrity_post_msg,
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
        else:
            sys.exit(0)

if __name__ == "__main__":
    main()
