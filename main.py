"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import argparse
import pandas as pd
from datetime import datetime, timedelta, timezone
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_storage
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
        parser = argparse.ArgumentParser(description="Data Harvester CLI (Market Session Logic)")
        parser.add_argument("--date", type=str, help="Target date for harvest in YYYY-MM-DD format", default=None)
        args = parser.parse_args()

        now_et = datetime.now(US_EASTERN)

        if args.date:
            try:
                target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            except ValueError:
                msg = f"❌ Invalid date format provided: {args.date}."
                logger.log(msg)
                critical_errors += f"- {msg}\n"
                return
        else:
            # Default to today's date in ET
            target_date = now_et.date()

        # --- CALCULATE SESSION BOUNDARIES (ET -> UTC) ---
        # 1. Find the PREVIOUS trading day to determine session start
        cal = USFederalHolidayCalendar()
        # Look back up to 10 days to find a valid trading day
        search_start = target_date - timedelta(days=10)
        search_end = target_date - timedelta(days=1)
        holidays = cal.holidays(start=search_start, end=search_end).date
        
        prev_trading_day = target_date - timedelta(days=1)
        while prev_trading_day.weekday() > 4 or prev_trading_day in holidays:
            prev_trading_day -= timedelta(days=1)

        # Session Start: 8:01:00 PM ET of previous trading day
        session_start_et = US_EASTERN.localize(datetime.combine(prev_trading_day, datetime.strptime("20:01:00", "%H:%M:%S").time()))
        # Session End: 8:00:00 PM ET of target trading day
        session_end_et = US_EASTERN.localize(datetime.combine(target_date, datetime.strptime("20:00:00", "%H:%M:%S").time()))

        # Convert to UTC for logic
        session_start_utc = session_start_et.astimezone(timezone.utc)
        session_end_utc = session_end_et.astimezone(timezone.utc)

        logger.log(f"🎯 Market Date: {target_date}")
        logger.log(f"⏰ Session Range (ET) : {session_start_et.strftime('%Y-%m-%d %H:%M:%S')} to {session_end_et.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.log(f"🌍 Session Range (UTC): {session_start_utc.strftime('%Y-%m-%d %H:%M:%S')} to {session_end_utc.strftime('%Y-%m-%d %H:%M:%S')}")

        # 1. Pre-Harvest Parity Check (Using target_date for reference)
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
            start_dt=session_start_utc,
            end_dt=session_end_utc,
            db_map=symbol_map,
            logger=logger,
            massive_provider=massive_provider
        )
        
        # 4. Dual Write & Post-Harvest Parity
        if final_df is not None and not final_df.empty:
            # --- SOURCE-AWARE CLEANING (TIERED DELETION) ---
            # Rule: Only wipe symbols if we have a fresh Tier-1 (Authoritative) replacement.
            # Tier-2 data (Capital/Yahoo) is stacked incrementally without deleting morning data.
            tier_1_symbols = final_df[final_df['source'].isin(['MASSIVE', 'BINANCE'])]['symbol'].unique().tolist()
            
            if tier_1_symbols:
                from src.database.operations import clear_market_data_for_range
                logger.log(f"🧹 Tier-1 Upgrade: Clearing {len(tier_1_symbols)} symbols to ensure high-fidelity replacement...")
                clear_market_data_for_range(archive_client, session_start_utc, session_end_utc, logger, "Archive", symbols=tier_1_symbols)
                clear_market_data_for_range(mirror_client, session_start_utc, session_end_utc, logger, "Mirror", symbols=tier_1_symbols)
            else:
                logger.log("🔹 Incremental Mode: Only Tier-2 data found. Skipping Clean to allow data stacking.")

            if save_data_to_storage(final_df, logger, archive_client=archive_client, mirror_client=mirror_client):
                logger.log(f"✅ Session data written to Archive & Mirror. Rows: {len(final_df)}")
                logger.log(f"✅ Session data written to Archive & Mirror. Rows: {len(final_df)}")
                
                logger.log(f"🔍 Post-Harvest Parity Check for {target_date}...")
                ok_post, msg_post = ensure_database_parity(archive_client, mirror_client, str(target_date), logger)
                integrity_post_msg = msg_post
                
                if not ok_post:
                    critical_errors += f"- Post-Harvest Parity Failure: {msg_post}\n"
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
