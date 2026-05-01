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
    Main entry point for the single-database data harvesting engine.
    Writes only to the Archive database. Mirror is synced separately
    via the sync_mirror GitHub Actions workflow.
    """
    logger = None
    archive_client = None
    target_date = None
    final_df = None
    report_df = None
    critical_errors = ""
    log_filename = f"logs/harvest_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    
    try:
        # 0. Initialize Logger and Secrets
        logger = CLILogger(log_path=log_filename)
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        
        discord_webhook = mgr.get_secret("discord_captain_data_webhook_url")
        if discord_webhook:
            os.environ["DISCORD_WEBHOOK_URL"] = discord_webhook
            
        # 0. Initialize Database Client (Archive Only)
        from src.database.connection import get_archive_db_connection
        archive_client = get_archive_db_connection()

        if not archive_client:
            msg = "❌ CRITICAL: Could not connect to Turso Archive."
            logger.log(msg)
            critical_errors += f"- {msg}\n"
            return # Jump to finally for notification

        # Initialize database schema
        init_db(archive_client)
        
        # Parse command line arguments
        parser = argparse.ArgumentParser(description="Data Harvester CLI (Market Session Logic)")
        parser.add_argument("--date", type=str, help="Target date for harvest in YYYY-MM-DD format", default=None)
        args = parser.parse_args()

        now_et = datetime.now(US_EASTERN)

        # --- 1. DETERMINE TARGET DATE (SESSION END) ---
        # The Target Date represents the "Closing Day" of the session.
        # If today is a weekend or holiday, the "Session" belongs to the NEXT trading day.
        
        cal = USFederalHolidayCalendar()
        
        if args.date:
            try:
                initial_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            except ValueError:
                msg = f"❌ Invalid date format provided: {args.date}."
                logger.log(msg)
                critical_errors += f"- {msg}\n"
                return
        else:
            # Automatic: If > 20:00 ET, we are technically in the start of the *next* day's session.
            cutoff_time = datetime.strptime("20:00", "%H:%M").time()
            if now_et.time() > cutoff_time:
                initial_date = now_et.date() + timedelta(days=1)
            else:
                initial_date = now_et.date()

        # ROLL FORWARD: Ensure target_date is a valid trading day
        # (e.g., If Saturday, Target = Monday. If Holiday, Target = Next Day)
        target_date = initial_date
        # Look ahead up to 10 days
        search_start = target_date
        search_end = target_date + timedelta(days=10)
        holidays = cal.holidays(start=search_start, end=search_end).date
        
        while target_date.weekday() > 4 or target_date in holidays:
            target_date += timedelta(days=1)

        # --- 2. CALCULATE SESSION BOUNDARIES (ET -> UTC) ---
        # Session Start: Previous Trading Day @ 20:00 ET (The boundary)
        # Session End:   Target Trading Day @ 20:00 ET (The boundary)
        
        # Find PREVIOUS trading day relative to the VALID target_date
        # (e.g. If Target=Monday, Prev=Friday)
        prev_trading_day = target_date - timedelta(days=1)
        
        # We need a fresh holiday search for the lookback
        back_holidays = cal.holidays(start=prev_trading_day - timedelta(days=10), end=prev_trading_day).date
        
        while prev_trading_day.weekday() > 4 or prev_trading_day in back_holidays:
            prev_trading_day -= timedelta(days=1)

        # Session Start: 20:00 ET of prev trading day
        session_start_et = US_EASTERN.localize(datetime.combine(prev_trading_day, datetime.strptime("20:00", "%H:%M").time()))
        # Session End: 20:00 ET of target trading day
        session_end_et = US_EASTERN.localize(datetime.combine(target_date, datetime.strptime("20:00", "%H:%M").time()))

        # Convert to UTC for logic
        session_start_utc = session_start_et.astimezone(timezone.utc)
        session_end_utc = session_end_et.astimezone(timezone.utc)

        # --- 3. DETERMINE SESSION STATUS (ACTIVE vs COMPLETED) ---
        # If NOW is past the session end, the session is COMPLETED (Past).
        # If NOW is before the session end, the session is ACTIVE (Current).
        
        if now_et > session_end_et:
            session_status = "COMPLETED"
            harvest_mode_label = "🏁 Previous Session (Massive)"
        else:
            session_status = "ACTIVE"
            harvest_mode_label = "⚡ Current Session (Capital)"

        logger.log(f"🎯 Market Date: {target_date} ({session_status})")
        logger.log(f"⏰ Session Range (ET) : {session_start_et.strftime('%Y-%m-%d %H:%M')} to {session_end_et.strftime('%Y-%m-%d %H:%M')}")
        logger.log(f"🌍 Session Range (UTC): {session_start_utc.strftime('%Y-%m-%d %H:%M')} to {session_end_utc.strftime('%Y-%m-%d %H:%M')}")
        logger.log(f"⚙️  Harvest Mode: {harvest_mode_label}")

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
        
        # Pass session_status to harvest logic to control source switching
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            start_dt=session_start_utc,
            end_dt=session_end_utc,
            db_map=symbol_map,
            logger=logger,
            massive_provider=massive_provider,
            session_status=session_status
        )
        
        # 3. Write to Archive
        if final_df is not None and not final_df.empty:
            # --- TARGETED CLEANING ---
            # Rule: We only clear data for symbols where we have fresh High-Quality data (MASSIVE/BINANCE).
            # This prevents us from wiping Capital-only tickers (like UUP) which might not return data 
            # if we are >16h past the session.
            
            hq_symbols = final_df[final_df['source'].isin(['MASSIVE', 'BINANCE'])]['symbol'].unique().tolist()
            
            if hq_symbols:
                from src.database.operations import clear_market_data_for_range
                logger.log(f"🧹 High-Quality Replacement: Clearing {len(hq_symbols)} symbols before commit...")
                clear_market_data_for_range(archive_client, session_start_utc, session_end_utc, logger, "Archive", symbols=hq_symbols)
            else:
                logger.log(f"⤵️  Incremental Mode: No High-Quality sources. Appending data without clearing...")

            if save_data_to_storage(final_df, logger, archive_client=archive_client):
                logger.log(f"✅ Session data written to Archive. Rows: {len(final_df)}")
                
                # Visual Density Summary
                try:
                    logger.print_density_summary(final_df, session_start_utc, session_end_utc)
                except AttributeError:
                    pass # Fallback if logger doesn't have this method yet
            else:
                msg = "❌ Failed to save data to Archive."
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
            
            db_health_grid = ""
            if not report_df.empty and 'session_start_utc' in locals() and 'session_end_utc' in locals():
                try:
                    from src.database.operations import get_session_row_counts
                    from src.utils.discord import build_database_health_grid
                    
                    inventory_list = report_df['Ticker'].tolist()
                    db_counts = get_session_row_counts(archive_client, inventory_list, session_start_utc, session_end_utc)
                    
                    is_active = session_status == "ACTIVE" if 'session_status' in locals() else False
                    db_health_grid = build_database_health_grid(db_counts, inventory_list, session_start_utc, session_end_utc, is_active_session=is_active)
                except Exception as e:
                    logger.log(f"⚠️ Could not generate Database Health Grid: {e}")
            
            if os.getenv("SKIP_DISCORD") == "true":
                logger.log("📢 SKIP_DISCORD is true. Skipping Discord notification.")
            else:
                send_discord_harvest_report(
                    report_df=report_df, 
                    target_date=target_date, 
                    total_rows=rows,
                    file_path=log_filename,
                    health_alerts=health_alerts,
                    critical_errors=critical_errors,
                    db_health_grid=db_health_grid
                )

        if archive_client:
            try: archive_client.close()
            except: pass
        
        # Exit with error if any critical errors happened
        if critical_errors:
            sys.exit(1)
        else:
            sys.exit(0)

if __name__ == "__main__":
    main()
