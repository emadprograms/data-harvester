"""
CLI/automation worker script for scheduled data harvesting.
"""
import os
import sys
import logging
from datetime import datetime
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_turso
from src.data.harvester import run_harvest_logic
from src.config import US_EASTERN

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------
class StreamlitWarningFilter(logging.Filter):
    """Drops Streamlit 'ScriptRunContext' and usage warnings."""
    def filter(self, record):
        msg = record.getMessage()
        return "ScriptRunContext" not in msg and "view this Streamlit app on a browser" not in msg

# Apply filter to noisy loggers
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").addFilter(StreamlitWarningFilter())
logging.getLogger("streamlit").addFilter(StreamlitWarningFilter())

class CLILogger:
    """Simple logger for CLI output."""
    def __init__(self):
        pass
    
    def log(self, message):
        print(f"🔹 {message}")

# -----------------------------------------------------------------------------
# Main Execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        # Initialize database
        init_db()
        
        # Setup parameters
        # Use US/Eastern time to determine the "Trading Day"
        today = datetime.now(US_EASTERN).date()
        symbol_map = get_symbol_map_from_db()
        inventory_list = list(symbol_map.keys())
        
        # Create CLI logger
        logger = CLILogger()
        
        # Harvest full day for all symbols
        logger.log(f"Starting harvest for {len(inventory_list)} symbols on {today}")
        
        final_df, report_df = run_harvest_logic(
            tickers_to_harvest=inventory_list,
            target_date=today,
            db_map=symbol_map,
            logger=logger,
            harvest_mode="🚀 Full Day"
        )
        
        # Save data if successful
        if not final_df.empty:
            if save_data_to_turso(final_df, logger):
                logger.log(f"✅ Data successfully harvested and saved. Total rows: {len(final_df)}")
            else:
                logger.log("❌ Failed to save data to database.")
        else:
            logger.log("⚠️ No data harvested.")
        
        # Print summary
        if not report_df.empty:
            print("\n📊 Harvest Summary:")
            print(report_df.to_string(index=False))

    except KeyboardInterrupt:
        print("\n🛑 Harvest interrupted by user.")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    finally:
        # FORCE EXIT:
        # Streamlit libraries (imported via src.api.capital etc) spin up background threads 
        # that can hang the script indefinitely. os._exit(0) kills them instantly.
        print("\n👋 Harvest complete. Exiting...")
        sys.stdout.flush() # Ensure all output is printed
        os._exit(0)
