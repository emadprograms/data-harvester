"""
CLI/automation worker script for scheduled data harvesting.
"""
from datetime import datetime
from src.database.schema import init_db
from src.database.operations import get_symbol_map_from_db, save_data_to_turso
from src.data.harvester import run_harvest_logic



import logging

# Suppress Streamlit's 'missing ScriptRunContext' warning aggressively
logging.getLogger('streamlit').setLevel(logging.ERROR)
logging.getLogger('streamlit.runtime.scriptrunner_utils.script_run_context').setLevel(logging.ERROR)
logging.getLogger('streamlit.runtime.state.session_state_proxy').setLevel(logging.ERROR)

class CLILogger:
    """Simple logger for CLI output."""
    def __init__(self):
        pass
    
    def log(self, message):
        print(f"🔹 {message}")


from src.config import US_EASTERN

# ...

if __name__ == "__main__":
    # Initialize database
    init_db()
    
    # Setup parameters
    # Use US/Eastern time to determine the "Trading Day"
    # If this runs at 01:15 UTC (20:15 ET prev day), it correctly picks 'yesterday' relative to UTC-midnight
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
