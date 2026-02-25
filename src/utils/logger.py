import os
from datetime import datetime, timezone

class CLILogger:
    """A simple logger that prints to console and optionally a file."""
    def __init__(self, log_path=None):
        self.log_path = log_path
        if self.log_path:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            # Clear/Init file
            with open(self.log_path, 'w', encoding='utf-8') as f:
                f.write(f"--- Harvest Log Started: {datetime.now(timezone.utc).isoformat()} ---\n")
    
    def log(self, message):
        msg = f"🔹 {message}"
        print(msg)
        if self.log_path:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(f"{datetime.now(timezone.utc).isoformat()} | {message}\n")

    def print_density_summary(self, df, start_utc, end_utc):
        """Prints a visual density bar for each symbol in the dataframe."""
        if df.empty:
            return

        import pandas as pd
        from datetime import timedelta
        
        # Compute actual session duration from the boundaries passed in
        session_duration = end_utc - start_utc
        total_minutes = int(session_duration.total_seconds() // 60)
        if total_minutes <= 0:
            return

        # Scale slots to session length: 48 slots for a standard 24h session,
        # more for longer sessions (e.g. 144 for a 72h Monday session)
        mins_per_slot = 30
        num_slots = max(1, total_minutes // mins_per_slot)
        # Cap at 96 slots (2-day width) for readability; widen slots if needed
        max_slots = 96
        if num_slots > max_slots:
            mins_per_slot = total_minutes // max_slots
            num_slots = max_slots

        hours = total_minutes / 60
        self.log(f"\n📊 SESSION DATA DENSITY ({hours:.0f}h session, {num_slots} slots of {mins_per_slot}m)")
        
        symbols = sorted(df['symbol'].unique())

        for symbol in symbols:
            symbol_df = df[df['symbol'] == symbol]
            found_ts = pd.to_datetime(symbol_df['timestamp']).dt.tz_localize(None)
            
            # Remove TZ from start for comparison
            s_utc = start_utc.replace(tzinfo=None)
            
            bar = ""
            for i in range(num_slots):
                slot_start = s_utc + timedelta(minutes=i * mins_per_slot)
                slot_end = slot_start + timedelta(minutes=mins_per_slot)
                has_data = any((found_ts >= slot_start) & (found_ts < slot_end))
                bar += "█" if has_data else "░"
            
            row_count = len(symbol_df)
            self.log(f"{symbol.ljust(10)} [{bar}] {row_count}/{total_minutes}")
