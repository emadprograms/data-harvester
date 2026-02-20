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
