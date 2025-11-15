"""
Utility logger for writing status messages to Streamlit containers.
"""

class StreamlitLogger:
    """A simple logger that writes to a Streamlit container."""
    def __init__(self, container):
        self.container = container
    
    def log(self, message):
        if self.container:
            self.container.write(f"🔹 {message}")
        print(message)
