"""
Configuration and constants for the market data harvester.
"""
from pytz import timezone


# Timezone Configuration
US_EASTERN = timezone('US/Eastern')
BAHRAIN_TZ = timezone('Asia/Bahrain')
UTC = timezone('UTC')

# Data Schema
SCHEMA_COLS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session', 'source']

# Binance Configuration
BINANCE_DOMAINS = [
    "https://api.binance.us",
    "https://api1.binance.com", 
    "https://api2.binance.com", 
    "https://api3.binance.com", 
    "https://api.binance.com"
]

# Approved real-time ingest scope (v5.0): 19 single-stock US equities.
# Source of truth for the Databento gap-fill scope; the lake registry holds
# exactly these symbols (SYMB-01).
APPROVED_EQUITY_SYMBOLS = (
    "AAPL", "ADBE", "AMD", "AMZN", "APP", "AVGO", "BABA", "GOOGL", "META",
    "MSFT", "MU", "NDAQ", "NVDA", "ORCL", "PANW", "QCOM", "SHOP", "TSLA", "TSM",
)
