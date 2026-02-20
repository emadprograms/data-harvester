"""
Configuration and constants for the market data harvester.
"""
from pytz import timezone


# Timezone Configuration
US_EASTERN = timezone('US/Eastern')
BAHRAIN_TZ = timezone('Asia/Bahrain')
UTC = timezone('UTC')

# Data Schema
SCHEMA_COLS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session']

# Binance Configuration
BINANCE_DOMAINS = [
    "https://api.binance.com",
    "https://api.binance.us",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api-gcp.binance.com",
    "https://api-aws.binance.com"
]