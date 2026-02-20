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
BINANCE_DOMAINS = ["https://api.binance.com", "https://api.binance.us"]