"""
Configuration and constants for the market data harvester.
"""
from pytz import timezone

# API Configuration
CAPITAL_API_URL_BASE = "https://api-capital.backend-capital.com/api/v1"

# Timezone Configuration
US_EASTERN = timezone('US/Eastern')
BAHRAIN_TZ = timezone('Asia/Bahrain')
UTC = timezone('UTC')

# Data Schema
SCHEMA_COLS = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session']
