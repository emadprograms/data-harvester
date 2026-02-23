"""
Massive (Polygon.io) API implementation for market data.
"""
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.infisical_manager import InfisicalManager
from src.config import SCHEMA_COLS, UTC

class MassiveProvider:
    def __init__(self, logger):
        self.logger = logger
        mgr = InfisicalManager()
        self.api_keys = mgr.get_massive_keys()
        if not self.api_keys:
            self.logger.log("❌ No Massive API keys found in Infisical.")
        self.clients = [RESTClient(key) for key in self.api_keys]
        self._key_index = 0

    def _get_next_client(self):
        if not self.clients:
            return None
        client = self.clients[self._key_index]
        self._key_index = (self._key_index + 1) % len(self.clients)
        return client

    def fetch_data(self, ticker: str, target_date) -> pd.DataFrame:
        """
        Fetches 1-minute historical data for a ticker using Polygon REST SDK.
        Implements paged fetching to collect all data for the day.
        """
        client = self._get_next_client()
        if not client:
            return pd.DataFrame()

        start_time = datetime.combine(target_date, datetime.min.time())
        end_time = datetime.combine(target_date, datetime.max.time())
        
        # Polygon expects milliseconds since epoch or ISO formatted strings
        from_ts = int(start_time.timestamp() * 1000)
        to_ts = int(end_time.timestamp() * 1000)

        all_aggs = []
        try:
            # Query aggregates
            aggs = client.list_aggs(
                ticker=ticker,
                multiplier=1,
                timespan="minute",
                from_=from_ts,
                to=to_ts,
                limit=50000 # Large limit for daily data
            )
            
            for agg in aggs:
                all_aggs.append({
                    "timestamp": pd.to_datetime(agg.timestamp, unit="ms", utc=True),
                    "open": agg.open,
                    "high": agg.high,
                    "low": agg.low,
                    "close": agg.close,
                    "volume": agg.volume,
                    "symbol": ticker
                })
        except Exception as e:
            self.logger.log(f"   ⚠️ Massive Error for {ticker}: {e}")
            return pd.DataFrame()

        if not all_aggs:
            return pd.DataFrame()

        df = pd.DataFrame(all_aggs)
        df = df.sort_values("timestamp")
        return df

def fetch_massive_data(ticker: str, target_date, logger) -> pd.DataFrame:
    """Wrapper function for standalone Massive data fetching."""
    provider = MassiveProvider(logger)
    return provider.fetch_data(ticker, target_date)
