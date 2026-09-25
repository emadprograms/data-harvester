"""
Tests for src/config.py — Verify constants and timezone configurations.
"""
import unittest
import src.config as config_module
from pytz import timezone


class TestConfig(unittest.TestCase):

    def test_timezone_constants(self):
        """Standard timezones must be defined as pytz objects."""
        self.assertEqual(config_module.US_EASTERN.zone, 'US/Eastern')
        self.assertEqual(config_module.UTC.zone, 'UTC')

    def test_schema_cols(self):
        """Data schema must contain required market columns."""
        required = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session']
        for col in required:
            self.assertIn(col, config_module.SCHEMA_COLS)

    def test_binance_domains(self):
        """Binance domains list must be non-empty and contains valid URLs."""
        self.assertGreater(len(config_module.BINANCE_DOMAINS), 0)
        self.assertTrue(all(d.startswith("http") for d in config_module.BINANCE_DOMAINS))

    def test_no_capital_api_constant(self):
        """Capital.com has been removed — ensure no stale constant exists."""
        # This is a regression test to ensure we don't accidentally re-add it
        self.assertFalse(hasattr(config_module, 'CAPITAL_API_URL_BASE'),
                         "CAPITAL_API_URL_BASE should have been removed")
        self.assertFalse(hasattr(config_module, 'CAPITAL_X_CAP_API_KEY'),
                         "CAPITAL_X_CAP_API_KEY should have been removed")


if __name__ == "__main__":
    unittest.main()
