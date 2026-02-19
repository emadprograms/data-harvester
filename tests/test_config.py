"""
Tests for src/config.py — Verify all constants are correct and complete.
"""
import unittest
from src.config import US_EASTERN, BAHRAIN_TZ, UTC, SCHEMA_COLS, BINANCE_DOMAINS


class TestConfig(unittest.TestCase):

    def test_timezones_resolve(self):
        """Timezones must be valid pytz timezone objects."""
        self.assertEqual(str(US_EASTERN), "US/Eastern")
        self.assertEqual(str(BAHRAIN_TZ), "Asia/Bahrain")
        self.assertEqual(str(UTC), "UTC")

    def test_schema_cols_complete(self):
        """Schema must have all 8 required columns in exact order."""
        expected = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'session']
        self.assertEqual(SCHEMA_COLS, expected)

    def test_schema_no_extras(self):
        """Schema must not have unexpected columns."""
        self.assertEqual(len(SCHEMA_COLS), 8)

    def test_binance_domains(self):
        """Binance must have at least one domain configured."""
        self.assertIsInstance(BINANCE_DOMAINS, list)
        self.assertGreater(len(BINANCE_DOMAINS), 0)
        for domain in BINANCE_DOMAINS:
            self.assertTrue(domain.startswith("https://"), f"Domain {domain} must use HTTPS")

    def test_no_capital_api_constant(self):
        """Capital.com has been removed — ensure no stale constant exists."""
        import src.config as config_module
        self.assertFalse(hasattr(config_module, 'CAPITAL_API_URL_BASE'), 
                         "CAPITAL_API_URL_BASE should have been removed")

    def test_no_streamlit_import(self):
        """Config must not import streamlit."""
        import inspect
        import src.config as config_mod
        source = inspect.getsource(config_mod)
        self.assertNotIn('streamlit', source)


if __name__ == '__main__':
    unittest.main()
