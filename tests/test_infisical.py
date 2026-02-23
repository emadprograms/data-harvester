"""
Tests for src/infisical_manager.py — Verify singleton, caching, and error handling.
"""
import unittest
from unittest.mock import patch, MagicMock
import os


class TestInfisicalManager(unittest.TestCase):

    def setUp(self):
        """Reset singleton for clean tests."""
        from src.infisical_manager import InfisicalManager
        InfisicalManager._instance = None

    @patch.dict(os.environ, {}, clear=True)
    @patch("src.infisical_manager.os.path.exists", return_value=False)
    def test_no_credentials_not_connected(self, mock_exists):
        """No env vars and no secrets file → must not crash, is_connected=False."""
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        self.assertFalse(mgr.is_connected)

    @patch.dict(os.environ, {}, clear=True)
    @patch("src.infisical_manager.os.path.exists", return_value=False)
    def test_get_secret_when_not_connected(self, mock_exists):
        """get_secret when not connected must return None."""
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        result = mgr.get_secret("some_key")
        self.assertIsNone(result)

    @patch.dict(os.environ, {}, clear=True)
    @patch("src.infisical_manager.os.path.exists", return_value=False)
    def test_get_capital_credentials_when_not_connected(self, mock_exists):
        """get_capital_credentials when not connected must return dict with None values."""
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        creds = mgr.get_capital_credentials()
        self.assertIsInstance(creds, dict)
        self.assertIsNone(creds.get("api_key"))

    @patch.dict(os.environ, {}, clear=True)
    @patch("src.infisical_manager.os.path.exists", return_value=False)
    def test_singleton_pattern(self, mock_exists):
        """Multiple instantiations must return the same object."""
        from src.infisical_manager import InfisicalManager
        mgr1 = InfisicalManager()
        mgr2 = InfisicalManager()
        self.assertIs(mgr1, mgr2)

    @patch.dict(os.environ, {}, clear=True)
    @patch("src.infisical_manager.os.path.exists", return_value=False)
    def test_secrets_cache(self, mock_exists):
        """Secrets must be cached after first retrieval."""
        from src.infisical_manager import InfisicalManager
        mgr = InfisicalManager()
        mgr.is_connected = True
        mgr.client = MagicMock()
        
        mock_secret = MagicMock()
        mock_secret.secretValue = "test_value"
        mgr.client.secrets.get_secret_by_name.return_value = mock_secret
        mgr.project_id = "test_project"
        
        # First call — should hit API
        val1 = mgr.get_secret("my_key")
        # Second call — should use cache
        val2 = mgr.get_secret("my_key")
        
        self.assertEqual(val1, "test_value")
        self.assertEqual(val2, "test_value")
        # API should only be called once (cached on second call)
        mgr.client.secrets.get_secret_by_name.assert_called_once()


if __name__ == '__main__':
    unittest.main()
