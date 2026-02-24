"""
Tests for src/infisical_manager.py — Verify SDK integration, credential loading, secret retrieval.
"""
import unittest
from unittest.mock import patch, MagicMock
from src.infisical_manager import InfisicalManager


class TestInfisicalManager(unittest.TestCase):

    def setUp(self):
        """Reset Singleton state for isolation."""
        InfisicalManager._instance = None

    @patch("src.infisical_manager.InfisicalSDKClient")
    @patch("os.getenv")
    def test_singleton_initialization(self, mock_getenv, mock_sdk_cls):
        """InfisicalManager must always return the same instance."""
        mock_getenv.side_effect = lambda k: "TEST_VAL" if "INFISICAL" in k else None
        
        mgr1 = InfisicalManager()
        mgr2 = InfisicalManager()
        self.assertIs(mgr1, mgr2)

    @patch("src.infisical_manager.InfisicalSDKClient")
    @patch("os.getenv")
    def test_massive_keys_retrieval(self, mock_getenv, mock_sdk_cls):
        """get_massive_keys must list secrets and filter by prefix."""
        mock_getenv.side_effect = lambda k: "TEST_VAL" if "INFISICAL" in k else None
        
        mock_sdk = MagicMock()
        mock_sdk_cls.return_value = mock_sdk
        
        # Mock List Secrets response
        mock_secret_1 = MagicMock()
        mock_secret_1.secretKey = "massive-1"
        mock_secret_1.secretValue = "key1"
        
        mock_secret_2 = MagicMock()
        mock_secret_2.secretKey = "massive-2"
        mock_secret_2.secretValue = "key2"
        
        mock_secret_3 = MagicMock()
        mock_secret_3.secretKey = "other-secret"
        
        mock_response = MagicMock()
        mock_response.secrets = [mock_secret_1, mock_secret_2, mock_secret_3]
        mock_sdk.secrets.list_secrets.return_value = mock_response
        
        mgr = InfisicalManager()
        keys = mgr.get_massive_keys()
        
        self.assertEqual(len(keys), 2)
        self.assertIn("key1", keys)
        self.assertIn("key2", keys)

    @patch("os.path.exists", return_value=False)
    @patch("os.getenv", return_value=None)
    def test_massive_keys_when_not_connected(self, mock_getenv, mock_exists):
        """get_massive_keys when not connected must return empty list."""
        mgr = InfisicalManager()
        keys = mgr.get_massive_keys()
        self.assertEqual(keys, [])

    @patch("src.infisical_manager.InfisicalSDKClient")
    @patch("os.getenv")
    def test_get_secret_caching(self, mock_getenv, mock_sdk_cls):
        """get_secret must cache results to avoid redundant network calls."""
        mock_getenv.side_effect = lambda k: "TEST_VAL" if "INFISICAL" in k else None
        
        mock_sdk = MagicMock()
        mock_sdk_cls.return_value = mock_sdk
        
        mock_secret = MagicMock()
        mock_secret.secretValue = "secret_val"
        mock_sdk.secrets.get_secret_by_name.return_value = mock_secret
        
        mgr = InfisicalManager()
        
        # First call hits "API"
        val1 = mgr.get_secret("test_secret")
        # Second call hits cache
        val2 = mgr.get_secret("test_secret")
        
        self.assertEqual(val1, "secret_val")
        self.assertEqual(val2, "secret_val")
        self.assertEqual(mock_sdk.secrets.get_secret_by_name.call_count, 1)


if __name__ == "__main__":
    unittest.main()
