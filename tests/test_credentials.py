"""
Tests for src/credentials.py — Verify local .env credential loading and parsing.
"""
import os
from unittest.mock import patch
from src.credentials import get_capital_credentials, get_massive_keys, get_discord_webhook_url


class TestCredentials:

    def test_get_capital_credentials_prefixed(self):
        with patch.dict(os.environ, {
            "CAPITAL_COM_X_CAP_API_KEY": "test_api_key",
            "CAPITAL_COM_IDENTIFIER": "test_user@example.com",
            "CAPITAL_COM_PASSWORD": "test_password_123"
        }, clear=True):
            creds = get_capital_credentials()
            assert creds["api_key"] == "test_api_key"
            assert creds["identifier"] == "test_user@example.com"
            assert creds["password"] == "test_password_123"

    def test_get_capital_credentials_fallback_keys(self):
        with patch.dict(os.environ, {
            "CAPITAL_API_KEY": "fallback_api_key",
            "CAPITAL_IDENTIFIER": "fallback_user@example.com",
            "CAPITAL_PASSWORD": "fallback_password"
        }, clear=True):
            creds = get_capital_credentials()
            assert creds["api_key"] == "fallback_api_key"
            assert creds["identifier"] == "fallback_user@example.com"
            assert creds["password"] == "fallback_password"

    def test_get_capital_credentials_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            creds = get_capital_credentials()
            assert creds["api_key"] is None
            assert creds["identifier"] is None
            assert creds["password"] is None

    def test_get_massive_keys_multiple(self):
        with patch.dict(os.environ, {
            "MASSIVE_API_KEYS": "key1, key2 , key3"
        }, clear=True):
            keys = get_massive_keys()
            assert keys == ["key1", "key2", "key3"]

    def test_get_massive_keys_single(self):
        with patch.dict(os.environ, {
            "MASSIVE_API_KEY": "single_key"
        }, clear=True):
            keys = get_massive_keys()
            assert keys == ["single_key"]

    def test_get_massive_keys_empty(self):
        with patch.dict(os.environ, {}, clear=True):
            keys = get_massive_keys()
            assert keys == []

    def test_get_discord_webhook_url(self):
        with patch.dict(os.environ, {
            "DISCORD_WEBHOOK_URL": "https://discord.com/api/webhooks/test"
        }, clear=True):
            url = get_discord_webhook_url()
            assert url == "https://discord.com/api/webhooks/test"

    @patch("requests.post")
    def test_capital_get_session_success(self, mock_post):
        """_get_session succeeds when valid env credentials exist and API responds 200."""
        import src.api.capital as capital_mod
        capital_mod._CAPITAL_SESSION = None

        mock_resp = patch("requests.Response").start()
        mock_resp.status_code = 200
        mock_resp.headers = {"CST": "MOCK_CST", "X-SECURITY-TOKEN": "MOCK_TOKEN"}
        mock_post.return_value = mock_resp

        with patch.dict(os.environ, {
            "CAPITAL_COM_X_CAP_API_KEY": "key123",
            "CAPITAL_COM_IDENTIFIER": "user@test.com",
            "CAPITAL_COM_PASSWORD": "pass"
        }, clear=True):
            session = capital_mod._get_session()
            assert session is not None
            assert session["CST"] == "MOCK_CST"
            assert session["X-SECURITY-TOKEN"] == "MOCK_TOKEN"
            assert session["api_key"] == "key123"

        capital_mod._CAPITAL_SESSION = None

    def test_capital_get_session_missing_credentials(self):
        """_get_session returns None when any credential is missing."""
        import src.api.capital as capital_mod
        capital_mod._CAPITAL_SESSION = None

        with patch.dict(os.environ, {}, clear=True):
            session = capital_mod._get_session()
            assert session is None

        capital_mod._CAPITAL_SESSION = None

    @patch("src.api.massive.RESTClient")
    def test_massive_provider_initialization_with_keys(self, mock_rest_client):
        """MassiveProvider initializes client pool from env keys and round-robins."""
        from src.api.massive import MassiveProvider
        from unittest.mock import MagicMock

        logger = MagicMock()
        with patch.dict(os.environ, {
            "MASSIVE_API_KEYS": "key_alpha, key_beta"
        }, clear=True):
            provider = MassiveProvider(logger)
            assert len(provider.api_keys) == 2
            assert len(provider.clients) == 2
            
            # Test round-robin
            c1 = provider._get_next_client()
            c2 = provider._get_next_client()
            c3 = provider._get_next_client()
            assert c1 == provider.clients[0]
            assert c2 == provider.clients[1]
            assert c3 == provider.clients[0]

    def test_massive_provider_no_keys_logs_warning(self):
        """MassiveProvider logs warning and has empty clients when no keys exist."""
        from src.api.massive import MassiveProvider
        from unittest.mock import MagicMock

        logger = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            provider = MassiveProvider(logger)
            assert provider.api_keys == []
            assert provider.clients == []
            assert provider._get_next_client() is None
            assert any("No Massive API keys found" in str(c) for c in logger.log.call_args_list)

