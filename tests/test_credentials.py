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
