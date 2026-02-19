"""
Tests for src/api/retry.py — Verify retry session configuration.
"""
import unittest
from src.api.retry import get_retry_session


class TestRetrySession(unittest.TestCase):

    def test_returns_session(self):
        """Must return a valid requests.Session."""
        import requests
        session = get_retry_session()
        self.assertIsInstance(session, requests.Session)

    def test_has_retry_adapter(self):
        """Session must have retry adapters mounted for both http and https."""
        session = get_retry_session()
        https_adapter = session.get_adapter("https://example.com")
        self.assertIsNotNone(https_adapter)
        # Check retry config
        self.assertEqual(https_adapter.max_retries.total, 3)

    def test_custom_retries(self):
        """Custom retry count must be respected."""
        session = get_retry_session(retries=5)
        adapter = session.get_adapter("https://example.com")
        self.assertEqual(adapter.max_retries.total, 5)

    def test_default_status_forcelist(self):
        """Default status forcelist must include 500, 502, 504."""
        session = get_retry_session()
        adapter = session.get_adapter("https://example.com")
        forcelist = adapter.max_retries.status_forcelist
        self.assertIn(500, forcelist)
        self.assertIn(502, forcelist)
        self.assertIn(504, forcelist)

    def test_429_not_in_default_forcelist(self):
        """429 must NOT be in retry list — key rotation handles rate limits."""
        session = get_retry_session()
        adapter = session.get_adapter("https://example.com")
        forcelist = adapter.max_retries.status_forcelist
        self.assertNotIn(429, forcelist)


if __name__ == '__main__':
    unittest.main()
