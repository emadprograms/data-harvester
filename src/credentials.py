"""
Credentials management — reads secrets directly from .env and environment variables.
Replaces legacy Infisical cloud SDK with 100% local configuration.
"""
import os
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Ensure .env is loaded
load_dotenv()


def get_capital_credentials() -> Dict[str, Optional[str]]:
    """Retrieves Capital.com credentials from environment variables."""
    return {
        "api_key": os.getenv("CAPITAL_COM_X_CAP_API_KEY") or os.getenv("CAPITAL_API_KEY"),
        "identifier": os.getenv("CAPITAL_COM_IDENTIFIER") or os.getenv("CAPITAL_IDENTIFIER"),
        "password": os.getenv("CAPITAL_COM_PASSWORD") or os.getenv("CAPITAL_PASSWORD"),
    }


def get_massive_keys() -> List[str]:
    """
    Retrieves Massive / Polygon.io API keys from environment variables.
    Supports comma-separated keys in MASSIVE_API_KEYS or single MASSIVE_API_KEY.
    """
    raw_keys = os.getenv("MASSIVE_API_KEYS") or os.getenv("MASSIVE_API_KEY", "")
    if not raw_keys:
        return []
    return [k.strip() for k in raw_keys.split(",") if k.strip()]


def get_discord_webhook_url() -> Optional[str]:
    """Retrieves Discord webhook URL from environment variables."""
    return os.getenv("DISCORD_WEBHOOK_URL")
