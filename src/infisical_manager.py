from infisical_sdk import InfisicalSDKClient
import os
import toml
import threading
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class InfisicalManager:
    _instance = None
    _cache_lock = threading.Lock()

    def __new__(cls):
        with cls._cache_lock:
            if cls._instance is None:
                cls._instance = super(InfisicalManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if getattr(self, '_initialized', False):
            return

        self.client = None
        self.is_connected = False
        self._secrets_cache = {}

        # Load from Env or Secrets file
        client_id = os.getenv("INFISICAL_CLIENT_ID")
        client_secret = os.getenv("INFISICAL_CLIENT_SECRET")
        self.project_id = os.getenv("INFISICAL_PROJECT_ID")

        if not client_id:
            try:
                # Attempt to load from .streamlit/secrets.toml if env vars are missing
                secrets_path = ".streamlit/secrets.toml"
                if os.path.exists(secrets_path):
                    data = toml.load(secrets_path)
                    sec = data.get("infisical", {})
                    client_id = sec.get("client_id")
                    client_secret = sec.get("client_secret")
                    self.project_id = sec.get("project_id")
            except Exception:
                pass

        if client_id and client_secret and self.project_id:
            try:
                # Initialize Infisical Client
                self.client = InfisicalSDKClient(
                    host="https://app.infisical.com")
                self.client.auth.universal_auth.login(
                    client_id=client_id,
                    client_secret=client_secret
                )

                self.is_connected = True
                self._initialized = True
                print("✅ Infisical Connected")
            except Exception as e:
                print(f"❌ Infisical Connection Failed: {e}")
        else:
            missing = []
            if not client_id:
                missing.append("INFISICAL_CLIENT_ID")
            if not client_secret:
                missing.append("INFISICAL_CLIENT_SECRET")
            if not self.project_id:
                missing.append("INFISICAL_PROJECT_ID")
            print(
                f"❌ Infisical Credentials not found. Missing: {', '.join(missing)}")
            print(f"   Set them as environment variables or in .streamlit/secrets.toml")

    def get_secret(self, secret_name):
        if not self.is_connected:
            return None

        if secret_name in self._secrets_cache:
            return self._secrets_cache[secret_name]

        try:
            secret = self.client.secrets.get_secret_by_name(
                secret_name=secret_name,
                project_id=self.project_id,
                environment_slug="dev",
                secret_path="/"
            )
            # NOTE: New SDK uses secretValue attribute
            val = secret.secretValue
            self._secrets_cache[secret_name] = val
            return val
        except Exception:
            return None

    def get_massive_keys(self) -> list:
        """Retrieves all Polygon/Massive API keys matching the 'massive-' prefix."""
        if not self.is_connected:
            return []

        try:
            # List all secrets to find those matching 'massive-'
            response = self.client.secrets.list_secrets(
                project_id=self.project_id,
                environment_slug="dev",
                secret_path="/"
            )

            keys = []
            for s in response.secrets:
                if s.secretKey.startswith("massive-") or s.secretKey == "massive_api_key":
                    keys.append(s.secretValue)

            return keys
        except Exception as e:
            print(f"⚠️ Error fetching Massive keys: {e}")
            return []

    def get_turso_archive_creds(self) -> dict:
        """Retrieves Turso Stock Data Archive credentials."""
        return {
            "url": self.get_secret("turso_arshademad_stockdataarchive_db_url"),
            "token": self.get_secret("turso_arshademad_stockdataarchive_auth_token")
        }

    def get_turso_mirror_creds(self) -> dict:
        """Retrieves Turso Stock Data Archive Mirror 1 credentials."""
        return {
            "url": self.get_secret("turso_hamzaarshadalam_stockdataarchivemirror1_db_url"),
            "token": self.get_secret("turso_hamzaarshadalam_stockdataarchivemirror1_auth_token")
        }

    def get_capital_credentials(self) -> dict:
        """Retrieves Capital.com API credentials."""
        return {
            "api_key": self.get_secret("capital_com_x_cap_api_key"),
            "identifier": self.get_secret("capital_com_identifier"),
            "password": self.get_secret("capital_com_password")
        }
