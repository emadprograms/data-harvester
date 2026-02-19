from infisical_client import InfisicalClient, ClientSettings, GetSecretOptions, AuthenticationOptions, UniversalAuthMethod
import os
import toml
import threading

class InfisicalManager:
    _instance = None
    _massive_keys_cache = None
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
                auth_method = UniversalAuthMethod(
                    client_id=client_id,
                    client_secret=client_secret
                )
                
                self.client = InfisicalClient(ClientSettings(
                    auth=AuthenticationOptions(
                        universal_auth=auth_method
                    )
                ))
                self.is_connected = True
                self._initialized = True
                # logger.log("✅ Infisical Connected") # Noisy in parallel
            except Exception as e:
                # logger.log(f"   ❌ Infisical Connection Failed: {e}")
                pass

    def get_secret(self, secret_name):
        if not self.is_connected: 
            return None
        
        if secret_name in self._secrets_cache:
            return self._secrets_cache[secret_name]
            
        try:
            # NOTE: Use snake_case for options
            secret = self.client.getSecret(options=GetSecretOptions(
                secret_name=secret_name,
                project_id=self.project_id,
                environment="dev",
                path="/"
            ))
            # NOTE: Use snake_case for attribute access (.secret_value, NOT .secretValue)
            val = secret.secret_value 
            self._secrets_cache[secret_name] = val
            return val
        except Exception:
            return None

    def get_massive_api_keys(self):
        """
        Retrieves all 9 identified Massive API keys for rotation.
        Uses a shared class-level cache to avoid redundant slow calls in parallel threads.
        """
        with self._cache_lock:
            if InfisicalManager._massive_keys_cache is not None:
                return InfisicalManager._massive_keys_cache
            
            keys = []
            
            # 1. New keys from "minephysical" / "Stock Data Archive"
            discovered_keys = [
                "massive-arshademad",
                "massive-fbbfecc3",
                "massive-ghf44378",
                "massive-emadarshadalam",
                "massive-arshadbah",
                "massive-dunola8439",
                "massive-fifamobile8439",
                "massive-emadarshadalam1",
                "massive-hamzaarshadalam"
            ]
            
            print(f"   🔐 Bootstrapping Massive keys from Infisical (First time sync)...")
            for nk in discovered_keys:
                val = self.get_secret(nk)
                if val: keys.append(val)

            # 2. Legacy/Standard keys fallback
            legacy_base = self.get_secret("massive_stock_data_API_KEY")
            if legacy_base and legacy_base not in keys: 
                keys.append(legacy_base)
                
            for i in range(1, 11):
                ki = self.get_secret(f"massive_stock_data_API_KEY_{i}")
                if ki and ki not in keys:
                    keys.append(ki)
            
            InfisicalManager._massive_keys_cache = keys
            print(f"   ✅ Successfully cached {len(keys)} Massive API keys.")
            return keys
