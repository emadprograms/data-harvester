from infisical_client import InfisicalClient, ClientSettings, GetSecretOptions, AuthenticationOptions, UniversalAuthMethod
import os
import toml

class InfisicalManager:
    def __init__(self):
        self.client = None
        self.is_connected = False
        
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

        if client_id and client_secret:
            try:
                auth_method = UniversalAuthMethod(client_id=client_id, client_secret=client_secret)
                options = AuthenticationOptions(universal_auth=auth_method)
                self.client = InfisicalClient(ClientSettings(auth=options))
                self.is_connected = True
                print("✅ Infisical Connected")
            except Exception as e:
                print(f"❌ Infisical Auth Failed: {e}")
        else:
             print("❌ Infisical Credentials not found")

    def get_secret(self, secret_name):
        if not self.is_connected: 
            return None
        try:
            # NOTE: Use snake_case for options
            secret = self.client.getSecret(options=GetSecretOptions(
                secret_name=secret_name,
                project_id=self.project_id,
                environment="dev",
                path="/"
            ))
            # NOTE: Use snake_case for attribute access (.secret_value, NOT .secretValue)
            return secret.secret_value 
        except Exception:
            return None

    def get_massive_api_keys(self):
        """
        Retrieves all available Massive API keys for rotation.
        Logic: Checks for base name, then _1, _2, _3... up to _10.
        """
        keys = []
        
        # 1. Check base name
        k0 = self.get_secret("massive_stock_data_API_KEY")
        if k0: keys.append(k0)
            
        # 2. Check numbered variants 1-10
        for i in range(1, 11):
            ki = self.get_secret(f"massive_stock_data_API_KEY_{i}")
            if ki and ki not in keys: # Avoid duplicates if base and _1 are same
                keys.append(ki)
                
        return keys

    def get_twelve_data_key(self):
        """
        Retrieves Twelve Data API Key.
        User specified: twelve_data_stock_data_API_KEY_1
        """
        # Try specific user key first
        k = self.get_secret("twelve_data_stock_data_API_KEY_1")
        if k: return k
        
        # Fallback to standard
        return self.get_secret("twelve_data_stock_data_API_KEY")
