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
        except Exception as e:
            print(f"❌ Missing Secret: {secret_name}")
            return None
