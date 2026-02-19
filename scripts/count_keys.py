from infisical_client import InfisicalClient, ClientSettings, AuthenticationOptions, UniversalAuthMethod, ListSecretsOptions
import os
import toml

def count_massive_keys():
    secrets_path = ".streamlit/secrets.toml"
    if not os.path.exists(secrets_path):
        print("❌ .streamlit/secrets.toml not found")
        return

    data = toml.load(secrets_path)
    sec = data.get("infisical", {})
    client_id = sec.get("client_id")
    client_secret = sec.get("client_secret")
    project_id = sec.get("project_id")

    auth_method = UniversalAuthMethod(client_id=client_id, client_secret=client_secret)
    options = AuthenticationOptions(universal_auth=auth_method)
    client = InfisicalClient(ClientSettings(auth=options))

    try:
        secrets = client.listSecrets(options=ListSecretsOptions(
            project_id=project_id,
            environment="dev",
            path="/"
        ))
        
        massive_keys = []
        for s in secrets:
            key_name = s.secret_key.lower()
            if "massive" in key_name and "api_key" in key_name:
                massive_keys.append(s.secret_key)
            elif key_name.startswith("massive-") and len(s.secret_key) > 8:
                massive_keys.append(s.secret_key)
                
        print(f"Total Massive Keys Found: {len(massive_keys)}")
        for i, k in enumerate(massive_keys):
            print(f"{i+1}. {k}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    count_massive_keys()
