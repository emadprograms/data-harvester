from src.infisical_manager import InfisicalManager
from infisical_client import ListSecretsOptions

def list_all_secrets():
    mgr = InfisicalManager()
    if not mgr.is_connected:
        print("❌ Not connected")
        return
    
    try:
        secrets = mgr.client.listSecrets(options=ListSecretsOptions(
            project_id=mgr.project_id,
            environment="dev",
            path="/"
        ))
        if secrets:
            print(f"Attributes: {dir(secrets[0])}")
            for s in secrets:
                # Common candidates: secretKey, secret_key, key
                name = getattr(s, 'secret_key', getattr(s, 'secretKey', 'unknown'))
                print(f"- {name}")
    except Exception as e:
        print(f"❌ Error listing: {e}")

if __name__ == "__main__":
    list_all_secrets()
