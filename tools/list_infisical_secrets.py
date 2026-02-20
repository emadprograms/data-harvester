from src.infisical_manager import InfisicalManager
import os

def list_all_secrets():
    mgr = InfisicalManager()
    if not mgr.is_connected:
        print("❌ Not connected to Infisical")
        return

    from infisical_client import ListSecretsOptions
    try:
        secrets = mgr.client.listSecrets(options=ListSecretsOptions(
            project_id=mgr.project_id,
            environment="dev",
            path="/"
        ))
        print(f"--- Found {len(secrets)} secrets ---")
        for s in secrets:
            # Inspection revealed SecretElement doesn't have secret_name. 
            # Trying common variations based on typical SDK patterns
            key = getattr(s, 'secret_key', getattr(s, 'secretKey', None))
            if key is None:
                # Try to see what it DOES have
                attrs = [a for a in dir(s) if not a.startswith('_')]
                print(f"DEBUG: {attrs}")
                break
            print(f"- {key}")
    except Exception as e:
        print(f"❌ Failed to list secrets: {e}")

if __name__ == "__main__":
    list_all_secrets()
