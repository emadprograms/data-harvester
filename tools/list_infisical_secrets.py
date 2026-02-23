from src.infisical_manager import InfisicalManager
import os

def list_all_secrets():
    mgr = InfisicalManager()
    if not mgr.is_connected:
        print("❌ Not connected to Infisical")
        return

    try:
        secrets = mgr.client.secrets.list_secrets(
            project_id=mgr.project_id,
            environment_slug="dev",
            secret_path="/"
        )
        print(f"--- Found {len(secrets)} secrets ---")
        for s in secrets:
            # New SDK uses secretKey and secretValue
            print(f"- {s.secretKey}")
    except Exception as e:
        print(f"❌ Failed to list secrets: {e}")

if __name__ == "__main__":
    list_all_secrets()
