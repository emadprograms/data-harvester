from src.infisical_manager import InfisicalManager
import os

def search_secrets():
    mgr = InfisicalManager()
    if not mgr.is_connected:
        return

    # Try common names
    names = ["analyst_workbench_db_url", "analyst_workbench_auth_token", "ANALYST_WORKBENCH_URL", "ANALYST_WORKBENCH_TOKEN", "turso_analyst_workbench_url"]
    for n in names:
        val = mgr.get_secret(n)
        if val:
            print(f"Found '{n}': {val}")
        else:
            print(f"'{n}' not found.")

if __name__ == "__main__":
    search_secrets()
