from src.infisical_manager import InfisicalManager
import sys

def test_infisical():
    print("Testing Infisical Integration...")
    mgr = InfisicalManager()
    
    if not mgr.is_connected:
        print("❌ Failed to connect to Infisical.")
        # Try to print why
        # The __init__ prints status.
        return

    print("✅ Manager connected.")
    
    # Try to fetch one of the known keys
    key_name = "turso_emadprograms_analystworkbench_DB_URL"
    print(f"Fetching {key_name}...")
    secret = mgr.get_secret(key_name)
    
    if secret:
        print(f"✅ Successfully fetched secret! Value length: {len(secret)}")
    else:
        print(f"❌ Failed to fetch secret: {key_name}")

if __name__ == "__main__":
    test_infisical()
