from src.infisical_manager import InfisicalManager

def check_keys():
    mgr = InfisicalManager()
    
    # User mentioned "Massive" and "Polygon"
    keys_to_try = [
        "massive_stock_data_API_KEY",
        "polygon_api_key",
        "POLYGON_API_KEY",
        "polygon_io_api_key"
    ]
    
    print("Probing Infisical for Polygon/Massive keys...")
    for key in keys_to_try:
        val = mgr.get_secret(key)
        if val:
            masked = val[:4] + "*" * (len(val) - 8) + val[-4:] if len(val) > 8 else "****"
            print(f"✅ Found {key}: {masked}")
        else:
            print(f"❌ {key} not found")

if __name__ == "__main__":
    check_keys()
