import requests
import json

def create_capital_session(identifier, password, api_key):
    """
    Creates a session with Capital.com and returns the required authentication tokens.
    
    Args:
        identifier (str): Your Capital.com Login ID / Email.
        password (str): Your Capital.com Password.
        api_key (str): Your 'X-CAP-API-KEY'.
        
    Returns:
        dict: containing 'CST' and 'X-SECURITY-TOKEN' or None on failure.
    """
    url = "https://api-capital.backend-capital.com/api/v1/session"
    
    headers = {
        'X-CAP-API-KEY': api_key,
        'Content-Type': 'application/json'
    }
    
    payload = {
        "identifier": identifier,
        "password": password
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        
        # Check for success
        if response.status_code == 200:
            cst = response.headers.get('CST')
            x_security_token = response.headers.get('X-SECURITY-TOKEN')
            
            print("✅ Session Created Successfully!")
            print(f"CST Token: {cst}")
            print(f"X-Security-Token: {x_security_token}")
            
            return {
                "CST": cst,
                "X-SECURITY-TOKEN": x_security_token
            }
        else:
            print(f"❌ Failed to create session: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    # --- ENTER YOUR CREDENTIALS HERE TO TEST ---
    IDENTIFIER = "YOUR_LOGIN_ID" 
    PASSWORD = "YOUR_PASSWORD"
    API_KEY = "YOUR_API_KEY"
    
    if IDENTIFIER == "YOUR_LOGIN_ID":
        print("⚠️ Please edit the script with your real credentials to test.")
    else:
        create_capital_session(IDENTIFIER, PASSWORD, API_KEY)
