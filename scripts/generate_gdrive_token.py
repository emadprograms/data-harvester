import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow

# Scopes required for the app
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def generate_refresh_token(client_secrets_path):
    """
    Performs the OAuth flow to generate a refresh token.
    """
    if not os.path.exists(client_secrets_path):
        print(f"❌ File not found: {client_secrets_path}")
        return

    flow = InstalledAppFlow.from_client_secrets_file(client_secrets_path, SCOPES)
    creds = flow.run_local_server(port=0)

    print("\n✅ Authentication Successful!")
    print("-" * 40)
    print(f"CLIENT_ID: {creds.client_id}")
    print(f"CLIENT_SECRET: {creds.client_secret}")
    print(f"REFRESH_TOKEN: {creds.refresh_token}")
    print("-" * 40)
    print("\nCopy these values into Infisical!")

if __name__ == "__main__":
    path = input("Enter path to your client_secret_xxxx.json file: ").strip().strip("'").strip('"')
    generate_refresh_token(path)
