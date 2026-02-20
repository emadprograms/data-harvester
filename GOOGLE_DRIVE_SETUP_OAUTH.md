# Google Drive OAuth Setup Guide

Since you are using a personal Google account, we need to use **OAuth 2.0** so the harvester can use your personal 15GB storage quota.

---

## 1. Create OAuth Credentials
1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Select your project (`gen-lang-client-xxxx`).
3.  Go to **APIs & Services** > **Enabled APIs & Services**.
4.  Ensure **Google Drive API** is enabled.
5.  Go to **OAuth consent screen**:
    *   Choose **External**.
    *   Add your email and basic info.
    *   **Crucial**: Add your own email as a **Test User**.
    *   Publish the app (or keep it in testing, it doesn't matter since you are the test user).
6.  Go to **Credentials**:
    *   Click **+ CREATE CREDENTIALS** > **OAuth client ID**.
    *   Application type: **Desktop app**.
    *   Name: `Harvester CLI`.
    *   Click **Create**.
7.  Click the **Download JSON** icon for your new Client ID. Save it to your computer.

---

## 2. Generate Refresh Token
Run the helper script I created to generate your permanent refresh token:

```bash
python3 scripts/generate_gdrive_token.py
```

1.  It will ask for the path to the JSON file you just downloaded.
2.  A browser window will open. **Log in with your Google Account**.
3.  You might see a "Google hasn't verified this app" warning. Click **Advanced** > **Go to Harvester CLI (unsafe)**.
4.  Once finished, the script will print 3 values in your terminal.

---

## 3. Add to Infisical
Add these secrets to your project:
- `GDRIVE_CLIENT_ID` = (From the script output)
- `GDRIVE_CLIENT_SECRET` = (From the script output)
- `GDRIVE_REFRESH_TOKEN` = (From the script output)
- `gdrive_market_data_folder_id` = (The ID of your folder from the URL)

**Once these are added, the harvester will have native access to your personal drive!**
