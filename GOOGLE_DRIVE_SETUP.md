# Google Drive Setup Guide

Follow these steps to generate the credentials needed for your data harvester.

---

## 1. Create a Google Drive Folder
1.  Go to your [Google Drive](https://drive.google.com/).
2.  Create a new folder (e.g., `Market Data Backups`).
3.  Open the folder and look at the **URL**.
4.  Copy the long string of characters at the end of the URL.
    *   Example URL: `https://drive.google.com/drive/u/0/folders/1A2B3C4D5E6F7G8H9I0J`
    *   **Folder ID**: `1A2B3C4D5E6F7G8H9I0J`
5.  **Save this ID** for your Infisical secret: `GDRIVE_FOLDER_ID`.

---

## 2. Create a Google Service Account
1.  Go to the [Google Cloud Console](https://console.cloud.google.com/).
2.  Create a new project (or select an existing one).
3.  Go to **APIs & Services** > **Library**.
4.  Search for **"Google Drive API"** and click **Enable**.
5.  Go to **APIs & Services** > **Credentials**.
6.  Click **+ CREATE CREDENTIALS** > **Service Account**.
7.  Give it a name (e.g., `data-harvester-sync`) and click **Create and Continue** > **Done**.
8.  In the Service Accounts list, click on the **Email** of the account you just created.
9.  Go to the **Keys** tab.
10. Click **ADD KEY** > **Create new key** > **JSON**.
11. This will download a `.json` file to your computer.
12. **Copy the entire content** of this JSON file for your Infisical secret: `GDRIVE_SERVICE_ACCOUNT_JSON`.

---

## 3. Share the Folder
1.  Inside your `.json` file, find the `"client_email"` (it looks like `data-harvester-sync@project-id.iam.gserviceaccount.com`).
2.  Go back to your Google Drive folder.
3.  Right-click the folder > **Share**.
4.  Paste the **Service Account Email** and give it **Editor** permissions.
5.  Click **Send**.

---

## 4. Add to Infisical
Add these two secrets to your project:
- `GDRIVE_FOLDER_ID` = (The ID from step 1)
- `GDRIVE_SERVICE_ACCOUNT_JSON` = (The content of the JSON file from step 2)

**That's it! Your next harvest run will now sync to this folder.**
