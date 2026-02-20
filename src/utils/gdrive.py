import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def upload_to_gdrive(file_path: str, folder_id: str, service_account_json_str: str, logger=None):
    """
    Uploads a file to a specific Google Drive folder using a Service Account.
    If a file with the same name exists in the folder, it updates it.
    """
    if not service_account_json_str or not folder_id:
        if logger: logger.log("⚠️ GDrive Config missing (JSON or Folder ID)")
        return False

    try:
        # Load credentials
        info = json.loads(service_account_json_str)
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)

        file_name = os.path.basename(file_path)

        # 1. Search for existing file with same name in this folder
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])

        media = MediaFileUpload(file_path, resumable=True)

        if items:
            # Update existing file
            file_id = items[0]['id']
            if logger: logger.log(f"🔄 Updating existing GDrive file: {file_name} ({file_id})")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            # Create new file
            if logger: logger.log(f"📤 Uploading new file to GDrive: {file_name}")
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        if logger: logger.log("✅ GDrive Sync Complete")
        return True

    except Exception as e:
        err = f"GDrive Sync Error: {e}"
        if logger: logger.log(f"❌ {err}")
        else: print(err)
        return False
