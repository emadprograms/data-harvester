import os
import io
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

def _get_gdrive_service(client_id, client_secret, refresh_token):
    """Internal helper to get the GDrive service with refreshed tokens."""
    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        client_id=client_id,
        client_secret=client_secret,
        token_uri="https://oauth2.googleapis.com/token"
    )
    if not creds.valid:
        creds.refresh(Request())
    return build('drive', 'v3', credentials=creds)

def download_from_gdrive_oauth(file_name: str, local_path: str, folder_id: str, client_id: str, client_secret: str, refresh_token: str, logger=None):
    """
    Downloads a specific file from GDrive to a local path.
    Returns True if downloaded, False if not found or error.
    """
    if not all([client_id, client_secret, refresh_token]):
        return False

    try:
        service = _get_gdrive_service(client_id, client_secret, refresh_token)
        
        # 1. Search for existing file
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])

        if not items:
            if logger: logger.log(f"ℹ️ File '{file_name}' not found on GDrive. Starting fresh.")
            return False

        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        
        with io.FileIO(local_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
        
        if logger: logger.log(f"📥 Downloaded existing '{file_name}' from GDrive.")
        return True

    except Exception as e:
        if logger: logger.log(f"⚠️ GDrive Download skipped: {e}")
        return False

def upload_to_gdrive_oauth(file_path: str, folder_id: str, client_id: str, client_secret: str, refresh_token: str, logger=None):
    """
    Uploads a file to a specific Google Drive folder using OAuth 2.0 Credentials.
    """
    if not all([client_id, client_secret, refresh_token]):
        if logger: logger.log("⚠️ GDrive OAuth Config missing")
        return False

    try:
        service = _get_gdrive_service(client_id, client_secret, refresh_token)
        file_name = os.path.basename(file_path)

        # 1. Search for existing file
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        results = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])

        media = MediaFileUpload(file_path, resumable=True)

        if items:
            file_id = items[0]['id']
            if logger: logger.log(f"🔄 Updating existing GDrive file: {file_name}")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            if logger: logger.log(f"📤 Uploading new file to GDrive: {file_name}")
            file_metadata = {'name': file_name, 'parents': [folder_id]}
            service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        if logger: logger.log("✅ GDrive OAuth Sync Complete")
        return True

    except Exception as e:
        err = f"GDrive OAuth Error: {e}"
        if logger: logger.log(f"❌ {err}")
        else: print(err)
        return False
