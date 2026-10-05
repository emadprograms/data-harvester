"""Discord webhook transport for operational notifications.

Only the transport lives here: the notification content is built by the caller
(see `src/utils/notifications.py`). A webhook failure must never block or crash
ingestion, so every helper returns a boolean and swallows transport errors.
"""
import os

import requests


def _post_embed(webhook_url, embed_dict):
    """Helper to post a single embed to Discord."""
    if not webhook_url:
        return False
    try:
        payload = {"embeds": [embed_dict]}
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code not in [200, 204]:
            print(f"❌ Discord Embed Error {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Discord Post Error: {e}")
        return False


def _post_file(webhook_url, content, file_path):
    """Helper to post a file with a message to Discord."""
    if not webhook_url:
        return False
    try:
        with open(file_path, "rb") as f:
            files = {"file": (os.path.basename(file_path), f)}
            resp = requests.post(webhook_url, data={"content": content}, files=files, timeout=30)

        if resp.status_code not in [200, 204]:
            print(f"❌ Discord File Error {resp.status_code}: {resp.text}")
            return False
        return True
    except Exception as e:
        print(f"❌ Discord Post Error: {e}")
        return False
