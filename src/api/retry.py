"""
Network retry logic for resilient API calls.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_retry_session(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 504)):
    """Creates a requests session with automatic retries."""
    session = requests.Session()
    retry = Retry(
        total=retries, read=retries, connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
