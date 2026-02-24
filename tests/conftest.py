import pytest
from datetime import datetime, timedelta, date, timezone
from src.config import US_EASTERN, UTC

@pytest.fixture
def safe_test_date():
    """Returns a recent trading day (not a weekend) within the last 30 days."""
    now_et = datetime.now(US_EASTERN).date()
    target = now_et - timedelta(days=2) # 2 days back to be safe
    while target.weekday() > 4:
        target -= timedelta(days=1)
    return target

@pytest.fixture
def safe_test_date_str(safe_test_date):
    """Returns the safe test date as a YYYY-MM-DD string."""
    return safe_test_date.strftime("%Y-%m-%d")

@pytest.fixture
def safe_test_range(safe_test_date):
    """
    Returns (start_dt, end_dt) in UTC for the 8 PM ET session 
    covering the safe_test_date.
    """
    # Previous trading day for session start
    prev = safe_test_date - timedelta(days=1)
    while prev.weekday() > 4:
        prev -= timedelta(days=1)
    
    start_et = US_EASTERN.localize(datetime.combine(prev, datetime.strptime("20:00", "%H:%M").time()))
    end_et = US_EASTERN.localize(datetime.combine(safe_test_date, datetime.strptime("20:00", "%H:%M").time()))
    
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)
