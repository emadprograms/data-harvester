import pytest
from datetime import datetime, timedelta, date
from src.config import US_EASTERN

@pytest.fixture
def safe_test_date():
    """Returns a recent trading day (not a weekend) within the last 30 days."""
    now_et = datetime.now(US_EASTERN).date()
    # Go back 3 days to ensure data is likely finalized and available
    target = now_et - timedelta(days=3)
    
    # If target is weekend, roll back to Friday
    while target.weekday() > 4:
        target -= timedelta(days=1)
    
    return target

@pytest.fixture
def safe_test_date_str(safe_test_date):
    """Returns the safe test date as a YYYY-MM-DD string."""
    return safe_test_date.strftime("%Y-%m-%d")
