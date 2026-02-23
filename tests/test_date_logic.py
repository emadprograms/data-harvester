import pytest
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock
from src.config import US_EASTERN
import logging

# We need to test the logic directly or encapsulate it. 
# Since it's inside main() in main.py, let's create a functional test 
# that validates the core calculation logic directly here to ensure correctness.

def get_target_date(simulated_now_et: datetime) -> date:
    """Extracts the exact date logic from main.py for testing."""
    if simulated_now_et.hour < 4:
        target_date = (simulated_now_et - timedelta(days=1)).date()
    else:
        target_date = simulated_now_et.date()
        
    while target_date.weekday() > 4:  # If Saturday (5) or Sunday (6)
        target_date -= timedelta(days=1)
        
    return target_date


class TestDateLogic:
    def test_regular_weekday_after_4am(self):
        # Wednesday, Feb 18, 2026, 10:00 AM ET -> Should be Wednesday, Feb 18
        simulated_now = datetime(2026, 2, 18, 10, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 18)

    def test_regular_weekday_before_4am(self):
        # Wednesday, Feb 18, 2026, 2:00 AM ET -> Should be Tuesday, Feb 17
        simulated_now = datetime(2026, 2, 18, 2, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 17)

    def test_saturday_after_4am(self):
        # Saturday, Feb 21, 2026, 10:00 AM ET -> Should roll back to Friday, Feb 20
        simulated_now = datetime(2026, 2, 21, 10, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 20)

    def test_saturday_before_4am(self):
        # Saturday, Feb 21, 2026, 2:00 AM ET -> Initial target is Friday Feb 20 (Weekday=4). Returns Friday, Feb 20.
        simulated_now = datetime(2026, 2, 21, 2, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 20)

    def test_sunday_after_4am(self):
        # Sunday, Feb 22, 2026, 12:00 PM ET -> Initial target is Sunday Feb 22 (Weekday=6). Rolls back to Friday, Feb 20.
        simulated_now = datetime(2026, 2, 22, 12, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 20)
        
    def test_sunday_before_4am(self):
        # Sunday, Feb 22, 2026, 2:00 AM ET -> Initial target is Saturday Feb 21 (Weekday=5). Rolls back to Friday, Feb 20.
        simulated_now = datetime(2026, 2, 22, 2, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 20)
        
    def test_monday_before_4am(self):
        # Monday, Feb 23, 2026, 2:00 AM ET -> Initial target is Sunday Feb 22 (Weekday=6). Rolls back to Friday, Feb 20.
        simulated_now = datetime(2026, 2, 23, 2, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 20)
