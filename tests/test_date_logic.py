import pytest
from datetime import datetime, date, timedelta
from src.config import US_EASTERN
from pandas.tseries.holiday import USFederalHolidayCalendar

def get_target_date(simulated_now_et: datetime) -> date:
    """Extracts the exact date logic from main.py for testing."""
    if simulated_now_et.hour < 20:
        target_date = (simulated_now_et - timedelta(days=1)).date()
    else:
        target_date = simulated_now_et.date()
        
    cal = USFederalHolidayCalendar()
    # Broad range for safety
    holidays = cal.holidays(start="2020-01-01", end="2030-12-31").date
    
    while target_date.weekday() > 4 or target_date in holidays:
        target_date -= timedelta(days=1)
        
    return target_date


class TestDateLogic:
    def test_weekday_after_8pm(self):
        # Wednesday, Feb 18, 2026, 9:00 PM ET -> Should be Feb 18
        simulated_now = datetime(2026, 2, 18, 21, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 18)

    def test_weekday_before_8pm(self):
        # Wednesday, Feb 18, 2026, 7:00 PM ET -> Should be Feb 17
        simulated_now = datetime(2026, 2, 18, 19, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 17)

    def test_weekend_rollback(self):
        # Saturday, Feb 21, 2026, 9:00 PM ET -> Rolls back to Friday, Feb 20
        simulated_now = datetime(2026, 2, 21, 21, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 20)

    def test_holiday_rollback(self):
        # Christmas Day 2025 (Thursday) -> Should roll back to Wednesday, Dec 24
        # (Assuming Christmas is a trading holiday)
        simulated_now = datetime(2025, 12, 25, 21, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2025, 12, 24)

    def test_monday_before_8pm_holiday_rollback(self):
        # Labor Day (Monday, Sep 1, 2025) at 10 AM ET
        # Before 8 PM ET -> target Sunday (yesterday)
        # Sunday is weekend -> target Saturday
        # Saturday is weekend -> target Friday, Aug 29
        simulated_now = datetime(2025, 9, 1, 10, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2025, 8, 29)
