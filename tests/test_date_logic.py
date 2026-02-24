import pytest
from datetime import datetime, date, timedelta
from src.config import US_EASTERN
from pandas.tseries.holiday import USFederalHolidayCalendar

def get_target_date(simulated_now_et: datetime) -> date:
    """Extracts the exact date logic from main.py for testing.
    
    Rule: If past 20:00 ET, we are in the NEXT day's session.
    Otherwise, we are in today's session.
    No weekend/holiday rollback — the harvest handles non-trading
    days gracefully (equities return empty, crypto still harvests).
    """
    cutoff_time = datetime.strptime("20:00", "%H:%M").time()
    if simulated_now_et.time() > cutoff_time:
        return simulated_now_et.date() + timedelta(days=1)
    else:
        return simulated_now_et.date()


class TestDateLogic:
    def test_weekday_after_8pm(self):
        # Wednesday, Feb 18, 2026, 9:00 PM ET -> Next day's session = Feb 19
        simulated_now = datetime(2026, 2, 18, 21, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 19)

    def test_weekday_before_8pm(self):
        # Wednesday, Feb 18, 2026, 7:00 PM ET -> Today's session = Feb 18
        simulated_now = datetime(2026, 2, 18, 19, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 18)

    def test_exactly_at_8pm(self):
        # Exactly 20:00 is NOT past cutoff -> Today's session = Feb 18
        simulated_now = datetime(2026, 2, 18, 20, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 18)

    def test_weekend_after_8pm(self):
        # Saturday, Feb 21, 2026, 9:00 PM ET -> Next day = Feb 22 (Sunday)
        # No rollback in main.py; harvest handles non-trading days gracefully
        simulated_now = datetime(2026, 2, 21, 21, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2026, 2, 22)

    def test_holiday_after_8pm(self):
        # Christmas Day 2025 (Thursday) at 9PM -> Dec 26
        # No rollback; harvest returns empty for equities on holidays
        simulated_now = datetime(2025, 12, 25, 21, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2025, 12, 26)

    def test_monday_before_8pm(self):
        # Labor Day (Monday, Sep 1, 2025) at 10 AM ET -> Today = Sep 1
        simulated_now = datetime(2025, 9, 1, 10, 0, tzinfo=US_EASTERN)
        assert get_target_date(simulated_now) == date(2025, 9, 1)
