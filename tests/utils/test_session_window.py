"""SCHED-01 — one timezone policy for the ingestion window.

The window is **weekdays 04:00–20:00 ET** (Mon–Fri, half-open: 04:00 is inside,
20:00 is outside). Every scheduling decision in the toolkit reads this module, so
these tests pin the boundary cases, the DST transitions, host-timezone
independence, and the injectable clock.
"""
from __future__ import annotations

import os
import time as _time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from src.utils.session_window import (
    EXCHANGE_TZ,
    NOW_OVERRIDE_ENV,
    WINDOW_END,
    WINDOW_START,
    describe,
    is_eligible,
    next_open,
    now_et,
    seconds_until_open,
)

ET = ZoneInfo("America/New_York")


def et(*parts) -> datetime:
    """Shorthand for an ET timestamp: et(2026, 10, 7, 4, 0)."""
    return datetime(*parts, tzinfo=ET)


# --------------------------------------------------------------- window bounds


def test_window_constants_are_the_owner_schedule():
    assert WINDOW_START.hour == 4 and WINDOW_START.minute == 0
    assert WINDOW_END.hour == 20 and WINDOW_END.minute == 0


@pytest.mark.parametrize(
    "moment, expected",
    [
        (et(2026, 10, 7, 3, 59, 59, 999999), False),  # Wednesday, one microsecond early
        (et(2026, 10, 7, 4, 0, 0), True),  # open, inclusive
        (et(2026, 10, 7, 9, 30), True),
        (et(2026, 10, 7, 19, 59, 59, 999999), True),  # last microsecond inside
        (et(2026, 10, 7, 20, 0, 0), False),  # close, exclusive
        (et(2026, 10, 7, 23, 59), False),
    ],
)
def test_weekday_boundaries_are_half_open(moment, expected):
    assert is_eligible(moment) is expected


@pytest.mark.parametrize(
    "moment, expected",
    [
        (et(2026, 10, 9, 19, 0), True),  # Friday before close
        (et(2026, 10, 9, 20, 30), False),  # Friday after close
        (et(2026, 10, 10, 12, 0), False),  # Saturday midday
        (et(2026, 10, 11, 12, 0), False),  # Sunday midday
        (et(2026, 10, 12, 4, 0), True),  # Monday open
        (et(2026, 10, 12, 3, 0), False),  # Monday pre-open
    ],
)
def test_weekends_are_never_eligible(moment, expected):
    assert is_eligible(moment) is expected


def test_naive_timestamps_are_read_as_exchange_local_time():
    """A naive timestamp is exchange-local, never host-local."""
    assert is_eligible(datetime(2026, 10, 7, 12, 0)) is True
    assert is_eligible(datetime(2026, 10, 7, 3, 0)) is False


def test_the_same_instant_is_judged_identically_in_any_host_timezone(monkeypatch):
    """SCHED-01: behaviour is identical when the host is not in ET."""
    moment = et(2026, 10, 7, 19, 30)
    for host_tz in ("UTC", "Asia/Bahrain", "America/Los_Angeles"):
        monkeypatch.setenv("TZ", host_tz)
        _time.tzset()
        try:
            assert is_eligible(moment) is True, host_tz
            assert is_eligible(moment.astimezone(ZoneInfo(host_tz))) is True, host_tz
        finally:
            monkeypatch.delenv("TZ", raising=False)
            _time.tzset()


# ------------------------------------------------------------------ next open


@pytest.mark.parametrize(
    "moment, expected",
    [
        (et(2026, 10, 7, 3, 0), et(2026, 10, 7, 4, 0)),  # same day, pre-open
        (et(2026, 10, 7, 21, 0), et(2026, 10, 8, 4, 0)),  # same night, next weekday
        (et(2026, 10, 9, 21, 0), et(2026, 10, 12, 4, 0)),  # Friday night -> Monday
        (et(2026, 10, 10, 9, 0), et(2026, 10, 12, 4, 0)),  # Saturday -> Monday
        (et(2026, 10, 11, 22, 0), et(2026, 10, 12, 4, 0)),  # Sunday night -> Monday
    ],
)
def test_next_open_skips_nights_and_weekends(moment, expected):
    assert next_open(moment) == expected


def test_next_open_is_the_earliest_eligible_instant():
    """At or after ``moment``: identity while open, later while closed."""
    for hour in range(0, 24):
        for day in (5, 6, 7, 9, 10, 11):  # Mon/Tue/Wed/Fri/Sat/Sun of that week
            moment = et(2026, 10, day, hour, 30)
            opening = next_open(moment)
            assert is_eligible(opening) is True, moment
            if is_eligible(moment):
                assert opening == moment, moment
            else:
                assert opening > moment, moment
                assert not is_eligible(opening - timedelta(seconds=1)), moment


def test_seconds_until_open_matches_the_timestamp_difference():
    friday_close = et(2026, 10, 9, 20, 0)
    assert seconds_until_open(friday_close) == pytest.approx(
        (et(2026, 10, 12, 4, 0) - friday_close).total_seconds()
    )
    assert seconds_until_open(friday_close) == pytest.approx(
        (next_open(friday_close) - friday_close).total_seconds()
    )


def test_seconds_until_open_is_zero_while_open():
    assert seconds_until_open(et(2026, 10, 7, 4, 0)) == 0.0
    assert seconds_until_open(et(2026, 10, 7, 12, 0)) == 0.0
    assert seconds_until_open(et(2026, 10, 7, 19, 59, 59)) == 0.0


# ------------------------------------------------------------------------ DST


def test_spring_forward_keeps_the_wall_clock_window():
    """2026-03-08 is the US spring-forward Sunday; Monday opens at 04:00 EDT."""
    monday = et(2026, 3, 9, 4, 0)
    assert monday.utcoffset() == timedelta(hours=-4)  # EDT
    assert is_eligible(monday) is True
    assert is_eligible(monday - timedelta(microseconds=1)) is False

    saturday = et(2026, 3, 7, 12, 0)
    assert saturday.utcoffset() == timedelta(hours=-5)  # EST
    opening = next_open(saturday)
    assert opening == monday
    # Wall clock reads 40 hours; the lost DST hour makes the elapsed time 39.
    assert (opening - saturday) == timedelta(hours=40)
    assert (
        opening.astimezone(ZoneInfo("UTC")) - saturday.astimezone(ZoneInfo("UTC"))
    ) == timedelta(hours=39)


def test_fall_back_keeps_the_wall_clock_window():
    """2026-11-01 is the US fall-back Sunday; Monday opens at 04:00 EST."""
    friday_close = et(2026, 10, 30, 20, 0)
    monday = et(2026, 11, 2, 4, 0)
    assert monday.utcoffset() == timedelta(hours=-5)  # EST
    assert next_open(friday_close) == monday
    # Wall clock reads 2d 8h (Fri 20:00 -> Mon 04:00); the repeated DST hour
    # makes the real elapsed time one hour longer.
    assert (monday - friday_close) == timedelta(days=2, hours=8)
    assert (
        monday.astimezone(ZoneInfo("UTC")) - friday_close.astimezone(ZoneInfo("UTC"))
    ) == timedelta(days=2, hours=9)


def test_close_boundary_survives_both_dst_days():
    for close_moment in (et(2026, 3, 9, 20, 0), et(2026, 11, 2, 20, 0)):
        assert is_eligible(close_moment) is False
        assert is_eligible(close_moment - timedelta(microseconds=1)) is True


# ------------------------------------------------------------- injectable clock


def test_now_et_returns_an_exchange_zone_timestamp():
    current = now_et()
    assert current.tzinfo is not None
    assert current.utcoffset() in (timedelta(hours=-4), timedelta(hours=-5))


def test_now_et_honours_an_explicit_clock():
    fixed = et(2026, 10, 7, 12, 0)
    assert now_et(clock=lambda: fixed) == fixed


def test_now_et_honours_the_environment_override(monkeypatch):
    monkeypatch.setenv(NOW_OVERRIDE_ENV, "2026-10-05T21:30:00")
    assert now_et() == et(2026, 10, 5, 21, 30)
    assert is_eligible(now_et()) is False


def test_now_et_override_accepts_an_aware_timestamp(monkeypatch):
    monkeypatch.setenv(NOW_OVERRIDE_ENV, "2026-10-06T01:30:00+00:00")
    assert now_et() == datetime(2026, 10, 6, 1, 30, tzinfo=ZoneInfo("UTC"))
    assert now_et().astimezone(ET) == et(2026, 10, 5, 21, 30)


def test_invalid_override_fails_loudly(monkeypatch):
    monkeypatch.setenv(NOW_OVERRIDE_ENV, "yesterday")
    with pytest.raises(ValueError) as excinfo:
        now_et()
    assert NOW_OVERRIDE_ENV in str(excinfo.value)


def test_explicit_clock_wins_over_the_override(monkeypatch):
    monkeypatch.setenv(NOW_OVERRIDE_ENV, "2026-10-05T21:30:00")
    fixed = et(2026, 10, 7, 12, 0)
    assert now_et(clock=lambda: fixed) == fixed


# ---------------------------------------------------------------- diagnostics


def test_describe_states_the_window_and_time_to_open():
    waiting = describe(et(2026, 10, 5, 21, 0))  # Monday night
    assert "WAITING_FOR_WINDOW" in waiting
    assert "04:00" in waiting or "04:00 ET" in waiting or "open" in waiting.lower()
    assert "2026-10-06" in waiting

    inside = describe(et(2026, 10, 6, 10, 0))
    assert "INGESTING" in inside
    assert "20:00" in inside


def test_no_holiday_calendar_is_applied():
    """Weekdays only, by design: a market holiday is still an eligible window."""
    christmas_friday = et(2026, 12, 25, 10, 0)
    assert christmas_friday.strftime("%A") == "Friday"
    assert is_eligible(christmas_friday) is True
