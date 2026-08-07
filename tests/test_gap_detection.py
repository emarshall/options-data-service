from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from service.ingestion.gap_detection import MARKET_TZ, expected_bar_minutes, find_gaps


def et(y, m, d, hh, mm) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=MARKET_TZ)


def test_expected_bar_minutes_covers_full_session_only():
    # Wed 2026-07-22, full session 09:30-16:00 ET inclusive = 391 minutes.
    start = et(2026, 7, 22, 0, 0).astimezone(timezone.utc)
    end = et(2026, 7, 22, 23, 59).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    assert len(expected) == 391
    assert expected[0] == et(2026, 7, 22, 9, 30).astimezone(timezone.utc)
    assert expected[-1] == et(2026, 7, 22, 16, 0).astimezone(timezone.utc)


def test_expected_bar_minutes_skips_weekends():
    # Fri 2026-07-24 through Mon 2026-07-27 — no Saturday/Sunday minutes.
    start = et(2026, 7, 24, 0, 0).astimezone(timezone.utc)
    end = et(2026, 7, 27, 23, 59).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    days = {t.astimezone(MARKET_TZ).date().weekday() for t in expected}
    assert days == {4, 0}  # Friday and Monday only


def test_expected_bar_minutes_respects_partial_range():
    # Only asking for 10:00-10:05 ET on a Wednesday — 6 minutes, not the full session.
    start = et(2026, 7, 22, 10, 0).astimezone(timezone.utc)
    end = et(2026, 7, 22, 10, 5).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    assert len(expected) == 6


def test_expected_bar_minutes_requires_tz_aware_input():
    with pytest.raises(ValueError):
        expected_bar_minutes(datetime(2026, 7, 22), datetime(2026, 7, 23))


def test_find_gaps_no_gaps_when_everything_present():
    start = et(2026, 7, 22, 9, 30).astimezone(timezone.utc)
    end = et(2026, 7, 22, 9, 40).astimezone(timezone.utc)
    existing = set(expected_bar_minutes(start, end))
    assert find_gaps(existing, start, end) == []


def test_find_gaps_detects_a_single_missing_minute():
    start = et(2026, 7, 22, 9, 30).astimezone(timezone.utc)
    end = et(2026, 7, 22, 9, 40).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    missing_minute = expected[5]  # 09:35
    existing = set(expected) - {missing_minute}

    gaps = find_gaps(existing, start, end)

    assert len(gaps) == 1
    assert gaps[0].start == gaps[0].end == missing_minute
    assert gaps[0].minutes == 1


def test_find_gaps_merges_contiguous_missing_minutes_into_one_range():
    # 9a-11a present, nothing again until 12p — the exact scenario from the
    # bug report this feature was built for.
    start = et(2026, 7, 22, 9, 30).astimezone(timezone.utc)
    end = et(2026, 7, 22, 12, 0).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    morning_cutoff = et(2026, 7, 22, 11, 0).astimezone(timezone.utc)
    afternoon_start = et(2026, 7, 22, 12, 0).astimezone(timezone.utc)
    existing = {t for t in expected if t <= morning_cutoff or t >= afternoon_start}

    gaps = find_gaps(existing, start, end)

    assert len(gaps) == 1
    assert gaps[0].start == morning_cutoff + timedelta(minutes=1)
    assert gaps[0].end == afternoon_start - timedelta(minutes=1)
    assert gaps[0].minutes == 59


def test_find_gaps_multi_day_gap():
    # Bars on June 26, nothing again until July 20 — the other example
    # scenario from the bug report.
    june_26 = et(2026, 6, 26, 16, 0).astimezone(timezone.utc)
    july_20 = et(2026, 7, 20, 9, 30).astimezone(timezone.utc)
    start = et(2026, 6, 26, 9, 30).astimezone(timezone.utc)
    end = et(2026, 7, 20, 16, 0).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    existing = {t for t in expected if t <= june_26 or t >= july_20}

    gaps = find_gaps(existing, start, end)

    assert len(gaps) == 1
    assert gaps[0].start > june_26
    assert gaps[0].end < july_20
    assert gaps[0].minutes > 1000  # many trading days missing


def test_find_gaps_respects_min_gap_minutes_threshold():
    start = et(2026, 7, 22, 9, 30).astimezone(timezone.utc)
    end = et(2026, 7, 22, 9, 40).astimezone(timezone.utc)
    expected = expected_bar_minutes(start, end)
    existing = set(expected) - {expected[5]}  # one missing minute

    assert find_gaps(existing, start, end, min_gap_minutes=1) != []
    assert find_gaps(existing, start, end, min_gap_minutes=2) == []


def test_find_gaps_empty_range_returns_no_gaps():
    # A weekend-only range has no expected minutes at all, so trivially no gaps.
    start = et(2026, 7, 25, 0, 0).astimezone(timezone.utc)  # Saturday
    end = et(2026, 7, 26, 23, 59).astimezone(timezone.utc)  # Sunday
    assert find_gaps(set(), start, end) == []
