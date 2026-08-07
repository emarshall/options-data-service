"""
Detects missing 1-minute bars for a continuously-quoted series (an
underlying ticker) over a given range.

**Deliberately scoped to underlying bars, not option contracts.** An
underlying index/ETF is expected to have a bar for essentially every
regular-trading-hours minute — a missing one is a real gap (an outage, or
this exact codebase's own truncation bug fixed alongside this module, see
PLAN.md Section 7). An option contract is not: it's completely normal for
a contract to have no quote activity for many consecutive minutes, so
"missing minute" isn't a meaningful gap signal there the way it is for an
underlying — applying this same logic to option contracts would flag
thousands of false positives that are just quiet trading, not lost data.
If per-contract coverage auditing is ever needed, it wants a different
definition of "gap" (e.g. "no bar for an entire session" rather than "no
bar for a minute") — deliberately not built here to avoid a half-fitting
generalization.

**Known simplification: does not account for market holidays or early
closes.** Expected minutes are generated for every weekday's full regular
session (09:30-16:00 America/New_York) — a holiday will show up as a
"gap" spanning the whole session, which is a false positive, not a real
data problem. Filtering holidays properly needs a maintained holiday
calendar (NYSE's isn't a fixed rule — Good Friday, occasional one-off
closures); left out deliberately rather than hand-rolling an
inevitably-incomplete list. In practice this just means: a reported gap
that exactly spans one full session on a plausible holiday is very likely
not a real problem, and it's on the reader to recognize that until this
gets a real calendar.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


@dataclass(frozen=True)
class Gap:
    start: datetime  # first missing expected minute (inclusive), UTC
    end: datetime  # last missing expected minute (inclusive), UTC

    @property
    def minutes(self) -> int:
        return int((self.end - self.start).total_seconds() // 60) + 1


def expected_bar_minutes(start: datetime, end: datetime) -> list[datetime]:
    """Every regular-trading-hours minute (America/New_York, weekdays)
    between `start` and `end` (both UTC, inclusive), returned as UTC
    datetimes. See module docstring for the holiday caveat."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware")

    start_et = start.astimezone(MARKET_TZ)
    end_et = end.astimezone(MARKET_TZ)

    expected: list[datetime] = []
    day: date = start_et.date()
    while day <= end_et.date():
        if day.weekday() < 5:  # Mon-Fri; see module docstring re: holidays
            session_start = datetime.combine(day, MARKET_OPEN, tzinfo=MARKET_TZ)
            session_end = datetime.combine(day, MARKET_CLOSE, tzinfo=MARKET_TZ)
            minute = max(session_start, start_et)
            session_end = min(session_end, end_et)
            while minute <= session_end:
                expected.append(minute.astimezone(timezone.utc))
                minute += timedelta(minutes=1)
        day += timedelta(days=1)
    return expected


def find_gaps(
    existing_times: set[datetime], start: datetime, end: datetime, min_gap_minutes: int = 1
) -> list[Gap]:
    """Compares `existing_times` (bar timestamps actually present, UTC)
    against every expected minute in [start, end], and returns contiguous
    missing ranges as `Gap`s. Gaps shorter than `min_gap_minutes` are
    dropped — useful for tolerating the occasional single dropped tick
    that isn't worth reporting/reconciling on its own; default 1 reports
    everything.
    """
    expected = expected_bar_minutes(start, end)
    if not expected:
        return []

    gaps: list[Gap] = []
    gap_start: datetime | None = None
    prev: datetime | None = None

    for minute in expected:
        missing = minute not in existing_times
        if missing and gap_start is None:
            gap_start = minute
        elif not missing and gap_start is not None:
            gaps.append(Gap(start=gap_start, end=prev))
            gap_start = None
        prev = minute

    if gap_start is not None:
        gaps.append(Gap(start=gap_start, end=prev))

    return [g for g in gaps if g.minutes >= min_gap_minutes]
