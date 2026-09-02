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

# **Underlying candle retention is count-based, not date-based — confirmed
# directly from real per-day density data (see PLAN.md Section 7), not
# Task 0's original ~6-week finding, which only checked whether *any*
# event appeared at a boundary date and never checked density. A real
# gap-reconcile report's `candles_per_day` breakdown showed a sharp
# cliff (near-zero straight to several hundred/day) at a DIFFERENT
# calendar date for each of three tickers — SPX ~27 days back, NDX ~23,
# VIX ~14 — with nothing in between but a single boundary-marker event.
# Multiplying each ticker's dense-window length by its own per-day candle
# count landed all three within ~7650-7950 total candles retained,
# despite the wildly different calendar-day windows and per-day rates
# (VIX alone is ~2x SPX's per-day volume) — strong evidence of a roughly
# fixed *count* of trailing candles kept per symbol, not a fixed number
# of days. A single day-count constant can only approximate this (this
# codebase doesn't model per-symbol, count-based retention), so this is
# set conservatively to the *shortest* observed dense window (VIX's ~14
# days) rather than an average — a higher-frequency symbol added later
# could plausibly have an even shorter effective window, but this is the
# best current evidence.
#
# Used to bound gap scanning/reconciliation: time before this cutoff can
# never have a bar no matter what, so treating it as an actionable gap
# the same way as a real one means `/gaps` and `gap-reconcile` would
# report — and `gap-reconcile` would keep "trying to fix" — the exact
# same permanently-unfillable stretch forever. This isn't a bug in this
# codebase; it's a real, upstream data-availability limit no amount of
# client-side request tuning can work around — see PLAN.md Section 7 for
# the full investigation (several rounds of genuine client-side bugs were
# found and fixed along the way, which is what made it possible to trust
# this final measurement rather than blaming the retention limit for
# what were, at the time, real bugs elsewhere).
RETENTION_DAYS = 14


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
