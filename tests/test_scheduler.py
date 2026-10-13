"""Tests for the after-hours maintenance scheduler.

Focus is on the pure, deterministic parts — the next-run-time calculation and
the job argv construction. The actual job execution is a thin subprocess
wrapper, exercised via the injectable clock/sleeper on `run_forever`.
"""

import asyncio
from datetime import datetime, time, timezone

import pytest

from service.ingestion.scheduler import (
    MARKET_TZ,
    build_jobs,
    next_run_at,
    parse_hhmm,
    run_forever,
)

RUN_TIME = time(16, 30)


def et(y, m, d, hh=0, mm=0, ss=0) -> datetime:
    return datetime(y, m, d, hh, mm, ss, tzinfo=MARKET_TZ)


def utc(y, m, d, hh=0, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# parse_hhmm
# --------------------------------------------------------------------------


def test_parse_hhmm():
    assert parse_hhmm("16:30") == time(16, 30)
    assert parse_hhmm("00:00") == time(0, 0)
    assert parse_hhmm("23:59") == time(23, 59)


@pytest.mark.parametrize("bad", ["16-30", "1630", "16:60", "24:00", "abc", ""])
def test_parse_hhmm_rejects_garbage(bad):
    with pytest.raises(ValueError):
        parse_hhmm(bad)


# --------------------------------------------------------------------------
# next_run_at
# --------------------------------------------------------------------------


def test_next_run_is_today_when_before_run_time():
    # Mon 2026-09-28, 09:30 ET -> same day 16:30 ET = 20:30 UTC (EDT, UTC-4)
    now = et(2026, 9, 28, 9, 30)
    assert next_run_at(now, RUN_TIME) == utc(2026, 9, 28, 20, 30)


def test_next_run_rolls_to_tomorrow_when_slot_already_passed():
    # Mon 2026-09-28, 18:00 ET -> Tue 2026-09-29 16:30 ET
    now = et(2026, 9, 28, 18, 0)
    assert next_run_at(now, RUN_TIME) == utc(2026, 9, 29, 20, 30)


def test_next_run_is_strictly_future_at_exact_slot_boundary():
    # Exactly at the slot time we advance to the next day, deliberately. This
    # is what guarantees the loop can never spin on a zero-length sleep.
    now = et(2026, 9, 28, 16, 30)
    assert next_run_at(now, RUN_TIME) == utc(2026, 9, 29, 20, 30)


def test_next_run_before_slot_advances_one_minute_short():
    now = et(2026, 9, 28, 16, 29)
    assert next_run_at(now, RUN_TIME) == utc(2026, 9, 28, 20, 30)


def test_friday_evening_rolls_to_monday():
    # Fri 2026-10-02 20:00 ET -> Mon 2026-10-05 16:30 ET (skips the weekend)
    now = et(2026, 10, 2, 20, 0)
    assert next_run_at(now, RUN_TIME) == utc(2026, 10, 5, 20, 30)


def test_saturday_rolls_to_monday():
    # Sat 2026-10-03 -> Mon 2026-10-05
    now = et(2026, 10, 3, 12, 0)
    assert next_run_at(now, RUN_TIME) == utc(2026, 10, 5, 20, 30)


def test_sunday_evening_rolls_to_monday():
    now = et(2026, 10, 4, 23, 59)
    assert next_run_at(now, RUN_TIME) == utc(2026, 10, 5, 20, 30)


def test_weekdays_only_false_can_schedule_the_weekend():
    now = et(2026, 10, 3, 12, 0)  # Saturday
    assert next_run_at(now, RUN_TIME, weekdays_only=False) == utc(2026, 10, 3, 20, 30)


def test_next_run_never_lands_on_a_weekend():
    # Walk every day across a full week and assert the target is a weekday.
    for day in range(20, 27):  # Mon 2026-09-20 .. Sun 2026-09-27
        now = et(2026, 9, day, 18, 0)
        target = next_run_at(now, RUN_TIME).astimezone(MARKET_TZ)
        assert target.weekday() < 5
        assert (target - now).total_seconds() > 0


def test_next_run_uses_est_offset_after_dst_transition():
    # DST ends Sun 2026-11-01. From Sat 2026-10-31 the next slot is Mon
    # 2026-11-02, which is EST (UTC-5), not EDT (UTC-4). If the code used a
    # fixed UTC offset the answer would be an hour wrong.
    now = et(2026, 10, 31, 12, 0)
    target = next_run_at(now, RUN_TIME)
    assert target == utc(2026, 11, 2, 21, 30)  # 16:30 EST == 21:30 UTC
    assert target.astimezone(MARKET_TZ).hour == 16
    assert target.astimezone(MARKET_TZ).minute == 30


def test_next_run_uses_edt_offset_before_dst_transition():
    # Same wall-clock slot time, still in EDT (UTC-4) in late September. This
    # is the counterpart to the test above: same HH:MM in ET, different UTC
    # instant, purely because of the date.
    now = et(2026, 9, 30, 18, 0)
    assert next_run_at(now, RUN_TIME) == utc(2026, 10, 1, 20, 30)  # 16:30 EDT


def test_next_run_requires_tz_aware_now():
    with pytest.raises(ValueError):
        next_run_at(datetime(2026, 9, 28, 9, 30), RUN_TIME)


def test_next_run_accepts_utc_input():
    # Input is converted to ET before reasoning about the slot, so a caller
    # holding UTC (the usual case, since that's what asyncio.sleep works in)
    # gets the right answer.
    assert next_run_at(utc(2026, 9, 28, 13, 30), RUN_TIME) == utc(2026, 9, 28, 20, 30)


def test_next_run_round_trips_for_early_close_days():
    # A half-day (early close) is just a normal weekday to this scheduler;
    # the slot is fixed wall-clock time, so the target is unchanged.
    now = et(2026, 11, 27, 18, 0)  # Fri after Thanksgiving, 13:00 close
    assert next_run_at(now, RUN_TIME) == utc(2026, 11, 30, 21, 30)  # Mon 16:30 EST


# --------------------------------------------------------------------------
# build_jobs
# --------------------------------------------------------------------------


def test_build_jobs_default_is_backfill_then_greeks():
    jobs = build_jobs(python_executable="python")
    assert [label for label, _ in jobs] == ["backfill", "greeks-backfill"]
    assert jobs[0][1] == [
        "python", "-m", "service.ingestion.backfill", "--report",
    ]
    assert jobs[1][1] == ["python", "-m", "service.greeks.backfill_job"]


def test_build_jobs_uses_live_interpreter_by_default():
    import sys

    assert build_jobs()[0][1][0] == sys.executable


def test_build_jobs_can_omit_report_flag():
    jobs = build_jobs(python_executable="python", report=False)
    assert "--report" not in jobs[0][1]


def test_build_jobs_can_disable_greeks():
    jobs = build_jobs(python_executable="python", include_greeks=False)
    assert [label for label, _ in jobs] == ["backfill"]


def test_build_jobs_can_add_gap_reconcile_last():
    jobs = build_jobs(python_executable="python", include_gap_reconcile=True)
    assert [label for label, _ in jobs] == [
        "backfill", "greeks-backfill", "gap-reconcile",
    ]
    reconcile = jobs[2][1]
    assert "--reconcile-underlying-gaps" in reconcile
    assert "--report" in reconcile
    # gap-reconcile must run *after* the plain backfill, not instead of it.
    assert [label for label, _ in jobs].index("backfill") < 2


def test_build_jobs_candles_run_before_greeks():
    # Ordering matters: the Greeks pass has nothing useful to do until the
    # candle backfill has written the day's option bars.
    jobs = build_jobs(python_executable="python")
    labels = [label for label, _ in jobs]
    assert labels.index("backfill") < labels.index("greeks-backfill")


# --------------------------------------------------------------------------
# run_forever loop (injected clock/sleeper — no real waiting)
# --------------------------------------------------------------------------


def test_run_forever_exits_quietly_when_disabled(monkeypatch):
    # scheduler.py does `from ... import get_settings`, so the name is bound in
    # its own module namespace — patch it there, not on the source module.
    monkeypatch.setattr(
        "service.ingestion.scheduler.get_settings",
        lambda: _settings(scheduler_enabled=False),
    )

    async def go():
        # If it ever got past the disabled check it would try to sleep; make
        # that a hard failure so the test fails loudly rather than hanging.
        await run_forever(sleeper=_fail_sleeper)

    asyncio.run(go())


def test_run_forever_runs_on_startup_then_sleeps_until_next_slot(monkeypatch):
    monkeypatch.setattr(
        "service.ingestion.scheduler.get_settings",
        lambda: _settings(scheduler_run_on_startup=True),
    )

    calls: list[tuple] = []
    sleeps: list[float] = []

    # Startup run happens, then one scheduled sleep, then we stop the loop by
    # raising a sentinel from the sleeper.
    async def sleeper(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            raise _Stop()

    fake_now = et(2026, 9, 28, 18, 0)  # after the slot -> next is Tue
    runs = {"n": 0}

    async def fake_run_jobs(jobs):
        runs["n"] += 1
        return {label: 0 for label, _ in jobs}

    monkeypatch.setattr("service.ingestion.scheduler.run_jobs", fake_run_jobs)

    with pytest.raises(_Stop):
        asyncio.run(
            run_forever(clock=lambda: fake_now, sleeper=sleeper),
        )

    # One catch-up run at startup, plus the scheduled run after the sleep.
    assert runs["n"] == 2
    # First sleep targets Tue 2026-09-29 16:30 ET from 18:00 ET Mon.
    assert sleeps[0] == pytest.approx(22.5 * 3600, abs=1)


def test_run_forever_skips_startup_run_when_disabled_for_it(monkeypatch):
    monkeypatch.setattr(
        "service.ingestion.scheduler.get_settings",
        lambda: _settings(scheduler_run_on_startup=False),
    )

    runs = {"n": 0}

    async def fake_run_jobs(jobs):
        runs["n"] += 1
        return {}

    async def sleeper(seconds):
        raise _Stop()

    monkeypatch.setattr("service.ingestion.scheduler.run_jobs", fake_run_jobs)

    with pytest.raises(_Stop):
        asyncio.run(run_forever(clock=lambda: et(2026, 9, 28, 18, 0), sleeper=sleeper))

    # No catch-up; the only run is the scheduled one after the first sleep.
    assert runs["n"] == 0


def test_run_forever_never_sleeps_zero_or_negative(monkeypatch):
    """The strict-future boundary in next_run_at must give a positive delay."""
    monkeypatch.setattr(
        "service.ingestion.scheduler.get_settings",
        lambda: _settings(scheduler_run_on_startup=False),
    )

    async def fake_run_jobs(jobs):
        return {}

    delays = []

    async def sleeper(seconds):
        delays.append(seconds)
        raise _Stop()

    monkeypatch.setattr("service.ingestion.scheduler.run_jobs", fake_run_jobs)

    for now in [
        et(2026, 9, 28, 16, 30),  # exact boundary
        et(2026, 9, 28, 16, 30, 1),  # just past
        et(2026, 10, 2, 23, 59),  # Friday, just past midnight-1min
    ]:
        delays.clear()
        with pytest.raises(_Stop):
            asyncio.run(run_forever(clock=lambda n=now: n, sleeper=sleeper))
        assert delays[0] > 0


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


class _Stop(Exception):
    """Sentinel to break out of the otherwise-infinite scheduler loop."""


def _fail_sleeper(seconds):
    raise AssertionError("scheduler should not have slept when disabled")


def _settings(**overrides):
    """Minimal stand-in for AppConfig with the fields the scheduler reads."""
    defaults = dict(
        scheduler_enabled=True,
        scheduler_run_time="16:30",
        scheduler_run_on_startup=True,
        scheduler_run_greeks=True,
        scheduler_run_gap_reconcile=False,
    )
    defaults.update(overrides)
    return type("FakeSettings", (), defaults)()
