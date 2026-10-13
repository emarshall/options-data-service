"""
After-hours maintenance scheduler: runs the daily `backfill` (and Greeks)
on a fixed weekday schedule, so nobody has to remember to do it by hand.

**Why this exists.** TastyTrade's DXFeed publishes live `Quote` events for
`SPX` but not for `NDX` or `VIX` under any symbol spelling — verified directly
against the live feed on 2026-09-30 (`NDX`, `NDX:IND`, `NDX:INDEX`, `VIX`,
`VIX:IND`, `VIX:INDEX` all delivered zero events over 30s while `SPX`
delivered 28), and the ingestion container's own logs show zero NDX/VIX quote
events on every day it has ever run. See PLAN.md Section 7, "NDX and VIX have
no live Quote feed." For those two tickers the candle-API backfill is the
*only* data source, which makes it a hard daily dependency rather than an
occasional convenience. Previously that was a manual step; this automates it.

**Design notes / deliberate choices:**

- **Subprocesses, not in-process calls.** Each job is spawned as
  `python -m <module> ...`, reusing the exact CLI entry points that are
  already used by hand (`docker compose run --rm backfill`) and covered by
  existing tests. A multi-hour backfill that OOMs, deadlocks, or calls
  `sys.exit` takes down only the child, not the scheduler loop, and the
  scheduler survives to try again tomorrow. Output is inherited so everything
  lands in `docker compose logs scheduler` / the container's stdout like any
  other service.

- **Schedule computed in `America/New_York`, not UTC.** A UTC schedule would
  drift an hour twice a year against a 16:00 ET close. Constructing the target
  in ET via `zoneinfo` handles DST automatically (16:30 ET is never ambiguous
  or nonexistent, since US DST transitions happen at 02:00).

- **Weekdays only, no holiday calendar.** The project has deliberately no
  NYSE holiday table (see `service/ingestion/gap_detection.py` for the same
  reasoning). Running on a market holiday is a harmless no-op: the candle API
  simply returns whatever it has for that date, and everything is idempotent.
  Not worth a hand-maintained date list that silently rots.

- **`next_run_at` is strictly in the future** (a target exactly equal to `now`
  resolves to the *next* slot, not "right now"). This deliberately trades a
  vanishingly-unlikely skipped day for the guarantee that the loop can never
  spin on a zero-length sleep.

- **Catch-up on startup by default.** See `SCHEDULER_RUN_ON_STARTUP` in
  `service/config/settings.py`. The failure this protects against is the
  common one: host rebooted, or container down across the 16:30 slot, and a
  whole trading day silently never gets backfilled. Nearly free in practice
  because `backfill` is incremental by default.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from service.config.settings import get_settings
from service.logging_config import configure_logging

log = logging.getLogger("scheduler")

MARKET_TZ = ZoneInfo("America/New_York")


def parse_hhmm(value: str) -> time:
    """Parse an `HH:MM` 24h string into a `time`. Raises ValueError otherwise."""
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def next_run_at(
    now: datetime, run_time: time, weekdays_only: bool = True
) -> datetime:
    """The next scheduled run strictly after `now`, as a UTC datetime.

    `now` must be timezone-aware. Weekends are skipped when `weekdays_only`
    (so a Friday-evening call lands on Monday). See the module docstring for
    why the "strictly after" boundary is the safe direction.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    now_et = now.astimezone(MARKET_TZ)
    candidate = datetime.combine(now_et.date(), run_time, tzinfo=MARKET_TZ)
    if candidate <= now_et:
        candidate = datetime.combine(
            now_et.date() + timedelta(days=1), run_time, tzinfo=MARKET_TZ
        )
    while weekdays_only and candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def build_jobs(
    *,
    python_executable: str | None = None,
    report: bool = True,
    include_greeks: bool = True,
    include_gap_reconcile: bool = False,
) -> list[tuple[str, list[str]]]:
    """The ordered (label, argv) job sequence the scheduler executes.

    Pure so the exact commands are unit-testable without running anything.
    `python_executable` defaults to the running interpreter, which is what
    makes this work identically inside the container and under pytest.
    """
    py = python_executable or sys.executable
    jobs: list[tuple[str, list[str]]] = []

    backfill_argv = [py, "-m", "service.ingestion.backfill"]
    if report:
        backfill_argv.append("--report")
    jobs.append(("backfill", backfill_argv))

    if include_greeks:
        jobs.append(
            ("greeks-backfill", [py, "-m", "service.greeks.backfill_job"])
        )
    if include_gap_reconcile:
        jobs.append(
            (
                "gap-reconcile",
                [
                    py, "-m", "service.ingestion.backfill",
                    "--reconcile-underlying-gaps", "--report",
                ],
            )
        )
    return jobs


async def run_jobs(jobs: list[tuple[str, list[str]]]) -> dict[str, int]:
    """Run each job in order, returning `{label: exit_code}`.

    A failing job does not abort the rest of the sequence: if the candle
    backfill fails but the Greeks pass succeeds, the Greeks pass may still
    cover rows written by an earlier successful run, and skipping it would
    compound the problem. Failures are logged loudly and reflected in the
    returned dict; the caller decides what to do about them.
    """
    results: dict[str, int] = {}
    for label, argv in jobs:
        log.info("starting %s: %s", label, " ".join(argv))
        started = datetime.now(timezone.utc)
        try:
            proc = await asyncio.create_subprocess_exec(*argv)
            returncode = await proc.wait()
        except Exception:
            # Never let one job's failure kill the scheduler — that would
            # silently stop every future trading day's data collection.
            log.exception("failed to launch %s", label)
            results[label] = -1
            continue
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        if returncode == 0:
            log.info("%s finished OK in %.1fs", label, elapsed)
        else:
            log.error("%s FAILED (exit %d) after %.1fs", label, returncode, elapsed)
        results[label] = returncode
    return results


async def run_forever(
    *,
    clock: Callable[[], datetime] | None = None,
    sleeper: Callable[[float], object] | None = None,
) -> None:
    """Main loop. Sleeps until each scheduled slot, then runs the job sequence.

    `clock` and `sleeper` are injectable so tests can drive the schedule
    deterministically without real waiting (same pattern as the pipeline's
    injectable clock).
    """
    now_fn = clock or (lambda: datetime.now(timezone.utc))
    sleep_fn = sleeper or asyncio.sleep

    settings = get_settings()
    if not settings.scheduler_enabled:
        log.info("disabled via config; exiting.")
        return

    run_time = parse_hhmm(settings.scheduler_run_time)
    jobs = build_jobs(
        report=True,
        include_greeks=settings.scheduler_run_greeks,
        include_gap_reconcile=settings.scheduler_run_gap_reconcile,
    )
    log.info(
        "scheduler: starting. Jobs=%s run_time=%s ET run_on_startup=%s",
        [label for label, _ in jobs],
        run_time.strftime("%H:%M"),
        settings.scheduler_run_on_startup,
    )

    if settings.scheduler_run_on_startup:
        log.info("startup catch-up run")
        results = await run_jobs(jobs)
        _log_summary(results)

    while True:
        now = now_fn()
        target = next_run_at(now, run_time)
        wait_s = (target - now).total_seconds()
        log.info(
            "scheduler: next run %s ET (in %.2f h)",
            target.astimezone(MARKET_TZ).strftime("%Y-%m-%d %H:%M"),
            wait_s / 3600.0,
        )
        await sleep_fn(wait_s)
        results = await run_jobs(jobs)
        _log_summary(results)


def _log_summary(results: dict[str, int]) -> None:
    failed = {k: v for k, v in results.items() if v != 0}
    if failed:
        log.error("run finished with failures: %s", failed)
    else:
        log.info("run finished, all jobs OK (%s)", ", ".join(results))


async def _main() -> None:
    configure_logging("scheduler", get_settings().log_level)
    try:
        await run_forever()
    except asyncio.CancelledError:
        log.info("cancelled, shutting down.")


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
