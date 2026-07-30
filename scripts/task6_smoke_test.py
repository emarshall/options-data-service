"""
Task 6 smoke test — runs the real GreeksBackfillJob against your actual
database.

Unlike the other smoke tests, this doesn't need TastyTrade credentials at
all — it's pure DB reconciliation, reading option_bars_1m/underlying_bars_1m
and writing computed Greeks back. What it checks that unit tests (in-memory
SQLite) can't: does this behave sensibly against your real, populated
Postgres/TimescaleDB — in particular, whether there's actually anything
for it to do yet (requires Task 5's backfill, or some live-ingested gaps,
to have already produced NULL-greeks_source rows).

Usage (from the repo root):
    python -m scripts.task6_smoke_test
    python -m scripts.task6_smoke_test --max-rows 50   # smaller test run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("smoke_test")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Cap on rows processed (default: unbounded — process everything available).",
    )
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.db.session import check_connection, get_session_factory
    from service.greeks.backfill_job import GreeksBackfillJob

    settings = get_settings()

    log.info("Checking DB connection...")
    if not await check_connection():
        raise SystemExit(
            "Could not reach the database. Is `docker compose up timescaledb migrate` running?"
        )
    log.info("DB OK. risk_free_rate=%.4f", settings.risk_free_rate)

    job = GreeksBackfillJob(get_session_factory(), settings.risk_free_rate)
    summary = await job.run(max_rows=args.max_rows)

    log.info("SMOKE TEST COMPLETE. Summary: %s", summary)
    if summary["rows_processed"] == 0:
        log.warning(
            "Nothing to do — no rows with greeks_source IS NULL were found. This is "
            "expected if you haven't run `docker compose run --rm backfill` yet (Task 5), "
            "or if live ingestion (Task 4) hasn't produced any Greeks gaps."
        )
    elif summary["computed"] == 0:
        log.warning(
            "Rows were found but none could be computed — check the skip-reason counts "
            "above (most commonly: no matching underlying bar at the same minute, which "
            "means the underlying wasn't backfilled/ingested for that ticker/time)."
        )


if __name__ == "__main__":
    asyncio.run(main())
