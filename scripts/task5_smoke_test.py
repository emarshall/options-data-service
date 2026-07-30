"""
Task 5 smoke test — runs the real BackfillJob against TastyTrade's actual
servers and your configured database.

Like the other smoke tests, this checks what unit tests (fake source, in-
memory SQLite) can't: does the real ~6-week candle history actually come
back and get written correctly against a real DB.

Requires .env (TastyTrade creds) and a reachable database (e.g.
`docker compose up timescaledb migrate`). Reads tickers/delta ranges from
config.yaml.

Usage (from the repo root):
    python -m scripts.task5_smoke_test
    python -m scripts.task5_smoke_test --lookback-days 10   # faster, smaller test run
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
        "--lookback-days", type=int, default=60,
        help="How far back to request candles (default 60 — the full "
             "window; use a smaller value like 5-10 for a quicker test run).",
    )
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.db.session import check_connection, get_session_factory
    from service.ingestion.backfill import BackfillJob
    from service.sources.tastytrade import TastyTradeSource

    settings = get_settings()
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        raise SystemExit(
            "TASTYTRADE_CLIENT_SECRET / TASTYTRADE_REFRESH_TOKEN not set — see "
            "scripts/task0_spike/README.md for how to obtain them."
        )
    if not settings.tickers:
        raise SystemExit("No tickers configured. Copy config.example.yaml to config.yaml first.")

    log.info("Checking DB connection...")
    if not await check_connection():
        raise SystemExit(
            "Could not reach the database. Is `docker compose up timescaledb migrate` running?"
        )
    log.info("DB OK.")

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )

    try:
        log.info("Authenticating...")
        await source.authenticate()

        job = BackfillJob(
            source, get_session_factory(), settings, lookback_days=args.lookback_days
        )
        log.info("Running backfill (lookback=%d days)...", args.lookback_days)
        summary = await job.run()

        log.info("SMOKE TEST COMPLETE. Summary: %s", summary)
        if summary["option_bars_written"] == 0 and summary["underlying_bars_written"] == 0:
            log.warning(
                "No bars were written at all. If this is the very first run, worth checking: "
                "did any contracts get tracked (see Task 3's ContractManager — requires live "
                "Greeks, so run during market hours), and did request_candles() actually "
                "return anything (check TASTYTRADE_USE_SANDBOX — sandbox candle history may "
                "be limited/absent, per Task 0 notes)."
            )
        if summary["contracts_failed"] or summary["underlyings_failed"]:
            log.warning(
                "%d contract(s) and %d underlying(s) failed to backfill — check the logged "
                "tracebacks above for details.",
                summary["contracts_failed"], summary["underlyings_failed"],
            )

    finally:
        log.info("Closing connection...")
        await source.close()


if __name__ == "__main__":
    asyncio.run(main())
