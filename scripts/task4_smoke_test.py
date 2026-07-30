"""
Task 4 smoke test — runs the real IngestionPipeline end-to-end for a short
window: auth, contract resolution/subscription, live bar aggregation, and
actual writes to your configured database.

Like the other smoke tests, this checks what unit tests (which use fake
sources/in-memory SQLite) can't: does the whole thing actually work
against TastyTrade's real servers and your real Postgres/TimescaleDB.

Requires .env (TastyTrade creds) and a reachable database (e.g.
`docker compose up timescaledb migrate`). Reads tickers/delta ranges from
config.yaml.

Usage (from the repo root):
    python -m scripts.task4_smoke_test
    python -m scripts.task4_smoke_test --run-seconds 90
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
        "--run-seconds", type=float, default=90.0,
        help="How long to run the pipeline before shutting down gracefully "
             "(default 90s — long enough to see at least one bar flush at "
             "the default 5s flush interval, once a minute boundary passes).",
    )
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.db.session import check_connection, get_session_factory
    from service.ingestion.pipeline import IngestionPipeline
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

        pipeline = IngestionPipeline(source, get_session_factory(), settings)

        log.info(
            "Running pipeline for %.0fs (tracking %s)...",
            args.run_seconds, [t.ticker for t in settings.tickers],
        )
        run_task = asyncio.ensure_future(pipeline.run())
        await asyncio.sleep(args.run_seconds)

        log.info("Time's up — shutting down gracefully (flushing any in-progress bars)...")
        run_task.cancel()
        try:
            await run_task
        except (asyncio.CancelledError, Exception):
            pass
        await pipeline.close()

        log.info(
            "SMOKE TEST COMPLETE. Tracked %d contract(s) by the end. "
            "Check your DB (option_bars_1m / underlying_bars_1m) for real rows — "
            "e.g.: docker compose exec timescaledb psql -U <user> -d options_data "
            "-c \"SELECT * FROM option_bars_1m ORDER BY time DESC LIMIT 10;\"",
            len(pipeline._contract_manager._tracked),
        )
        if not pipeline._contract_manager._tracked:
            log.warning(
                "No contracts were tracked at all — check delta ranges / "
                "max_days_to_expiration in config.yaml, and that this ran "
                "during market hours (Greeks snapshots need live data)."
            )

    finally:
        log.info("Closing connection...")
        await source.close()


if __name__ == "__main__":
    asyncio.run(main())
