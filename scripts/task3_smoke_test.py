"""
Task 3 smoke test — runs a real ContractManager.refresh() cycle against
TastyTrade's actual servers and your configured database.

Like scripts/task2_smoke_test.py, this checks what the unit tests
(tests/test_contract_manager.py, which use a fake source) can't: does
delta-based filtering actually produce sensible results against a real
option chain and real Greeks, and does persistence work against your real
DB (not just SQLite-in-memory).

Requires .env (TastyTrade creds) and either a reachable DATABASE_URL (e.g.
`docker compose up timescaledb` running) or defaults to whatever's in your
environment. Reads tickers/delta ranges from config.yaml, same as the real
service will.

Usage (from the repo root):
    python -m scripts.task3_smoke_test
    python -m scripts.task3_smoke_test --refresh-cycles 2
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
        "--refresh-cycles", type=int, default=2,
        help="How many times to call refresh() (default 2, to also exercise "
             "the 'already tracked, don't re-snapshot' path).",
    )
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.db.session import check_connection, get_session_factory
    from service.ingestion.contract_manager import ContractManager
    from service.sources.tastytrade import TastyTradeSource

    settings = get_settings()
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        raise SystemExit(
            "TASTYTRADE_CLIENT_SECRET / TASTYTRADE_REFRESH_TOKEN not set — see "
            "scripts/task0_spike/README.md for how to obtain them."
        )
    if not settings.tickers:
        raise SystemExit(
            "No tickers configured. Copy config.example.yaml to config.yaml first."
        )

    log.info("Checking DB connection...")
    if not await check_connection():
        raise SystemExit(
            "Could not reach the database. Is `docker compose up timescaledb` (and "
            "`migrate`) running, and DATABASE_URL set correctly?"
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

        mgr = ContractManager(source, get_session_factory(), settings.tickers)

        for cycle in range(1, args.refresh_cycles + 1):
            log.info("--- Refresh cycle %d/%d ---", cycle, args.refresh_cycles)
            diff = await mgr.refresh()
            log.info(
                "added=%d removed=%d current=%d",
                len(diff.added), len(diff.removed), len(diff.current),
            )
            for cid, rc in list(diff.added.items())[:5]:
                log.info(
                    "  + %s: %s %s strike=%s exp=%s settlement=%s",
                    cid, rc.underlying_ticker, rc.right.value, rc.strike,
                    rc.expiration_date, rc.settlement_type,
                )
            if len(diff.added) > 5:
                log.info("  ... and %d more", len(diff.added) - 5)

            if cycle == 1 and not diff.current:
                log.warning(
                    "No contracts resolved on the first cycle. Check your delta "
                    "ranges / max_days_to_expiration in config.yaml, and that "
                    "the market is open (Greeks snapshots need live data)."
                )

        log.info("SMOKE TEST PASSED. Final tracked count: %d", len(mgr._tracked))

    finally:
        log.info("Closing connection...")
        await source.close()


if __name__ == "__main__":
    asyncio.run(main())
