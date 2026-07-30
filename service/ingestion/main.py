"""
Ingestion service entrypoint.

Real wiring as of Task 4: authenticates against TastyTrade, then runs
IngestionPipeline (contract resolution + subscription + bar aggregation +
DB writes) indefinitely, with graceful shutdown on SIGTERM/SIGINT (so
`docker compose down`/restarts flush in-progress bars rather than losing
them).
"""

import asyncio
import logging
import signal

from service.config.settings import get_settings
from service.db.session import check_connection, get_session_factory
from service.ingestion.pipeline import IngestionPipeline
from service.sources.tastytrade import TastyTradeSource

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ingestion")


async def main() -> None:
    settings = get_settings()
    log.info(
        "Ingestion service starting. Tracking %d ticker(s): %s",
        len(settings.tickers),
        [t.ticker for t in settings.tickers],
    )

    if not await check_connection():
        log.error("Could not connect to the database — check DATABASE_URL / that the DB is up.")
        raise SystemExit(1)
    log.info("Database connection OK.")

    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        log.error(
            "No TastyTrade credentials configured (TASTYTRADE_CLIENT_SECRET / "
            "TASTYTRADE_REFRESH_TOKEN) — cannot start streaming without them."
        )
        raise SystemExit(1)

    if not settings.tickers:
        log.error("No tickers configured in config.yaml — nothing to track. Exiting.")
        raise SystemExit(1)

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )
    await source.authenticate()
    log.info("Authenticated with TastyTrade (sandbox=%s).", settings.tastytrade_use_sandbox)

    pipeline = IngestionPipeline(source, get_session_factory(), settings)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            # add_signal_handler isn't available on some platforms (e.g.
            # Windows) — not a concern for the docker-compose deployment
            # target, but shouldn't crash local dev on those platforms.
            pass

    run_task = asyncio.ensure_future(pipeline.run())
    stop_task = asyncio.ensure_future(stop_event.wait())

    done, pending = await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)

    if stop_task in done:
        log.info("Shutdown signal received — flushing and closing gracefully...")
    else:
        log.error("Ingestion pipeline exited unexpectedly.")
        for task in done:
            exc = task.exception()
            if exc:
                log.error("Pipeline error: %s", exc)

    run_task.cancel()
    try:
        await run_task
    except (asyncio.CancelledError, Exception):
        pass

    await pipeline.close()
    await source.close()
    log.info("Ingestion service stopped.")


if __name__ == "__main__":
    asyncio.run(main())
