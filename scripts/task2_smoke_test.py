"""
Task 2 smoke test — exercises TastyTradeSource against a REAL TastyTrade
connection.

Unlike tests/test_tastytrade_source.py (which validates the bookkeeping
logic with fakes, and runs in CI/locally without credentials), this
specifically checks the one thing unit tests can't: does this actually work
against TastyTrade's real servers. Run this after any change to
service/sources/tastytrade.py, and periodically thereafter (e.g. if the
`tastytrade` package gets upgraded).

Requires the same .env used by the main service — TASTYTRADE_CLIENT_SECRET
and TASTYTRADE_REFRESH_TOKEN. See scripts/task0_spike/README.md for how to
obtain these if you don't have them yet.

Usage (from the repo root):
    python -m scripts.task2_smoke_test --ticker SPY
    python -m scripts.task2_smoke_test --ticker SPY --listen-seconds 30
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow running this both as `python -m scripts.task2_smoke_test` (preferred)
# and as `python scripts/task2_smoke_test.py` directly (sys.path fallback).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("smoke_test")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", default="SPY", help="Underlying ticker to test (default SPY)")
    parser.add_argument(
        "--listen-seconds", type=float, default=15.0,
        help="How long to listen for live quote/Greeks events (default 15s). "
             "Outside market hours, expect zero events — that's normal, not a failure.",
    )
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.sources.tastytrade import TastyTradeSource

    settings = get_settings()
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        raise SystemExit(
            "TASTYTRADE_CLIENT_SECRET / TASTYTRADE_REFRESH_TOKEN not set. "
            "Copy .env.example to .env and fill them in first — see "
            "scripts/task0_spike/README.md for how to obtain them."
        )

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )

    try:
        log.info("Authenticating (sandbox=%s)...", settings.tastytrade_use_sandbox)
        await source.authenticate()
        log.info("OK.")

        log.info("Fetching option chain for %s...", args.ticker)
        chain = await source.get_option_chain(args.ticker)
        expirations = sorted(chain.keys())
        if not expirations:
            raise SystemExit(f"No option chain returned for {args.ticker}.")
        log.info("Got %d expiration(s). Nearest: %s", len(expirations), expirations[0])

        nearest = expirations[0]
        contract = chain[nearest][len(chain[nearest]) // 2]
        symbol = contract.streamer_symbol
        log.info("Using test contract: %s", symbol)

        quote_events: list = []
        greeks_events: list = []

        async def on_quote(event):
            quote_events.append(event)
            log.info(
                "QUOTE %s bid=%s ask=%s",
                event.event_symbol,
                getattr(event, "bid_price", None),
                getattr(event, "ask_price", None),
            )

        async def on_greeks(event):
            greeks_events.append(event)
            log.info(
                "GREEKS %s delta=%s iv=%s",
                event.event_symbol,
                getattr(event, "delta", None),
                getattr(event, "volatility", None),
            )

        log.info(
            "Subscribing to live quotes (%s, %s) + Greeks (%s), listening %.0fs...",
            symbol, args.ticker, symbol, args.listen_seconds,
        )
        await source.subscribe_quotes([symbol, args.ticker], on_quote)
        await source.subscribe_greeks([symbol], on_greeks)
        await asyncio.sleep(args.listen_seconds)

        log.info(
            "Received %d quote event(s), %d greeks event(s) so far.",
            len(quote_events), len(greeks_events),
        )
        if not quote_events and not greeks_events:
            log.warning(
                "No live events received. Normal outside market hours; "
                "worth double-checking if this happened during market hours."
            )

        log.info("Requesting 1m candle backfill for %s (~60 days back)...", symbol)
        candles = await source.request_candles(
            symbol, "1m", datetime.now(timezone.utc) - timedelta(days=60)
        )
        log.info("Got %d candle bar(s).", len(candles))
        if candles:
            first, last = candles[0], candles[-1]
            log.info(
                "First bar time=%s, last bar time=%s (raw dxfeed timestamps, ms since epoch)",
                getattr(first, "time", None), getattr(last, "time", None),
            )

        log.info("Unsubscribing...")
        await source.unsubscribe([symbol, args.ticker])

        log.info(
            "SMOKE TEST PASSED. quotes=%d greeks=%d candles=%d",
            len(quote_events), len(greeks_events), len(candles),
        )

    finally:
        log.info("Closing connection...")
        await source.close()


if __name__ == "__main__":
    asyncio.run(main())
