"""
Near-real-time option-candle poller for tickers whose option data is NOT
covered by Task 4's live quote ingestion.

**Why this exists.** SPX option quotes/bar-aggreggation arrive continuously
from DXFeed's live `Quote`/`Candle` feeds (the `ingestion` service). For
NDX that live stream does not exist for the underlying index (no live
Quote feed), and the intraday 1m *option* candles for NDX — the same
`request_candles`-based history the daily backfill uses — were only written
once a day by the after-hours `backfill` (the `scheduler` service, 16:30
ET). That is fine for building backtest history, but useless for anything
that needs today's 0DTE option bars as they print — e.g. the 0DTE
entry-signal monitor in the sibling `zerodte-backtester` repo, whose
NDX-configs need live MID-minute option bars (candles) all session long.

This service is that slice, and it mirrors `live_underlying.py` exactly in
spirit: it pulls just the last few minutes of candles for the day's
eligible contracts via the same `request_candles()` and writes them through
the same idempotent `BackfillJob._backfill_option_contract` write path
(skip-if-exists, never overwrite, IV/volume/vwap captured, bid/ask/greeks
NULL), so its rows are byte-identical in shape to the daily backfill's —
just produced continuously through the session instead of once a day. The
daily backfill still owns full multi-day history.

**How the candidate contract set is resolved.** For each polled ticker we
reuse `ContractManager.refresh()` plus the backfill's persisted-contract
merge (`BackfillJob._get_backfill_candidates`), restricted to contracts
expiring today (US Eastern). Two caveats handled explicitly:

1. ContractManager's delta-band filter needs a live Greeks snapshot for
   newly-seen candidates, so on a day when that snapshot isn't available
   for this ticker it resolves nothing — and a brand-new day starts with
   NO persisted contracts for today. To keep the poller self-starting
   (and to make the whole point — intraday NDX alerting — robust rather
   than dependent on whichever remote component resolves identities), a
   chain fallback persists today's PM-settled contracts directly from
   `source.get_option_chain(ticker)` inside a conservative strike window
   around the underlying's latest price, then backfills those too. The
   window is deliberately generous (`--strike-window-pct`, default 5%):
   even at 0DTE the configured delta band (|delta| 0.15–0.85) sits well
   inside it, and over-capturing a few extra strikes is harmless — it is
   the same net the daily backfill casts, just resolved without needing a
   live Greeks snapshot.
2. Contracts are only *backfilled* for today's expiration. The daily
   backfill's own multi-DTE coverage (max_days_to_expiration) is not the
   live poller's job.

**Cost.** Same reasoning as live_underlying: a handful of contracts, a
couple of minutes' worth of candles each, `request_candles`'s replay runs
to the live tail and stops on the repeated live candle, dedup skips
already-written minutes. A poll costs a few seconds, not minutes, even
concurrent (bounded by `--concurrency`).

Run it as the `options-live` compose service.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select

from service.config.settings import TickerConfig, get_settings
from service.db.models import OptionRight, SettlementType, UnderlyingBar1m
from service.db.session import check_connection, get_session_factory
from service.ingestion.backfill import BackfillJob
from service.ingestion.contract_manager import ContractManager, ResolvedContract
from service.ingestion.live_underlying import in_window, parse_hhmm
from service.logging_config import configure_logging
from service.sources._async_utils import get_attr_any
from service.sources.tastytrade import TastyTradeSource

# Explicit name, not `__name__` — this module is launched as
# `python -m service.ingestion.live_options`, which runs it under `__main__`
# — same logger-naming rationale as live_underlying.py / backfill.py.
log = logging.getLogger("service.ingestion.live_options")

_NY_TZ = ZoneInfo("America/New_York")

DEFAULT_TICKERS = ("NDX",)

# Slightly longer than live_underlying's 4-minute default: option candles
# print a little late relative to the underlying (a quote-less underlying's
# index settles a second or two behind the options' trade prints), and the
# replay cost for `lookback` minutes is negligible.
DEFAULT_LOOKBACK_MIN = 6.0


def eastern_today(now: datetime) -> date:
    """Today's date in America/New_York — the day 0DTE contracts expire on.
    `now` (UTC, tz-aware) is required; DST handled by zoneinfo."""
    return now.astimezone(_NY_TZ).date()


async def _last_underlying_close(session_factory, ticker: str) -> float | None:
    """Latest underlying close on record for `ticker`, used to center the
    chain-fallback strike window. Returns None (meaning "no window") if no
    underlying bar exists yet — e.g. pre-open on a fresh day."""
    async with session_factory() as session:
        stmt = (
            select(UnderlyingBar1m)
            .where(UnderlyingBar1m.ticker == ticker)
            .order_by(UnderlyingBar1m.time.desc())
            .limit(1)
        )
        row = (await session.execute(stmt)).scalars().first()
    if row is None or row.close is None:
        return None
    return float(row.close)


def filter_chain_for_today(
    chain,
    ticker_cfg: TickerConfig,
    day: date,
    spot: float | None,
    strike_window_pct: float,
    max_days_to_expiration: int,
) -> list[ResolvedContract]:
    """Reduce a `get_option_chain` result (mapping expiration -> options)
    to today's expiring, PM-settled contacts inside a conservative strike
    window around `spot` — the no-Greeks fallback path (see module
    docstring). Returns ResolvedContract list, ready to persist/backfill.

    Pure for testability; `chain` items only need to be attribute-accessed
    via the codebase's tolerant `get_attr_any`.
    """
    out: list[ResolvedContract] = []
    for expiration, options in chain.items():
        if expiration != day:
            continue
        for opt in options:
            dte = get_attr_any(opt, "days_to_expiration")
            if dte is not None and dte > max_days_to_expiration:
                continue
            if ticker_cfg.exclude_am_settled and ContractManager._is_am_settled(opt):
                continue
            strike = get_attr_any(opt, "strike_price")
            if strike is None:
                continue
            strike = float(strike)
            if spot is not None and spot > 0:
                window = spot * strike_window_pct / 100.0
                if not (spot - window <= strike <= spot + window):
                    continue
            symbol = getattr(opt, "streamer_symbol", None)
            if symbol is None:
                symbol = getattr(opt, "contract_symbol", None)
            if symbol is None:
                continue

            right = ContractManager._option_right(opt)
            settlement = ContractManager._settlement_type(opt)
            out.append(
                ResolvedContract(
                    contract_id=str(symbol),
                    underlying_ticker=ticker_cfg.ticker,
                    expiration_date=day,
                    strike=strike,
                    right=right,
                    settlement_type=settlement,
                )
            )
    return out


@dataclass
class _PollCounts:
    contracts: int = 0
    bars_written: int = 0
    bars_received: int = 0
    failed: int = 0
    fallback_persisted: int = 0


async def poll_ticker(
    job: BackfillJob,
    session_factory,
    ticker: str,
    start_time: datetime,
    concurrency: int,
    spot_lookup_now: datetime,
    strike_window_pct: float,
) -> _PollCounts:
    """One poll pass for a single ticker: resolve today's 0DTE contracts
    (with the chain fallback), then backfill their last `lookback` minutes.

    Mirrors `BackfillJob.run` but restricted to today's expiration and to
    the pass's ticker — the same `_get_backfill_candidates` + per-contract
    `_backfill_option_contract` internals, so the write path is identical.
    """
    counts = _PollCounts()
    settings = get_settings()
    cfg = next((t for t in settings.tickers if t.ticker == ticker), None)
    today = eastern_today(spot_lookup_now)

    diff = await job._contract_manager.refresh()
    candidates = await job._get_backfill_candidates(diff.current, spot_lookup_now)
    candidates = {
        cid: rc
        for cid, rc in candidates.items()
        if rc.underlying_ticker == ticker and rc.expiration_date == today
    }

    # Chain fallback for when ContractManager resolved nothing for this
    # ticker on a fresh day (see module docstring). Persisted so the FK for
    # the option bars below is satisfied and so future polls find them via
    # _get_backfill_candidates.
    prior_set = set(candidates)
    if cfg is not None:
        spot = await _last_underlying_close(session_factory, ticker)
        fallback = filter_chain_for_today(
            await job._source.get_option_chain(ticker),
            cfg,
            today,
            spot,
            strike_window_pct,
            cfg.max_days_to_expiration,
        )
        fallback_ids = {rc.contract_id for rc in fallback}
        new_fallback = [rc for rc in fallback if rc.contract_id not in prior_set]
        if new_fallback:
            await job._contract_manager._persist(new_fallback)
            counts.fallback_persisted = len(new_fallback)
        for rc in fallback:
            if rc.contract_id not in candidates:
                candidates[rc.contract_id] = rc
        if not fallback_ids:
            log.info(
                "%s: no today-expiring contracts resolved or found on the chain "
                "(exp=%s spot=%s) — nothing to poll this tick.",
                ticker, today, spot,
            )

    if not candidates:
        return counts
    counts.contracts = len(candidates)

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _one(contract_id: str, resolved: ResolvedContract) -> None:
        async with semaphore:
            diag: dict = {}
            try:
                # Does the request_candles + idempotent write in one step,
                # exactly the daily backfill's path.
                written = await job._backfill_option_contract(
                    contract_id, resolved, start_time, diagnostics=diag
                )
                counts.bars_written += written
                counts.bars_received += diag.get("event_count", 0)
            except Exception:
                counts.failed += 1
                log.exception("Poll failed for %s — continuing with the rest.", contract_id)

    await asyncio.gather(*(_one(cid, rc) for cid, rc in candidates.items()))
    return counts


async def run(args: argparse.Namespace) -> None:
    settings = get_settings()

    if not await check_connection():
        log.error("Could not connect to the database — check DATABASE_URL / that the DB is up.")
        raise SystemExit(1)
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        log.error(
            "No TastyTrade credentials configured (TASTYTRADE_CLIENT_SECRET / "
            "TASTYTRADE_REFRESH_TOKEN) — cannot poll without them."
        )
        raise SystemExit(1)

    tickers = sorted({t.strip().upper() for t in args.tickers.split(",") if t.strip()})
    known = {tc.ticker for tc in settings.tickers}
    unknown = [t for t in tickers if t not in known]
    if unknown:
        log.warning(
            "Polling ticker(s) not present in config.yaml: %s. Polling anyway — "
            "the candle feed is keyed on the symbol, not the config.",
            unknown,
        )

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )
    await source.authenticate()
    session_factory = get_session_factory()

    job = BackfillJob(
        source, session_factory, settings,
        lookback_days=1,  # irrelevant here — we only use candidates/backfill helpers
        contract_concurrency=args.concurrency,
    )

    log.info(
        "live options poller starting. tickers=%s window=%s..%s UTC weekdays_only=%s "
        "interval=%.0fs lookback=%.0fmin concurrency=%d",
        tickers, args.window_start, args.window_end, args.weekdays_only,
        args.interval, args.lookback_min, args.concurrency,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    wstart = parse_hhmm(args.window_start)
    wend = parse_hhmm(args.window_end)

    try:
        while not stop_event.is_set():
            now = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(timezone.utc)
            if args.once or in_window(now, wstart, wend, args.weekdays_only):
                start = now - timedelta(minutes=args.lookback_min)
                for ticker in tickers:
                    try:
                        counts = await poll_ticker(
                            job, session_factory, ticker, start,
                            args.concurrency, now, args.strike_window_pct,
                        )
                        if counts.contracts:
                            log.info(
                                "%s: %d contract(s), %d bar(s) written of %d received "
                                "(%d failed, %d fallback-persisted)",
                                ticker, counts.contracts, counts.bars_written,
                                counts.bars_received, counts.failed, counts.fallback_persisted,
                            )
                        else:
                            log.info(
                                "%s: no contracts to poll this tick "
                                "(received=%d, failed=%d)",
                                ticker, counts.bars_received, counts.failed,
                            )
                    except Exception:
                        # One ticker failing must not stop the others, nor the loop.
                        log.exception("%s: poll failed — continuing.", ticker)
            else:
                log.debug("outside window (%s UTC) — skipping this tick", now.strftime("%H:%M"))

            if args.once:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=args.interval)
            except asyncio.TimeoutError:
                pass
    finally:
        await source.close()
        log.info("live options poller stopped.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Poll recent 1m option candles for tickers with no live "
                    "option-quote ingestion (NDX) and write them to option_bars_1m "
                    "continuously, so 0DTE option bars exist intraday.",
    )
    p.add_argument(
        "--tickers",
        default=os.environ.get("LIVE_OPTIONS_TICKERS", ",".join(DEFAULT_TICKERS)),
        help="Comma-separated tickers to poll (default: %(default)s).",
    )
    p.add_argument("--interval", type=float, default=60.0, help="Seconds between polls (default 60).")
    p.add_argument(
        "--lookback-min", type=float, default=DEFAULT_LOOKBACK_MIN,
        help="Minutes of recent history to request each poll (default %.0f)." % DEFAULT_LOOKBACK_MIN,
    )
    p.add_argument("--window-start", default="13:20", help="UTC HH:MM poll window start (default 13:20).")
    p.add_argument("--window-end", default="20:20", help="UTC HH:MM poll window end (default 20:20).")
    p.add_argument("--all-days", dest="weekdays_only", action="store_false", help="Poll on weekends too.")
    p.add_argument("--concurrency", type=int, default=8, help="Concurrent candle requests per poll (default 8).")
    p.add_argument(
        "--strike-window-pct", type=float, default=5.0,
        help="Fallback chain filter: +/- this percent of the underlying's latest "
             "close around spot (default 5). Only used when ContractManager "
             "resolves nothing for the ticker and the chain fallback kicks in.",
    )
    p.add_argument("--once", action="store_true", help="Poll once (ignoring the window) then exit — for testing.")
    p.add_argument(
        "--as-of", default=None,
        help="ISO-8601 UTC timestamp to treat as 'now' (testing) — pulls that moment's "
             "recent candles instead of the live wall clock. Use with --once.",
    )
    p.set_defaults(weekdays_only=True)
    return p


def main() -> None:
    args = build_parser().parse_args()
    configure_logging("service.ingestion.live_options", get_settings().log_level)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()