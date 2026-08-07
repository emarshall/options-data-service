"""
Historical backfill via Candle events, bounded to TastyTrade's confirmed
~6-week trailing retention window for 1-minute option candles (Task 0
finding — see PLAN.md Section 2).

**Scope extension vs. the original plan draft:** also backfills the
underlying's own price history (not just option contracts) for tickers
with `capture_underlying_bars` enabled. Necessary addition, not scope
creep — Task 6's Black-Scholes calculator needs a spot price to compute
Greeks for backfilled option rows, and without this, that spot price
simply wouldn't exist for any day before the service started running.

**Idempotent / safe to re-run.** Only inserts rows for a (contract_id_or_
ticker, minute) combination that doesn't already exist — never overwrites
an existing row. Two consequences, both deliberate:
- Running this multiple times just fills in whatever gaps remain; safe to
  schedule as a recurring "catch up on downtime" job (Task 9), not just a
  one-time post-deploy step.
- If Task 4's live ingestion already wrote a bar for a given minute (e.g.
  backfill runs while a contract is actively streaming, or overlapping
  with "now"), that live bar — mark-price-based, with real Greeks — is
  left untouched. It's treated as more authoritative than backfilled
  candle data (which is trade/settlement-price based, a different price
  basis) for any minute where both could exist. This avoids silently
  mixing two different OHLC semantics under one column.

**Recovers from downtime, including contracts that fully expired during
the gap.** `ContractManager.refresh()` alone can only re-discover
contracts that are still live-resolvable (listed in the current chain,
with a live Greeks snapshot available) — so it's blind to anything whose
entire lifecycle (delta-eligible → expired) happened while the ingestion
service was down. To close that gap, candidate contracts also include
anything already persisted in the `contracts` table with an expiration
inside the lookback window, regardless of whether it's still
live-resolvable today. Candle history for those contracts is confirmed
(Task 0) to survive past expiration, so this recovers real data that would
otherwise be silently missed for any outage longer than a contract's
remaining lifetime.

**Deliberately does NOT compute Greeks.** Backfilled rows get price,
volume, open_interest, vwap, and IV straight from the candle — confirmed
in Task 0 to include `ImpVolatility` directly — but delta/gamma/theta/
vega/rho stay NULL, and `greeks_source` stays NULL (not 'live' or
'computed'), until Task 6's Black-Scholes calculator fills them in as a
separate pass.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from service.config.settings import AppConfig
from service.db.models import Contract, OptionBar1m, UnderlyingBar1m
from service.ingestion.contract_manager import ContractManager, ResolvedContract
from service.ingestion.gap_detection import find_gaps
from service.sources._async_utils import get_attr_any
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)

# Comfortably beyond Task 0's confirmed ~6-week (~43 day) retention window.
# Over-requesting is harmless — confirmed in Task 0 that the feed just
# returns whatever it actually has, it doesn't error on this.
DEFAULT_LOOKBACK_DAYS = 60


class BackfillJob:
    def __init__(
        self,
        source: MarketDataSource,
        session_factory,
        settings: AppConfig,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        full_rescan: bool = False,
    ):
        self._source = source
        self._session_factory = session_factory
        self._settings = settings
        self._lookback_days = lookback_days
        # See `run()`'s docstring — False (the default) is the fast,
        # every-run-safe mode added to fix a real reported problem
        # (15-30 minute runs re-requesting data already on disk every
        # single time). True restores the original always-full-lookback
        # behavior, for the occasional deliberate deep re-verify.
        self._full_rescan = full_rescan
        self._contract_manager = ContractManager(source, session_factory, settings.tickers)

    async def run(self) -> dict:
        """Resolves the current contract set, then backfills 1m candles for
        every tracked option contract plus each configured underlying.
        Returns a summary dict (also logged) — useful for the smoke test
        and for tests to assert against.

        **Incremental by default (`full_rescan=False`):** for each
        contract/underlying, requests candles starting from whichever is
        later — the full lookback window, or the latest bar timestamp
        already on disk for that specific contract/ticker — instead of
        always re-requesting the entire lookback window from scratch every
        run. A contract/ticker with no existing data at all still gets the
        full lookback (first-run/newly-discovered-contract behavior is
        unchanged). This is what makes routine re-runs (e.g. a scheduled
        "catch up since last run" job) fast: a ticker with days of
        already-backfilled history only requests the small amount of new
        data since its last known bar, rather than re-fetching and
        re-checking-for-duplicates weeks of data that was already correct.
        Existing-row dedup (see `_backfill_option_contract`/
        `_backfill_underlying`) still applies on top of this regardless —
        this is purely a request-size optimization, not a correctness
        mechanism; it's safe to fall back to `full_rescan=True` at any time
        without any risk of duplicating data.

        **This does not, by itself, fix gaps earlier in the data** (e.g. a
        multi-day outage that predates the latest bar) — it only looks at
        the *latest* timestamp per contract/ticker, so an old hole followed
        by more recent good data won't be noticed this way. That's what
        `reconcile_underlying_gaps()` is for, as a separate, occasional
        pass — see its docstring.
        """
        diff = await self._contract_manager.refresh()
        window_start = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)

        candidates = await self._get_backfill_candidates(diff.current, window_start)
        recovered_count = len(candidates) - len(diff.current)

        summary = {
            "contracts_attempted": 0, "contracts_failed": 0, "option_bars_written": 0,
            "contracts_recovered_from_db": max(recovered_count, 0),
            "underlyings_attempted": 0, "underlyings_failed": 0, "underlying_bars_written": 0,
        }

        option_starts = await self._effective_start_times(
            OptionBar1m, OptionBar1m.contract_id, list(candidates.keys()), window_start
        )
        for contract_id, resolved in candidates.items():
            summary["contracts_attempted"] += 1
            try:
                summary["option_bars_written"] += await self._backfill_option_contract(
                    contract_id, resolved, option_starts[contract_id]
                )
            except Exception:
                summary["contracts_failed"] += 1
                log.exception(
                    "Backfill failed for contract %s — continuing with the rest.", contract_id
                )

        underlying_tickers = {t.ticker for t in self._settings.tickers if t.capture_underlying_bars}
        underlying_starts = await self._effective_start_times(
            UnderlyingBar1m, UnderlyingBar1m.ticker, list(underlying_tickers), window_start
        )
        for ticker in underlying_tickers:
            summary["underlyings_attempted"] += 1
            try:
                summary["underlying_bars_written"] += await self._backfill_underlying(
                    ticker, underlying_starts[ticker]
                )
            except Exception:
                summary["underlyings_failed"] += 1
                log.exception("Backfill failed for underlying %s — continuing with the rest.", ticker)

        log.info("Backfill summary: %s", summary)
        return summary

    @staticmethod
    def _as_utc(dt: datetime) -> datetime:
        """SQLite (used in tests, and possible for a local dev DB) doesn't
        preserve tzinfo on DateTime columns, so a value read back can come
        out naive even though every timestamp this codebase writes is UTC
        (see `_candle_time` below) — normalize so comparisons against
        tz-aware datetimes elsewhere don't raise or silently mismatch."""
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    async def _effective_start_times(
        self, model, key_column, keys: list[str], window_start: datetime
    ) -> dict[str, datetime]:
        """For each of `keys` (contract_ids or tickers), returns
        `window_start` if `full_rescan` is set or no existing bar is found,
        else the later of `window_start` and that key's latest existing bar
        timestamp. One batched `GROUP BY` query rather than one query per
        key — this runs on every single backfill invocation, so it's worth
        keeping to a single round trip even with a few hundred candidates.
        """
        defaults = {k: window_start for k in keys}
        if self._full_rescan or not keys:
            return defaults

        async with self._session_factory() as session:
            stmt = (
                select(key_column, func.max(model.time))
                .where(key_column.in_(keys))
                .group_by(key_column)
            )
            result = await session.execute(stmt)
            for key, latest in result.all():
                if latest is not None:
                    defaults[key] = max(window_start, self._as_utc(latest))
        return defaults

    async def reconcile_underlying_gaps(
        self, ticker: str, start: datetime, end: datetime, min_gap_minutes: int = 1
    ) -> dict:
        """Occasional-use companion to the fast incremental path in
        `run()`: scans *all* of `[start, end]` for missing expected minutes
        (via `service.ingestion.gap_detection`, so see that module's
        docstring for what counts as a gap and its holiday caveat), and if
        any are found, re-requests candles starting from the *earliest*
        gap rather than the latest bar — one request naturally streams
        through every subsequent gap too (existing-row dedup makes
        re-covering already-correct data in between harmless, just
        somewhat wasteful — acceptable for a deliberately-occasional job,
        unlike the routine `run()` path where that waste is exactly what
        was being optimized away).

        Only meaningful for underlying tickers, not option contracts — see
        `gap_detection`'s module docstring for why.

        **Can't recover data older than TastyTrade's ~6-week candle
        retention (Task 0 finding)** — a gap entirely outside that window
        is real, reported, but permanently unfillable; this still attempts
        the request (harmless — Task 0 confirmed over-requesting just
        returns whatever's actually available) but don't expect it to
        succeed for a gap that old.
        """
        async with self._session_factory() as session:
            stmt = select(UnderlyingBar1m.time).where(
                UnderlyingBar1m.ticker == ticker,
                UnderlyingBar1m.time >= start,
                UnderlyingBar1m.time <= end,
            )
            existing_times = {self._as_utc(t) for t in (await session.execute(stmt)).scalars().all()}

        gaps = find_gaps(existing_times, start, end, min_gap_minutes=min_gap_minutes)
        if not gaps:
            return {"ticker": ticker, "gaps_found": 0, "gaps_reconciled": 0, "bars_written": 0}

        earliest_gap_start = min(g.start for g in gaps)
        written = await self._backfill_underlying(ticker, earliest_gap_start)
        return {
            "ticker": ticker, "gaps_found": len(gaps), "gaps_reconciled": len(gaps),
            "bars_written": written,
        }

    async def _get_backfill_candidates(
        self, live_resolved: dict[str, ResolvedContract], start_time: datetime
    ) -> dict[str, ResolvedContract]:
        """Combines `live_resolved` (from ContractManager, still
        delta-eligible today) with contracts already persisted in the DB
        whose expiration falls within the backfill window. This is what
        recovers contracts that fully expired during downtime —
        ContractManager alone can't re-discover those, since it requires a
        live Greeks snapshot to resolve anything, and an expired contract
        doesn't have one. Doesn't need Greeks here at all — just the
        contract's identity, already sitting in the `contracts` table from
        when it was originally tracked.
        """
        candidates = dict(live_resolved)

        tickers = [t.ticker for t in self._settings.tickers]
        if not tickers:
            return candidates

        async with self._session_factory() as session:
            stmt = select(Contract).where(
                Contract.underlying_ticker.in_(tickers),
                Contract.expiration_date >= start_time.date(),
            )
            result = await session.execute(stmt)
            for row in result.scalars():
                if row.contract_id in candidates:
                    continue  # already covered by the live-resolved set
                candidates[row.contract_id] = ResolvedContract(
                    contract_id=row.contract_id,
                    underlying_ticker=row.underlying_ticker,
                    expiration_date=row.expiration_date,
                    strike=float(row.strike),
                    right=row.right,
                    settlement_type=row.settlement_type,
                )

        return candidates

    async def _backfill_option_contract(
        self, contract_id: str, resolved: ResolvedContract, start_time: datetime
    ) -> int:
        candles = await self._source.request_candles(contract_id, "1m", start_time)
        if not candles:
            return 0

        written = 0
        async with self._session_factory() as session:
            for candle in candles:
                bar_time = self._candle_time(candle)
                if bar_time is None:
                    continue

                existing = await session.get(OptionBar1m, {"time": bar_time, "contract_id": contract_id})
                if existing is not None:
                    continue  # live-ingested (or already-backfilled) row wins — see module docstring

                session.add(
                    OptionBar1m(
                        time=bar_time,
                        contract_id=contract_id,
                        underlying_ticker=resolved.underlying_ticker,
                        expiration_date=resolved.expiration_date,
                        strike=resolved.strike,
                        right=resolved.right,
                        open=get_attr_any(candle, "open"),
                        high=get_attr_any(candle, "high"),
                        low=get_attr_any(candle, "low"),
                        close=get_attr_any(candle, "close"),
                        iv=get_attr_any(candle, "imp_volatility"),
                        volume=get_attr_any(candle, "volume"),
                        open_interest=get_attr_any(candle, "open_interest"),
                        vwap=get_attr_any(candle, "vwap"),
                        bid_volume=get_attr_any(candle, "bid_volume"),
                        ask_volume=get_attr_any(candle, "ask_volume"),
                        # bid/ask (candles don't carry quotes) and all
                        # Greeks fields deliberately left NULL — see
                        # module docstring.
                    )
                )
                written += 1
            await session.commit()
        return written

    async def _backfill_underlying(self, ticker: str, start_time: datetime) -> int:
        candles = await self._source.request_candles(ticker, "1m", start_time)
        if not candles:
            return 0

        written = 0
        async with self._session_factory() as session:
            for candle in candles:
                bar_time = self._candle_time(candle)
                if bar_time is None:
                    continue

                existing = await session.get(UnderlyingBar1m, {"time": bar_time, "ticker": ticker})
                if existing is not None:
                    continue

                session.add(
                    UnderlyingBar1m(
                        time=bar_time,
                        ticker=ticker,
                        open=get_attr_any(candle, "open"),
                        high=get_attr_any(candle, "high"),
                        low=get_attr_any(candle, "low"),
                        close=get_attr_any(candle, "close"),
                        volume=get_attr_any(candle, "volume"),
                    )
                )
                written += 1
            await session.commit()
        return written

    @staticmethod
    def _candle_time(candle) -> datetime | None:
        """Candle `time` is milliseconds since epoch (confirmed in Task 0's
        captured samples)."""
        ms = get_attr_any(candle, "time")
        if ms is None:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def _main() -> None:
    from service.config.settings import get_settings as _get_settings
    from service.logging_config import configure_logging

    configure_logging("service.ingestion.backfill", _get_settings().log_level)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"How far back to request candles (default {DEFAULT_LOOKBACK_DAYS}; "
             "over-requesting beyond the actual ~6-week retention window is harmless).",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Ignore each contract/ticker's existing latest bar and always request the full "
             "lookback window (the original, slower, every-run-from-scratch behavior). Useful "
             "for an occasional deep re-verify; routine runs should omit this.",
    )
    parser.add_argument(
        "--reconcile-underlying-gaps", action="store_true",
        help="Instead of the normal backfill, scan each capture_underlying_bars ticker's full "
             "lookback window for missing minutes (service.ingestion.gap_detection) and, for "
             "any found, backfill from the earliest gap forward. Slower than a routine run "
             "(scans everything rather than just 'since last bar') — meant to be run "
             "occasionally/on-demand, not on every schedule tick. Does not touch option "
             "contracts — see gap_detection's module docstring for why.",
    )
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.db.session import get_session_factory
    from service.sources.tastytrade import TastyTradeSource

    settings = get_settings()
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        raise SystemExit("TASTYTRADE_CLIENT_SECRET / TASTYTRADE_REFRESH_TOKEN not set.")
    if not settings.tickers:
        raise SystemExit("No tickers configured in config.yaml.")

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )
    try:
        log.info("Authenticating...")
        await source.authenticate()
        job = BackfillJob(
            source, get_session_factory(), settings,
            lookback_days=args.lookback_days, full_rescan=args.full,
        )
        if args.reconcile_underlying_gaps:
            window_start = datetime.now(timezone.utc) - timedelta(days=args.lookback_days)
            window_end = datetime.now(timezone.utc)
            for ticker_cfg in settings.tickers:
                if not ticker_cfg.capture_underlying_bars:
                    continue
                result = await job.reconcile_underlying_gaps(
                    ticker_cfg.ticker, window_start, window_end
                )
                log.info("Gap reconciliation for %s: %s", ticker_cfg.ticker, result)
        else:
            await job.run()
    finally:
        await source.close()


if __name__ == "__main__":
    asyncio.run(_main())
