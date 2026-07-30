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

from sqlalchemy import select

from service.config.settings import AppConfig
from service.db.models import Contract, OptionBar1m, UnderlyingBar1m
from service.ingestion.contract_manager import ContractManager, ResolvedContract
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
    ):
        self._source = source
        self._session_factory = session_factory
        self._settings = settings
        self._lookback_days = lookback_days
        self._contract_manager = ContractManager(source, session_factory, settings.tickers)

    async def run(self) -> dict:
        """Resolves the current contract set, then backfills 1m candles for
        every tracked option contract plus each configured underlying.
        Returns a summary dict (also logged) — useful for the smoke test
        and for tests to assert against."""
        diff = await self._contract_manager.refresh()
        start_time = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)

        candidates = await self._get_backfill_candidates(diff.current, start_time)
        recovered_count = len(candidates) - len(diff.current)

        summary = {
            "contracts_attempted": 0, "contracts_failed": 0, "option_bars_written": 0,
            "contracts_recovered_from_db": max(recovered_count, 0),
            "underlyings_attempted": 0, "underlyings_failed": 0, "underlying_bars_written": 0,
        }

        for contract_id, resolved in candidates.items():
            summary["contracts_attempted"] += 1
            try:
                summary["option_bars_written"] += await self._backfill_option_contract(
                    contract_id, resolved, start_time
                )
            except Exception:
                summary["contracts_failed"] += 1
                log.exception(
                    "Backfill failed for contract %s — continuing with the rest.", contract_id
                )

        underlying_tickers = {t.ticker for t in self._settings.tickers if t.capture_underlying_bars}
        for ticker in underlying_tickers:
            summary["underlyings_attempted"] += 1
            try:
                summary["underlying_bars_written"] += await self._backfill_underlying(ticker, start_time)
            except Exception:
                summary["underlyings_failed"] += 1
                log.exception("Backfill failed for underlying %s — continuing with the rest.", ticker)

        log.info("Backfill summary: %s", summary)
        return summary

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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"How far back to request candles (default {DEFAULT_LOOKBACK_DAYS}; "
             "over-requesting beyond the actual ~6-week retention window is harmless).",
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
        job = BackfillJob(source, get_session_factory(), settings, lookback_days=args.lookback_days)
        await job.run()
    finally:
        await source.close()


if __name__ == "__main__":
    asyncio.run(_main())
