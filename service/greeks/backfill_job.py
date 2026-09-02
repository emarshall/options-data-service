"""
Black-Scholes reconciliation job.

Finds `option_bars_1m` rows with `greeks_source IS NULL` — written either
by Task 5's backfill (which never writes Greeks at all, only price/volume/
IV) or by Task 4's live ingestion when a Quote arrived but no matching
Greeks event did that minute (a genuine gap) — and computes delta/gamma/
theta/vega/rho for them via `service/greeks/black_scholes.py`.

**Time-to-expiration convention:** assumes standard US equity/index option
expiration at 4:00 PM America/New_York time on the contract's expiration
date. Handled via `zoneinfo` (stdlib) so DST transitions are accounted for
automatically rather than needing manual UTC-offset arithmetic. This is a
simplification for products with a genuinely different expiration time
(some index variants settle differently) — not expected to matter for the
project's core equity-ETF-option focus, but worth knowing about.

**Requires a matching underlying bar at the exact same minute** to get a
spot price. If Task 5 didn't backfill the underlying for that ticker (see
`capture_underlying_bars`), or there's simply a gap in the underlying's own
data for that minute, the option row is skipped this run — not an error,
just nothing to compute against. It'll be picked up in a later run if the
underlying bar becomes available (e.g. after a subsequent backfill).

**Pagination:** repeatedly queries `WHERE greeks_source IS NULL LIMIT
batch_size` rather than using OFFSET — since successfully processed rows
drop out of that WHERE clause (they're no longer NULL), each iteration
naturally sees fresh work without OFFSET's well-known degradation on large
tables. Stops when a batch returns fewer rows than requested (nothing left)
or when `max_rows` (if given) is reached.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, datetime, time as dt_time, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select

from service.db.models import GreeksSource, OptionBar1m, OptionRight, UnderlyingBar1m
from service.greeks.black_scholes import Right, compute_greeks

log = logging.getLogger(__name__)

_MARKET_TZ = ZoneInfo("America/New_York")
_MARKET_CLOSE = dt_time(16, 0)  # 4:00 PM ET — standard equity/index option expiration


def _time_to_expiry_years(bar_time: datetime, expiration_date: date) -> float:
    # Defensive: SQLite (used in tests) doesn't preserve tzinfo the way
    # Postgres's TIMESTAMPTZ does — a round-tripped datetime can come back
    # naive there even though production always stores/reads UTC. Assume
    # UTC if naive rather than letting the subtraction below raise.
    if bar_time.tzinfo is None:
        bar_time = bar_time.replace(tzinfo=timezone.utc)
    expiry_dt = datetime.combine(expiration_date, _MARKET_CLOSE, tzinfo=_MARKET_TZ)
    expiry_utc = expiry_dt.astimezone(timezone.utc)
    return (expiry_utc - bar_time).total_seconds() / (365.25 * 24 * 3600)


def _to_bs_right(right: OptionRight) -> Right:
    return Right.CALL if right == OptionRight.CALL else Right.PUT


class GreeksBackfillJob:
    def __init__(self, session_factory, risk_free_rate: float, batch_size: int = 500):
        self._session_factory = session_factory
        self._risk_free_rate = risk_free_rate
        self._batch_size = batch_size

    async def run(self, max_rows: int | None = None) -> dict:
        summary = {
            "computed": 0,
            "skipped_expired": 0,
            "skipped_no_underlying_bar": 0,
            "skipped_no_iv_or_price": 0,
            "skipped_compute_failed": 0,
            "rows_processed": 0,
        }

        while max_rows is None or summary["rows_processed"] < max_rows:
            batch_limit = self._batch_size
            if max_rows is not None:
                batch_limit = min(batch_limit, max_rows - summary["rows_processed"])
                if batch_limit <= 0:
                    break

            async with self._session_factory() as session:
                stmt = (
                    select(OptionBar1m)
                    .where(OptionBar1m.greeks_source.is_(None))
                    .limit(batch_limit)
                )
                result = await session.execute(stmt)
                rows = result.scalars().all()
                if not rows:
                    break

                for row in rows:
                    outcome = await self._compute_for_row(session, row)
                    summary[outcome] += 1

                await session.commit()

            summary["rows_processed"] += len(rows)
            if len(rows) < batch_limit:
                break  # fewer than requested = nothing left to drain

        log.info("Greeks backfill summary: %s", summary)
        return summary

    async def _compute_for_row(self, session, row: OptionBar1m) -> str:
        T = _time_to_expiry_years(row.time, row.expiration_date)
        if T <= 0:
            return "skipped_expired"

        underlying = await session.get(
            UnderlyingBar1m, {"time": row.time, "ticker": row.underlying_ticker}
        )
        if underlying is None or underlying.close is None:
            return "skipped_no_underlying_bar"

        iv = float(row.iv) if row.iv is not None else None
        option_price = float(row.close) if row.close is not None else None
        if iv is None and option_price is None:
            return "skipped_no_iv_or_price"

        result = compute_greeks(
            S=float(underlying.close),
            K=float(row.strike),
            T=T,
            r=self._risk_free_rate,
            right=_to_bs_right(row.right),
            iv=iv,
            option_price=option_price if iv is None else None,
        )
        if result is None:
            return "skipped_compute_failed"

        row.delta = result.delta
        row.gamma = result.gamma
        row.theta = result.theta
        row.vega = result.vega
        row.rho = result.rho
        if row.iv is None:
            row.iv = result.iv  # was back-solved — persist it too
        row.greeks_source = GreeksSource.COMPUTED
        return "computed"


async def _main() -> None:
    from service.config.settings import get_settings as _get_settings
    from service.logging_config import configure_logging

    configure_logging("service.greeks.backfill_job", _get_settings().log_level)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Cap on rows processed this run (default: unbounded — process everything available).",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.db.session import get_session_factory

    settings = get_settings()
    job = GreeksBackfillJob(
        get_session_factory(), settings.risk_free_rate, batch_size=args.batch_size
    )
    await job.run(max_rows=args.max_rows)


if __name__ == "__main__":
    asyncio.run(_main())
