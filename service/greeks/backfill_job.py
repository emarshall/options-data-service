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
spot price. If the underlying bar is missing, the option row is skipped.

### Why this job used to hang forever

The first implementation paginated with a bare
`WHERE greeks_source IS NULL LIMIT batch_size` and relied on "processed
rows drop out of the WHERE clause" to advance. That assumption holds only
for rows that actually *compute*. Any row that gets **skipped** stays
`greeks_source IS NULL`, so it is returned again by the very next query —
and, with no `ORDER BY`, Postgres reliably returned the same leading block
every time. A table whose first N rows were all uncomputable therefore
spun forever: ~500 wasted `session.get()` round-trips per iteration, zero
progress, and (the only log line being the end-of-run summary) zero
output. On the real dataset that's 6.0M NULL rows, 1.9M of them
uncomputable — a "run" that burned 20h and computed nothing.

Three things prevent that now, and all three matter:

1. **Keyset pagination** (`WHERE (time, contract_id) > (:last_time,
   :last_id) ORDER BY time, contract_id`) instead of bare `LIMIT`. The
   cursor advances past every row it *examined*, skipped or not, so each
   row is visited at most once per run and the loop is guaranteed to
   terminate. This is the actual fix; the query filter below is an
   optimization on top of it.
2. **A SQL prefilter** that drops rows which can *never* be computed —
   expired, or missing an underlying bar that's now older than TastyTrade's
   candle retention (see `RETENTION_DAYS` in `service/ingestion/
   gap_detection.py`). Without this the job would still visit 1.6M dead
   rows and skip them one batch at a time: correct, but pointless work.
   It is deliberately a *prefilter only* — a row that survives it can
   still be skipped at runtime, and the cursor guarantee (1) is what makes
   that safe. Rows that are merely uncomputable *right now* (no underlying
   bar yet, but inside the retention window) are still served, so a later
   `backfill --reconcile-underlying-gaps` run can unblock them.
3. **Per-batch progress logging**, so a long run visibly moves. See
   `_PROGRESS_EVERY_BATCHES` for the cadence rationale.

### Other performance notes

The underlying spot price is fetched with **one batched query per batch**
(`_fetch_underlyings`, keyed on the composite PK) rather than one
`session.get()` per row, and computed rows are written with a single
executemany `UPDATE` per batch rather than N round-trips. Both were
N+1-per-row before; at 4.1M rows the difference is hours.

The prefilter's `EXISTS` subquery is evaluated per chunk, which is why
migration 0004 adds a partial index on `greeks_source IS NULL` — without
it each batch re-scans the hypertable.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time as time_mod
from datetime import date, datetime, time as dt_time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, func, or_, select, tuple_, update

from service.db.models import GreeksSource, OptionBar1m, OptionRight, UnderlyingBar1m
from service.greeks.black_scholes import Right, compute_greeks
from service.ingestion.gap_detection import RETENTION_DAYS

_MARKET_TZ = ZoneInfo("America/New_York")
_MARKET_CLOSE = dt_time(16, 0)  # 4:00 PM ET — standard equity/index option expiration

# NOT `logging.getLogger(__name__)`. This module is normally run as
# `python -m service.greeks.backfill_job`, which executes it under the
# name `__main__` — so `__name__` here would be `"__main__"`, a *different*
# logger from the `"service.greeks.backfill_job"` one that
# `configure_logging` raises to the configured level. The `__main__` logger
# would stay at the root WARNING level and every progress line would be
# silently dropped — which is exactly the "it doesn't log anything"
# symptom this job was reported to have. Naming the logger explicitly
# keeps the module's logs under `service.*` no matter how it's invoked.
log = logging.getLogger("service.greeks.backfill_job")

_MARKET_TZ = ZoneInfo("America/New_York")
_MARKET_CLOSE = dt_time(16, 0)  # 4:00 PM ET — standard equity/index option expiration

# Log every N batches rather than every batch. At the default batch size a
# 4.1M-row drain is ~8k batches; one line per batch would bury the signal
# this logging exists to provide (see the "why this used to hang" section).
# Every ~10k rows is frequent enough to distinguish "working" from "hung"
# within a couple of minutes, without becoming its own log-volume problem.
_PROGRESS_EVERY_BATCHES = 20


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
    def __init__(self, session_factory, risk_free_rate: float, batch_size: int = 2000):
        self._session_factory = session_factory
        self._risk_free_rate = risk_free_rate
        self._batch_size = batch_size

    def _build_batch_query(
        self,
        batch_limit: int,
        last_time,
        last_contract_id,
        cutoff,
        coverage: dict[str, tuple[datetime, datetime]],
    ):
        """Rows worth *examining*: NULL greeks, and either recent enough
        that a missing underlying bar might still be backfilled, or
        falling inside a window where this ticker's underlying bars
        actually exist.

        `coverage` maps ticker -> (earliest, latest) underlying bar, read
        once per run by `_underlying_coverage`. It replaces a correlated
        `EXISTS (...)` that was here before: the planner evaluated that
        subquery *per row per chunk*, re-planning a ChunkAppend across
        the underlying hypertable each time (5,470 executions on a single
        500-row batch). That put the N+1 back at the SQL layer after it
        had just been removed from the Python loop, and cost ~37ms of
        fixed overhead per batch before any rows were even returned — at
        ~8,200 batches that dominated the run.

        A precomputed range can't tell you whether a *specific* minute
        has a bar, so it's deliberately looser: it drops the rows that
        fall outside a ticker's entire coverage (the bulk of the dead
        pre-backfill data) and leaves the rest for `_fetch_underlyings`
        to resolve exactly. Same trade as the N+1 fix — approximate in
        SQL, exact in Python — but without the per-row cost.

        The two conditions dropped from the prefilter are the `iv`/`close`
        NULL pair and expiry, and both are handled exactly (and counted)
        by `_compute_for_row` instead. That's deliberate rather than an
        oversight:

        - Expiry needs "4:00 PM America/New_York on the expiration date"
          evaluated in SQL. Doing that portably across Postgres and the
          in-memory SQLite the tests run against means either a
          timezone-dependent DB function (breaks SQLite) or a fixed UTC
          offset (silently wrong across DST). Since the keyset cursor
          makes skipping harmless, the exact check belongs in Python.
        - The `iv IS NULL AND close IS NULL` pair is currently 0 rows
          against the real dataset, so there's nothing to prefilter.
        """
        conditions = [OptionBar1m.greeks_source.is_(None)]

        # Rows inside a covered window but before/after any actual bar.
        # Skipped entirely when no underlying data exists at all, which
        # would otherwise make the OR below a no-op.
        worth_examining = OptionBar1m.time >= cutoff
        if coverage:
            worth_examining = worth_examining | or_(
                *(
                    (OptionBar1m.underlying_ticker == ticker)
                    & (OptionBar1m.time >= lo)
                    & (OptionBar1m.time <= hi)
                    for ticker, (lo, hi) in coverage.items()
                )
            )
        conditions.append(worth_examining)

        stmt = (
            select(OptionBar1m)
            .where(*conditions)
            .order_by(OptionBar1m.time, OptionBar1m.contract_id)
            .limit(batch_limit)
        )
        if last_time is not None:
            # Keyset cursor. Row-value comparison is supported by both
            # Postgres and SQLite (SQLite 3.15+), so this stays portable.
            stmt = stmt.where(
                tuple_(OptionBar1m.time, OptionBar1m.contract_id)
                > tuple_(last_time, last_contract_id)
            )
        return stmt

    async def _underlying_coverage(self, session) -> dict[str, tuple[datetime, datetime]]:
        """Earliest/latest underlying bar per ticker — one grouped query
        for the whole run, instead of a per-row existence check."""
        stmt = select(
            UnderlyingBar1m.ticker,
            func.min(UnderlyingBar1m.time),
            func.max(UnderlyingBar1m.time),
        ).group_by(UnderlyingBar1m.ticker)
        result = await session.execute(stmt)
        return {ticker: (lo, hi) for ticker, lo, hi in result.all()}

    async def run(self, max_rows: int | None = None) -> dict:
        summary = {
            "computed": 0,
            "skipped_expired": 0,
            "skipped_no_underlying_bar": 0,
            "skipped_no_iv_or_price": 0,
            "skipped_compute_failed": 0,
            "rows_processed": 0,
            "batches": 0,
        }

        run_started = time_mod.monotonic()
        last_time = None
        last_contract_id = None
        # Computed once, not per batch: a run that crosses the retention
        # boundary mid-flight shouldn't shift its own definition of
        # "recent" underneath itself.
        cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

        async with self._session_factory() as session:
            coverage = await self._underlying_coverage(session)
        log.info(
            "Greeks backfill starting: underlying coverage known for %d ticker(s)",
            len(coverage),
        )

        while max_rows is None or summary["rows_processed"] < max_rows:
            batch_limit = self._batch_size
            if max_rows is not None:
                batch_limit = min(batch_limit, max_rows - summary["rows_processed"])
                if batch_limit <= 0:
                    break

            async with self._session_factory() as session:
                stmt = self._build_batch_query(
                    batch_limit, last_time, last_contract_id, cutoff, coverage
                )
                result = await session.execute(stmt)
                rows = result.scalars().all()
                if not rows:
                    break

                underlyings = await self._fetch_underlyings(session, rows)

                to_update = []
                for row in rows:
                    outcome, computed = self._compute_for_row(row, underlyings)
                    summary[outcome] += 1
                    if computed is not None:
                        to_update.append(computed)

                if to_update:
                    await self._bulk_update(session, to_update)
                await session.commit()

            summary["rows_processed"] += len(rows)
            summary["batches"] += 1

            # Advance the cursor past every row examined, whether or not it
            # computed. This is what guarantees termination.
            last_time, last_contract_id = rows[-1].time, rows[-1].contract_id

            if summary["batches"] % _PROGRESS_EVERY_BATCHES == 0:
                elapsed = time_mod.monotonic() - run_started
                rate = summary["rows_processed"] / elapsed if elapsed > 0 else 0.0
                log.info(
                    "Greeks backfill progress: %d rows processed "
                    "(%d computed, %d skipped) in %d batches, %.0fs elapsed "
                    "(%.0f rows/s)",
                    summary["rows_processed"],
                    summary["computed"],
                    summary["rows_processed"] - summary["computed"],
                    summary["batches"],
                    elapsed,
                    rate,
                )

        elapsed = time_mod.monotonic() - run_started
        log.info("Greeks backfill summary: %s (%.0fs elapsed)", summary, elapsed)
        return summary

    async def _fetch_underlyings(self, session, rows) -> dict:
        """One query for the whole batch's spot prices, keyed by
        `(time, ticker)` — the UnderlyingBar1m composite PK.

        Replaces a per-row `session.get()`. Note the batch may contain many
        rows sharing a minute (all the strikes for one underlying), so the
        deduped key set is usually far smaller than the row count.
        """
        keys = {(row.time, row.underlying_ticker) for row in rows}
        if not keys:
            return {}
        stmt = select(UnderlyingBar1m).where(
            tuple_(UnderlyingBar1m.time, UnderlyingBar1m.ticker).in_(keys)
        )
        result = await session.execute(stmt)
        return {(u.time, u.ticker): u for u in result.scalars().all()}

    async def _bulk_update(self, session, to_update: list[dict]) -> None:
        """Write a whole batch with one executemany round-trip.

        Issued against the Core `Table` rather than the ORM class on
        purpose: `session.execute(update(OptionBar1m), ...)` gets
        intercepted by the ORM's per-row bulk-UPDATE-by-PK path, which
        wants mapped-attribute PK names and refuses a WHERE-clause update
        outright. The Core form compiles to the same
        `UPDATE ... WHERE time = ? AND contract_id = ?` with no ORM
        involvement.

        `iv` is always in the SET list, but bound to the row's *existing*
        value when it already had one — so a pre-existing IV is preserved
        rather than clobbered by a re-derived one. Binding it
        unconditionally (rather than only for back-solved rows) is
        required: executemany needs one consistent parameter set across
        the batch, so a conditionally-present key would leave the UPDATE
        with a parameter it can't resolve.
        """
        tbl = OptionBar1m.__table__
        stmt = (
            update(tbl)
            .where(tbl.c.time == bindparam("p_time"))
            .where(tbl.c.contract_id == bindparam("p_contract_id"))
            .values(
                delta=bindparam("p_delta"),
                gamma=bindparam("p_gamma"),
                theta=bindparam("p_theta"),
                vega=bindparam("p_vega"),
                rho=bindparam("p_rho"),
                iv=bindparam("p_iv"),
                greeks_source=bindparam("p_greeks_source"),
            )
        )
        await session.execute(stmt, to_update)

    def _compute_for_row(self, row: OptionBar1m, underlyings: dict):
        """Pure computation — no session, no I/O. Returns
        `(outcome, params_or_None)`; `params` is the dict to hand to
        `_bulk_update`, or None when the row was skipped.

        Kept separate from the DB layer (and returning params rather than
        mutating the ORM object) so the per-row logic is directly testable
        and so a batch can be written in a single statement.
        """
        T = _time_to_expiry_years(row.time, row.expiration_date)
        if T <= 0:
            return "skipped_expired", None

        underlying = underlyings.get((row.time, row.underlying_ticker))
        if underlying is None or underlying.close is None:
            return "skipped_no_underlying_bar", None

        iv = float(row.iv) if row.iv is not None else None
        option_price = float(row.close) if row.close is not None else None
        if iv is None and option_price is None:
            return "skipped_no_iv_or_price", None

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
            return "skipped_compute_failed", None

        params = {
            "p_time": row.time,
            "p_contract_id": row.contract_id,
            "p_delta": result.delta,
            "p_gamma": result.gamma,
            "p_theta": result.theta,
            "p_vega": result.vega,
            "p_rho": result.rho,
            # Preserve whatever IV the row already had; only the
            # back-solved fallback produces a new one.
            "p_iv": row.iv if row.iv is not None else result.iv,
            "p_greeks_source": GreeksSource.COMPUTED,
        }
        return "computed", params


async def _main() -> None:
    from service.config.settings import get_settings as _get_settings
    from service.logging_config import configure_logging

    configure_logging("service.greeks.backfill_job", _get_settings().log_level)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Cap on rows processed this run (default: unbounded — process everything available).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Rows per batch. Larger amortizes the fixed per-batch query "
             "cost across more rows; the default is 4x the original 500 "
             "after profiling showed per-batch overhead dominating.",
    )
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
