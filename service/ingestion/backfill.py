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
import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from service.config.settings import AppConfig
from service.db.models import Contract, OptionBar1m, UnderlyingBar1m
from service.ingestion.contract_manager import ContractManager, ResolvedContract
from service.ingestion.gap_detection import RETENTION_DAYS, find_gaps
from service.sources._async_utils import get_attr_any
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)

# Comfortably beyond Task 0's confirmed ~6-week (~43 day) retention window
# for OPTION CONTRACTS specifically. Over-requesting is harmless for a
# normal backfill request — confirmed in Task 0 that the feed just
# returns whatever it actually has, it doesn't error on this. NOTE:
# underlying candle retention is shorter and count-based, not this
# fixed-day figure — see RETENTION_DAYS in gap_detection.py, which is
# what actually bounds underlying gap scanning/reconciliation. This
# constant staying generous is still fine/harmless for underlyings too,
# for the same "over-requesting is harmless" reason — it's only
# RETENTION_DAYS that needed correcting, since that one drives whether a
# gap gets treated as actionable.
DEFAULT_LOOKBACK_DAYS = 60


class BackfillJob:
    def __init__(
        self,
        source: MarketDataSource,
        session_factory,
        settings: AppConfig,
        lookback_days: int = DEFAULT_LOOKBACK_DAYS,
        full_rescan: bool = False,
        contract_concurrency: int = 8,
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
        # How many contracts' request_candles() calls run concurrently.
        # Added after a real report of a multi-hour run against ~a couple
        # hundred contracts (SPX/NDX's daily 0DTE listings over a 10-day
        # window, each with several strikes in range, adds up fast) — see
        # PLAN.md Section 7. DXLink multiplexes many symbol subscriptions
        # over one websocket connection already, and each contract's
        # events are correctly isolated by `request_candles`'
        # symbol-matching event_filter, so concurrent calls on the same
        # underlying streamer connection are expected to be safe; 8 is a
        # conservative starting point, not a measured optimum — raise it
        # if a real run shows the bottleneck is elsewhere (e.g. DB
        # writes), lower it if concurrent subscriptions turn out to
        # interact badly in practice (not verified against a live
        # connection in this environment).
        self._contract_concurrency = contract_concurrency
        self._contract_manager = ContractManager(source, session_factory, settings.tickers)

    @staticmethod
    def _write_report(report: dict, path_prefix: str) -> None:
        """Writes `<path_prefix>.json` (full detail, machine-readable) and
        `<path_prefix>.md` (the same data, formatted for a human to read
        without needing to parse JSON). Added specifically so
        troubleshooting a slow/incomplete backfill or gap-reconcile run
        doesn't require pasting a huge raw console log — see PLAN.md
        Section 7, where several rounds of exactly that were needed before
        this existed. Best-effort: a failure writing the report is logged,
        not raised — a diagnostic aid failing shouldn't fail the actual
        job.
        """
        from pathlib import Path

        try:
            path = Path(path_prefix)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.with_suffix(".json").write_text(json.dumps(report, indent=2, default=str))
            path.with_suffix(".md").write_text(_report_to_markdown(report))
            log.info("Report written to %s.md / .json", path)
        except Exception:
            log.exception("Failed to write diagnostic report to %s — continuing anyway.", path_prefix)

    async def run(self, report_path: str | None = None) -> dict:
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

        **Option contracts are backfilled concurrently** (bounded by
        `contract_concurrency`, default 8 — see the constructor's own
        comment), not one at a time — added after a real report of a
        multi-hour run against a few hundred contracts (SPX/NDX's daily
        0DTE listings over a wide DTE window, each with several strikes
        in range, adds up fast when strictly sequential). Progress is
        logged periodically so a long run is visibly making progress
        rather than looking stuck.

        **`report_path`, if given, writes a diagnostic report** (see
        `_write_report`) with per-underlying request diagnostics (always
        — there are only ever a handful of underlyings) and per-*option
        contract* diagnostics for any contract that hit a truncation
        warning or failed outright (not every contract — with a few
        hundred tracked, that would make the report itself the next
        "too much output" problem; the aggregate `summary` counts already
        cover the boring/successful majority). Added specifically so
        troubleshooting doesn't require pasting a huge raw console log —
        see PLAN.md Section 7.
        """
        run_started = datetime.now(timezone.utc)
        diff = await self._contract_manager.refresh()
        window_start = datetime.now(timezone.utc) - timedelta(days=self._lookback_days)

        candidates = await self._get_backfill_candidates(diff.current, window_start)
        recovered_count = len(candidates) - len(diff.current)

        summary = {
            "contracts_attempted": len(candidates), "contracts_failed": 0, "option_bars_written": 0,
            "contracts_recovered_from_db": max(recovered_count, 0),
            "underlyings_attempted": 0, "underlyings_failed": 0, "underlying_bars_written": 0,
        }

        option_starts = await self._effective_start_times(
            OptionBar1m, OptionBar1m.contract_id, list(candidates.keys()), window_start
        )

        semaphore = asyncio.Semaphore(max(1, self._contract_concurrency))
        progress_lock = asyncio.Lock()
        completed = 0
        total = len(candidates)
        option_diagnostics_of_interest: list[dict] = []

        async def _process_one(contract_id: str, resolved: ResolvedContract) -> None:
            nonlocal completed
            async with semaphore:
                diag: dict = {} if report_path else None
                try:
                    written = await self._backfill_option_contract(
                        contract_id, resolved, option_starts[contract_id], diagnostics=diag
                    )
                    async with progress_lock:
                        summary["option_bars_written"] += written
                    if diag and diag.get("warnings"):
                        async with progress_lock:
                            option_diagnostics_of_interest.append(diag)
                except Exception as e:
                    async with progress_lock:
                        summary["contracts_failed"] += 1
                        if report_path:
                            option_diagnostics_of_interest.append({
                                "contract_id": contract_id, "error": str(e), **(diag or {}),
                            })
                    log.exception(
                        "Backfill failed for contract %s — continuing with the rest.", contract_id
                    )
            async with progress_lock:
                completed += 1
                # Every 25 contracts (not every one — that'd be its own
                # log-volume problem, see PLAN.md Section 7's earlier
                # logging entry) so a long run visibly shows it's
                # progressing rather than looking hung.
                if completed % 25 == 0 or completed == total:
                    elapsed = (datetime.now(timezone.utc) - run_started).total_seconds()
                    log.info(
                        "Backfill progress: %d/%d contracts processed (%.0fs elapsed, "
                        "%d failed so far)",
                        completed, total, elapsed, summary["contracts_failed"],
                    )

        if candidates:
            await asyncio.gather(*(_process_one(cid, r) for cid, r in candidates.items()))

        underlying_tickers = {t.ticker for t in self._settings.tickers if t.capture_underlying_bars}
        underlying_starts = await self._effective_start_times(
            UnderlyingBar1m, UnderlyingBar1m.ticker, list(underlying_tickers), window_start
        )
        underlying_diagnostics: list[dict] = []
        for ticker in underlying_tickers:
            summary["underlyings_attempted"] += 1
            diag = {} if report_path else None
            try:
                summary["underlying_bars_written"] += await self._backfill_underlying(
                    ticker, underlying_starts[ticker], diagnostics=diag
                )
            except Exception as e:
                summary["underlyings_failed"] += 1
                if report_path:
                    diag = {"ticker": ticker, "error": str(e), **(diag or {})}
                log.exception("Backfill failed for underlying %s — continuing with the rest.", ticker)
            if report_path and diag:
                diag.setdefault("requested_start", underlying_starts[ticker].isoformat())
                underlying_diagnostics.append(diag)

        log.info("Backfill summary: %s", summary)

        if report_path:
            finished = datetime.now(timezone.utc)
            self._write_report({
                "run_type": "backfill",
                "started_at": run_started.isoformat(),
                "finished_at": finished.isoformat(),
                "elapsed_s": round((finished - run_started).total_seconds(), 2),
                "full_rescan": self._full_rescan,
                "lookback_days": self._lookback_days,
                "contract_concurrency": self._contract_concurrency,
                "summary": summary,
                "underlyings": underlying_diagnostics,
                "option_contracts_of_interest": option_diagnostics_of_interest,
            }, report_path)


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
        self, ticker: str, start: datetime, end: datetime, min_gap_minutes: int = 1,
        now: datetime | None = None, collect_diagnostics: bool = False,
    ) -> dict:
        """Occasional-use companion to the fast incremental path in
        `run()`: scans `[start, end]` — clipped to the last `RETENTION_DAYS`
        (see that constant's own comment for why) — for missing expected
        minutes (via `service.ingestion.gap_detection`, so see that
        module's docstring for what counts as a gap and its holiday
        caveat), and if any are found, re-requests candles starting from
        the *earliest* gap rather than the latest bar — one request
        naturally streams through every subsequent gap too (existing-row
        dedup makes re-covering already-correct data in between harmless,
        just somewhat wasteful — acceptable for a deliberately-occasional
        job, unlike the routine `run()` path where that waste is exactly
        what was being optimized away).

        Only meaningful for underlying tickers, not option contracts — see
        `gap_detection`'s module docstring for why.

        **Deliberately does not scan/report/attempt anything older than
        `RETENTION_DAYS`** (found from a real false alarm — see PLAN.md
        Section 7): time before that can never have a bar no matter what,
        so treating it as an actionable gap the same way as a real one
        means this would forever "try to fix," and forever fail to fix,
        the exact same unfillable stretch every single run, and trip the
        (otherwise useful) truncation-detection warning in
        `_backfill_underlying` every time too — both are just noise once
        the sliding retention window has moved past a given date, not a
        real, ongoing problem. If `start` is older than the retention
        cutoff, it's silently raised to that cutoff instead — the returned
        dict's `scan_start` reports what was actually used, and
        `requested_start_before_retention` flags when that clipping
        happened, so a caller/log-reader isn't left wondering why less was
        scanned than they asked for.

        **`collect_diagnostics`, if set, adds a `request_diagnostics` key**
        to the returned dict with everything `request_candles`/
        `collect_events` captured about the actual fetch this call
        triggered (`stop_reason`, `elapsed_s`, `earliest_candle`,
        `latest_candle`, etc.) — used by the CLI's `--report` option. See
        PLAN.md Section 7.
        """
        now = now or datetime.now(timezone.utc)
        retention_cutoff = now - timedelta(days=RETENTION_DAYS)
        scan_start = max(start, retention_cutoff)

        async with self._session_factory() as session:
            stmt = select(UnderlyingBar1m.time).where(
                UnderlyingBar1m.ticker == ticker,
                UnderlyingBar1m.time >= scan_start,
                UnderlyingBar1m.time <= end,
            )
            existing_times = {self._as_utc(t) for t in (await session.execute(stmt)).scalars().all()}

        gaps = find_gaps(existing_times, scan_start, end, min_gap_minutes=min_gap_minutes)
        result_base = {
            "ticker": ticker, "scan_start": scan_start, "scan_end": end,
            "requested_start_before_retention": start < retention_cutoff,
            "gaps_preview": [
                {"start": g.start.isoformat(), "end": g.end.isoformat(), "minutes": g.minutes}
                for g in sorted(gaps, key=lambda g: g.start)[:10]
            ] if collect_diagnostics else None,
        }
        if not gaps:
            return {**result_base, "gaps_found": 0, "gaps_reconciled": 0, "bars_written": 0}

        earliest_gap_start = min(g.start for g in gaps)
        diag: dict = {} if collect_diagnostics else None
        written = await self._backfill_underlying(ticker, earliest_gap_start, diagnostics=diag)
        result = {
            **result_base, "gaps_found": len(gaps), "gaps_reconciled": len(gaps),
            "bars_written": written,
        }
        if collect_diagnostics:
            result["request_diagnostics"] = diag
        return result

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

    # Underlying candle pulls are much larger/denser than a typical option
    # contract's (see request_candles' docstring) — a longer idle timeout
    # tolerates normal server-side batching pauses in a big historical
    # replay without misreading them as "no more data." Left at the
    # (shorter, faster) default for option contracts specifically, since
    # those genuinely are sparse and a long idle timeout there would just
    # slow down every contract's backfill for no benefit.
    #
    # `_UNDERLYING_TIMEOUT_S`: **reduced to 300s in an earlier pass, then
    # raised back — that reduction was a real mistake, caught from a
    # follow-up report that multi-week gaps still weren't closing even
    # after the repeated-key fix (see PLAN.md Section 7).** The mistake:
    # conflating two *different* things. `stop_on_repeated_key` only
    # solves *knowing when history is done* (detecting the live-tail
    # repeat) — it does nothing to speed up *receiving* the historical
    # backlog itself, which the retention probe measured at close to 900s
    # of real transfer time for a full ~43-day underlying history,
    # independent of that detection question entirely. A large
    # `reconcile_underlying_gaps` request (a multi-week gap) still has to
    # receive all of that volume *before* ever reaching the live tail
    # where the repeated-key condition would even apply — cutting the
    # ceiling to 300s meant those requests were being cut off well before
    # the historical transfer itself finished, silently, same failure
    # shape as the very first bug in this whole investigation. Restored
    # to comfortably above the ~900s the probe needed. The repeated-key
    # fix still provides a real, separate speed benefit for the *common*
    # case — a routine incremental call with only a small recent window
    # to catch up on reaches the live tail almost immediately and stops
    # there, rather than idling out a large timeout for no reason; it's
    # only large, multi-week catch-up requests (new tickers, `--full`,
    # gap reconciliation) that still need to actually wait out something
    # close to the full transfer time, and always will, until the
    # underlying transfer-rate constraint itself is understood.
    #
    # `_UNDERLYING_IDLE_TIMEOUT_S`: raised again after real evidence from
    # an actual `--report`'d `gap-reconcile` run against SPX/NDX/VIX (see
    # PLAN.md Section 7 — this one genuinely happened, unlike an earlier,
    # since-corrected entry that fabricated a scenario resembling this).
    # The real report showed `stop_reason: idle_timeout` for SPX and VIX
    # (not `repeated_key`, not `outer_timeout`) — SPX in particular wrote
    # only 16 bars despite the request nominally spanning six weeks,
    # while NDX, which *did* hit `repeated_key` in the same run, wrote
    # 425. That pattern (wildly different bars_written correlating with
    # which stop condition fired, not with anything about the symbols
    # themselves) points at idle_timeout_s firing on an ordinary pause
    # partway through delivery, not at genuinely running out of data —
    # consistent with the original hypothesis (a large historical replay
    # has pauses between batches) but now with a real, direct data point
    # confirming it still happens even at the previously-raised value.
    # Raised further as the next experiment — genuine uncertainty remains
    # about the true pause duration, so this isn't presented as a proven
    # fix, just the next reasonable value to try given real evidence the
    # previous one wasn't always enough.
    _UNDERLYING_IDLE_TIMEOUT_S = 60.0
    _UNDERLYING_TIMEOUT_S = 1800.0

    async def _backfill_option_contract(
        self, contract_id: str, resolved: ResolvedContract, start_time: datetime,
        diagnostics: dict | None = None,
    ) -> int:
        request_diag: dict = {} if diagnostics is not None else None
        candles = await self._source.request_candles(
            contract_id, "1m", start_time, diagnostics=request_diag
        )
        warnings: list[str] = []
        if not candles:
            written = 0
        else:
            warnings = self._warn_if_likely_truncated(contract_id, candles, start_time)
            written = 0
            async with self._session_factory() as session:
                for candle in candles:
                    bar_time = self._candle_time(candle)
                    if bar_time is None:
                        continue

                    existing = await session.get(
                        OptionBar1m, {"time": bar_time, "contract_id": contract_id}
                    )
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

        if diagnostics is not None:
            diagnostics.update(request_diag or {})
            diagnostics.update({
                "contract_id": contract_id, "bars_written": written, "warnings": warnings,
            })
        return written

    async def _backfill_underlying(
        self, ticker: str, start_time: datetime, diagnostics: dict | None = None
    ) -> int:
        request_diag: dict = {} if diagnostics is not None else None
        candles = await self._source.request_candles(
            ticker, "1m", start_time,
            timeout_s=self._UNDERLYING_TIMEOUT_S, idle_timeout_s=self._UNDERLYING_IDLE_TIMEOUT_S,
            diagnostics=request_diag,
        )
        warnings: list[str] = []
        skipped_existing = 0
        skipped_no_time = 0
        distinct_days: set = set()
        candles_per_day: dict = {}
        if not candles:
            written = 0
        else:
            warnings = self._warn_if_likely_truncated(ticker, candles, start_time)
            written = 0
            async with self._session_factory() as session:
                for candle in candles:
                    bar_time = self._candle_time(candle)
                    if bar_time is None:
                        skipped_no_time += 1
                        continue
                    day = bar_time.date()
                    distinct_days.add(day)
                    candles_per_day[day] = candles_per_day.get(day, 0) + 1

                    existing = await session.get(UnderlyingBar1m, {"time": bar_time, "ticker": ticker})
                    if existing is not None:
                        skipped_existing += 1
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

        if diagnostics is not None:
            diagnostics.update(request_diag or {})
            diagnostics.update({
                "ticker": ticker, "bars_written": written, "warnings": warnings,
                # Added specifically to distinguish "received a lot, mostly
                # already existed" from "received a lot, wrote almost none
                # of it for some other reason" — see PLAN.md Section 7,
                # where event_count/bars_written alone (8002 vs 9) weren't
                # enough to tell those apart.
                "skipped_existing": skipped_existing,
                "skipped_no_time_field": skipped_no_time,
                "distinct_days_in_result": len(distinct_days),
                # Added after distinct_days_in_result alone wasn't enough
                # either — it stayed identical across two runs (one that
                # stopped in ~1s, one that patiently rode out ~650s more)
                # while event_count grew, meaning whatever extra data
                # arrived during that extra time landed in days *already*
                # represented, not new ones. This per-day breakdown is
                # what actually shows density: a handful of stray events
                # on an old date (a boundary-marker artifact) looks very
                # different from real, dense multi-hundred-event coverage
                # of that date, and only this field can tell them apart.
                "candles_per_day": {
                    d.isoformat(): c for d, c in sorted(candles_per_day.items())
                },
            })
        return written

    @staticmethod
    def _warn_if_likely_truncated(
        symbol: str, candles: list, requested_start: datetime, now: datetime | None = None
    ) -> list[str]:
        """Best-effort diagnostic, not a correctness mechanism: flags a
        request whose result looks like it got cut short (by `max_count`,
        `timeout_s`, or `idle_timeout_s` — see `request_candles`'s
        docstring for the difference) rather than genuinely reflecting all
        the real data available. Turns what used to be a silent partial
        result into something visible in logs — and, since it also
        *returns* the same message(s) it logs, into something a caller
        can fold into a structured diagnostic report too (see
        `_write_report`) without needing to intercept logging. Checks two
        different, real failure modes found investigating actual reports
        (see PLAN.md Section 7) — neither one alone would have caught
        both:

        1. **Earliest candle much later than reasonably expected.**
           "Reasonably expected" is `max(requested_start, retention
           cutoff)`, not `requested_start` directly — `requested_start` is
           routinely further back than TastyTrade's ~`RETENTION_DAYS`-day
           candle retention on purpose (over-requesting is harmless per
           Task 0, and `reconcile_underlying_gaps` in particular may
           deliberately ask for a gap that predates retention entirely),
           so comparing against it directly flagged every one of those as
           "truncated" — every single run, forever — when the result was
           actually exactly right: real data starting right at the
           retention boundary.
        2. **Latest candle much earlier than "now."** A request that gets
           cut off by the overall `timeout_s` ceiling *before* catching up
           to live (confirmed to actually happen for a continuously-quoted
           underlying, whose full historical replay can take longer to
           fully stream than expected) looks completely fine on the
           earliest side — real data starting right where expected — while
           being silently short on the *recent* end instead. A day of
           slack on both checks avoids false positives from an ordinary
           weekend/holiday gap right at either edge.
        """
        now = now or datetime.now(timezone.utc)
        expected_earliest = max(requested_start, now - timedelta(days=RETENTION_DAYS))
        times = [t for t in (BackfillJob._candle_time(c) for c in candles) if t is not None]
        if not times:
            return []

        warnings: list[str] = []
        earliest, latest = min(times), max(times)
        if earliest - expected_earliest > timedelta(days=1):
            msg = (
                f"request_candles({symbol}) returned data starting at {earliest}, well after "
                f"the earliest reasonably-expected start {expected_earliest} (requested "
                f"{requested_start}) — this may be a truncated result (hit max_count/timeout_s/"
                f"idle_timeout_s — see request_candles' docstring) rather than genuinely missing "
                f"data. If this recurs, consider a larger idle_timeout_s/timeout_s for this symbol."
            )
            log.warning(msg)
            warnings.append(msg)
        if now - latest > timedelta(days=1):
            msg = (
                f"request_candles({symbol}) returned data ending at {latest}, well before now "
                f"({now}) — this may be a request that got cut off by timeout_s before catching "
                f"up to live, rather than genuinely having no more recent data. If this recurs, "
                f"consider a larger timeout_s for this symbol."
            )
            log.warning(msg)
            warnings.append(msg)
        return warnings

    @staticmethod
    def _candle_time(candle) -> datetime | None:
        """Candle `time` is milliseconds since epoch (confirmed in Task 0's
        captured samples)."""
        ms = get_attr_any(candle, "time")
        if ms is None:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _report_to_markdown(report: dict) -> str:
    """Formats a backfill/gap-reconcile diagnostic report (see
    `BackfillJob._write_report`) as readable markdown — a human-friendly
    companion to the `.json` file, not a replacement for it (anything not
    shown here is still in the JSON)."""
    lines = [
        f"# {report['run_type']} report — {report['started_at']}",
        "",
        f"Elapsed: {report['elapsed_s']}s "
        f"({report['started_at']} \u2192 {report['finished_at']})",
        "",
        "## Summary",
        "",
        "```",
        # `backfill` reports have a real `summary` dict (contract/underlying
        # counts) worth showing on its own; `gap_reconcile` reports don't —
        # everything meaningful there is already in the per-ticker table
        # below, so re-dumping the whole report here would just be noise.
        json.dumps(
            report.get("summary")
            or {k: v for k, v in report.items() if k not in ("tickers", "underlyings")},
            indent=2, default=str,
        ),
        "```",
        "",
    ]

    underlyings = report.get("underlyings")
    if underlyings:
        lines += ["## Underlyings", ""]
        lines += [
            "| ticker | requested_start | stop_reason | elapsed_s | event_count | distinct_days "
            "| skipped_existing | skipped_no_time | earliest_candle | latest_candle | bars_written "
            "| warnings |",
            "|---|---|---|---:|---:|---:|---:|---:|---|---|---:|---|",
        ]
        for d in underlyings:
            lines.append(
                f"| {d.get('ticker', '')} | {d.get('requested_start', '')} "
                f"| {d.get('stop_reason', '')} | {d.get('elapsed_s', '')} "
                f"| {d.get('event_count', '')} | {d.get('distinct_days_in_result', '')} "
                f"| {d.get('skipped_existing', '')} | {d.get('skipped_no_time_field', '')} "
                f"| {d.get('earliest_candle', '')} | {d.get('latest_candle', '')} "
                f"| {d.get('bars_written', '')} "
                f"| {'; '.join(d.get('warnings', [])) or (d.get('error', '') or '')} |"
            )
            per_day = d.get("candles_per_day")
            if per_day:
                lines.append("")
                lines.append(f"  Candles per day for {d.get('ticker', '')}:")
                for day, count in per_day.items():
                    lines.append(f"  - {day}: {count}")
        lines.append("")

    tickers = report.get("tickers")  # gap-reconcile shape
    if tickers:
        lines += ["## Tickers (gap reconciliation)", ""]
        lines += [
            "| ticker | scan_start | scan_end | gaps_found | bars_written | "
            "requested_start_before_retention | stop_reason | elapsed_s | event_count | distinct_days "
            "| skipped_existing | skipped_no_time | hit_max_count | earliest_candle | latest_candle |",
            "|---|---|---|---:|---:|---|---|---:|---:|---:|---:|---:|---|---|---|",
        ]
        for d in tickers:
            rd = d.get("request_diagnostics") or {}
            lines.append(
                f"| {d.get('ticker', '')} | {d.get('scan_start', '')} | {d.get('scan_end', '')} "
                f"| {d.get('gaps_found', '')} | {d.get('bars_written', '')} "
                f"| {d.get('requested_start_before_retention', '')} "
                f"| {rd.get('stop_reason', '')} | {rd.get('elapsed_s', '')} "
                f"| {rd.get('event_count', '')} | {rd.get('distinct_days_in_result', '')} "
                f"| {rd.get('skipped_existing', '')} | {rd.get('skipped_no_time_field', '')} "
                f"| {rd.get('hit_max_count', '')} "
                f"| {rd.get('earliest_candle', '')} | {rd.get('latest_candle', '')} |"
            )
            preview = d.get("gaps_preview")
            if preview:
                lines.append("")
                lines.append(f"  Gaps found for {d.get('ticker', '')} (up to 10 shown):")
                for g in preview:
                    lines.append(f"  - {g['start']} \u2192 {g['end']} ({g['minutes']} min)")
            per_day = rd.get("candles_per_day")
            if per_day:
                lines.append("")
                # Added specifically to distinguish real dense coverage of
                # an old date from a lone boundary-marker artifact (see
                # PLAN.md Section 7) — a date with 1-2 events is very
                # different from one with hundreds, and distinct_days_in_
                # result alone can't tell those apart.
                lines.append(f"  Candles per day for {d.get('ticker', '')} (density check):")
                for day, count in per_day.items():
                    lines.append(f"  - {day}: {count}")
        lines.append("")

    interesting = report.get("option_contracts_of_interest")
    if interesting:
        lines += [
            f"## Option contracts of interest ({len(interesting)} — failed or hit a truncation "
            "warning; successful/unremarkable contracts aren't listed individually, see Summary "
            "above for aggregate counts)",
            "",
        ]
        for d in interesting:
            lines.append(f"- `{d.get('contract_id', '?')}`")
            if d.get("error"):
                lines.append(f"  - error: {d['error']}")
            for w in d.get("warnings", []):
                lines.append(f"  - warning: {w}")
        lines.append("")

    return "\n".join(lines)


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
    parser.add_argument(
        "--concurrency", type=int, default=8,
        help="How many option contracts' candle requests run concurrently (default 8). Raise "
             "this if a run against many tracked contracts is slow and the bottleneck isn't "
             "the TastyTrade connection itself; lower it if concurrent candle subscriptions "
             "turn out to cause problems in practice.",
    )
    parser.add_argument(
        "--report", nargs="?", const="__auto__", default=None, metavar="PATH_PREFIX",
        help="Write a diagnostic report to PATH_PREFIX.json and PATH_PREFIX.md (e.g. "
             "'--report /tmp/run1' writes /tmp/run1.json and /tmp/run1.md) — per-underlying "
             "request diagnostics (stop reason, timing, earliest/latest candle actually "
             "received) plus any option contract that failed or hit a truncation warning. "
             "Built specifically so troubleshooting a slow/incomplete run doesn't require "
             "pasting a huge raw console log — see PLAN.md Section 7. Bare '--report' with no "
             "path writes to an auto-timestamped path under ./backfill_reports/.",
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

    report_path = args.report
    if report_path == "__auto__":
        kind = "reconcile" if args.reconcile_underlying_gaps else "backfill"
        report_path = f"backfill_reports/{kind}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

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
            contract_concurrency=args.concurrency,
        )
        if args.reconcile_underlying_gaps:
            reconcile_started = datetime.now(timezone.utc)
            window_start = datetime.now(timezone.utc) - timedelta(days=args.lookback_days)
            window_end = datetime.now(timezone.utc)
            ticker_results = []
            for ticker_cfg in settings.tickers:
                if not ticker_cfg.capture_underlying_bars:
                    continue
                result = await job.reconcile_underlying_gaps(
                    ticker_cfg.ticker, window_start, window_end,
                    collect_diagnostics=bool(report_path),
                )
                log.info("Gap reconciliation for %s: %s", ticker_cfg.ticker, result)
                ticker_results.append(result)
            if report_path:
                finished = datetime.now(timezone.utc)
                BackfillJob._write_report({
                    "run_type": "gap_reconcile",
                    "started_at": reconcile_started.isoformat(),
                    "finished_at": finished.isoformat(),
                    "elapsed_s": round((finished - reconcile_started).total_seconds(), 2),
                    "lookback_days": args.lookback_days,
                    "tickers": ticker_results,
                }, report_path)
        else:
            await job.run(report_path=report_path)
    finally:
        await source.close()


if __name__ == "__main__":
    asyncio.run(_main())
