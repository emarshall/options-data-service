"""
Live ingestion pipeline: wires TastyTradeSource + ContractManager +
BarAggregator together into the actual "stream market data -> aggregate
into 1m bars -> write to DB" loop. service/ingestion/main.py runs this for
real.

**Design note — a real interaction with Task 2's "one callback per event
type" rule, discovered while building this:** options and the underlying
both arrive as `Quote` events, but need different handling (options go
into the per-contract bar aggregator with denormalized contract metadata;
the underlying goes into a separate, simpler aggregator keyed by ticker).
Since `subscribe_quotes()` only supports one callback at a time — a second
call replaces it, it doesn't multiplex — both option and underlying
symbols are subscribed through the *same* callback (`_on_quote`), which
dispatches internally based on whether the symbol is a tracked underlying
ticker. This is exactly the "one handler dispatching by event_symbol"
pattern anticipated in Task 2's module docstring, just now with two
different downstream aggregators instead of one.

**Known limitation, deliberate:** live Quote/Greeks events don't carry
volume/open_interest/vwap/bid_volume/ask_volume — those only come from
Candle events (Task 5's backfill). Bars written by this pipeline will have
those columns NULL; only backfilled bars populate them. Revisit only if
that turns out to matter (e.g. by also subscribing to Trade events, not
currently in scope).

**Underlying symbols are resolved, not assumed (added after finding that
live underlying data was mostly absent).** A configured ticker like `SPX`
is a *chain underlying code*, which is not necessarily the symbol the
streaming feed recognizes for that instrument — indices in particular use
a different convention on DXLink. Subscribing with the config ticker
produced no error and (apparently) very little data, which is close to
impossible to diagnose from the outside. The authoritative mapping now
comes from the source's `get_underlying_streamer_symbol()` and is used
for subscribing and for routing incoming events; DB rows are still keyed
by the *config* ticker, since that's what the API and backfill speak in.
The two maps are kept separate in `_underlying_streamer_symbols` and
`_streamer_to_ticker` for exactly that reason.

**Write failures must not lose data (three related fixes, same root
theme).** Buckets are popped out of memory *before* being written, so a
failed write used to discard them permanently — nothing re-creates a
bucket for a minute that has already elapsed, so the only recovery was a
manual backfill, and the log message claiming the next cycle would retry
was simply untrue. `BarAggregator.restore()` now puts them back. Related:
option and underlying bars are written in *separate* transactions, so a
failure in the high-volume option path can't roll back the handful of
underlying bars riding along with it; and both writes are
conflict-tolerant, so an overlapping `backfill` run writing the same
`(time, identifier)` row no longer turns a live flush into an
`IntegrityError`. See `service/db/upsert.py`.

**Contract refresh scheduling (Task 9):** the refresh loop uses two
cadences, not one fixed interval — a faster one during a configurable
window around market open (`settings.contract_refresh_fast_window_start`/
`_end`, America/New_York, Mon-Fri only), and the normal cadence otherwise.
This is what gets a same-day (0DTE) listing subscribed to promptly instead
of waiting up to a full normal-cadence interval after it first appears on
the chain, without needing to poll the fast cadence all day. AM-settlement
exclusion itself needed no new work here — `ContractManager` already
filters using `settlement_type` sourced from the instruments-API option
chain (`get_option_chain()`), not any DXLink streaming event, which is
exactly what Task 9 called for; see `contract_manager.py`'s module
docstring.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from service.config.settings import AppConfig
from service.db.models import GreeksSource, OptionBar1m, UnderlyingBar1m
from service.db.upsert import insert_ignore
from service.ingestion.bar_aggregator import BarAggregator
from service.ingestion.contract_manager import ContractManager
from service.sources._async_utils import get_attr_any
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)


class IngestionPipeline:
    _MARKET_TZ = ZoneInfo("America/New_York")

    # How often to report quote-event activity. Long enough to be a
    # low-volume trickle of summary lines, short enough to make an obvious
    # "this symbol is delivering nothing" problem visible within a
    # trading session rather than only at the end of one.
    _COUNTER_LOG_INTERVAL_S = 300.0

    def __init__(
        self,
        source: MarketDataSource,
        session_factory,
        settings: AppConfig,
        contract_refresh_interval_s: float | None = None,
        contract_refresh_fast_interval_s: float | None = None,
        flush_interval_s: float = 5.0,
        clock=lambda: datetime.now(timezone.utc),
    ):
        self._source = source
        self._session_factory = session_factory
        self._settings = settings
        self._contract_manager = ContractManager(source, session_factory, settings.tickers)
        self._option_bars = BarAggregator()
        self._underlying_bars = BarAggregator()
        # Explicit constructor args win (mainly for tests); otherwise pull
        # from settings, which is where real config (env-var overridable)
        # lives as of Task 9 — see settings.py for defaults/rationale.
        self._contract_refresh_interval_s = (
            contract_refresh_interval_s
            if contract_refresh_interval_s is not None
            else settings.contract_refresh_interval_s
        )
        self._contract_refresh_fast_interval_s = (
            contract_refresh_fast_interval_s
            if contract_refresh_fast_interval_s is not None
            else settings.contract_refresh_fast_interval_s
        )
        self._fast_window_start = self._parse_hhmm(settings.contract_refresh_fast_window_start)
        self._fast_window_end = self._parse_hhmm(settings.contract_refresh_fast_window_end)
        self._flush_interval_s = flush_interval_s
        # Injectable so tests can control bucket timing deterministically
        # instead of needing to sleep real wall-clock seconds. Assigned
        # before the counters below, which read it to establish a baseline.
        self._clock = clock
        self._underlying_tickers: set[str] = {
            t.ticker for t in settings.tickers if t.capture_underlying_bars
        }
        # Config ticker -> the symbol the feed actually streams that
        # instrument under, resolved once at startup (see
        # _resolve_underlying_symbols). Deliberately two separate maps:
        # the *feed* symbol is what we subscribe to and what comes back on
        # each event, while the *config* ticker is what we key DB rows by
        # and what the rest of the app (API filters, backfill) speaks in.
        # They are not the same string for every instrument — indices
        # especially — and conflating them is what made live underlying
        # bars quietly not arrive.
        self._underlying_streamer_symbols: dict[str, str] = {}
        self._streamer_to_ticker: dict[str, str] = {}
        self._underlying_symbols_resolved = False
        # Event counters, purely for observability — see
        # _log_ingestion_counters(). The "why is there no underlying data"
        # question is otherwise very hard to answer from the outside,
        # because a symbol that silently streams nothing looks exactly
        # like a symbol that streams rarely.
        self._quote_events = {"underlying": 0, "option": 0}
        self._counters_logged_at = self._clock()
        self._closed = False

    async def _resolve_underlying_symbols(self) -> None:
        """Looks up the feed-native symbol for each configured underlying
        ticker. Runs once, before the first subscribe; a failure to resolve
        one ticker never blocks startup (the source falls back to the
        configured ticker)."""
        if self._underlying_symbols_resolved:
            return
        self._underlying_symbols_resolved = True
        for ticker in sorted(self._underlying_tickers):
            try:
                streamer_symbol = await self._source.get_underlying_streamer_symbol(ticker)
            except Exception:
                log.exception(
                    "Failed to resolve the feed symbol for underlying %r — using the "
                    "configured ticker.", ticker,
                )
                streamer_symbol = ticker
            self._underlying_streamer_symbols[ticker] = streamer_symbol
            self._streamer_to_ticker[streamer_symbol] = ticker

    @staticmethod
    def _parse_hhmm(value: str) -> time:
        hh, mm = value.split(":")
        return time(int(hh), int(mm))

    def _current_refresh_interval_s(self) -> float:
        """Fast cadence during the configured pre/post-open window on a
        weekday, normal cadence otherwise (including all weekends — see
        module docstring)."""
        now_et = self._clock().astimezone(self._MARKET_TZ)
        if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
            return self._contract_refresh_interval_s
        if self._fast_window_start <= now_et.time() <= self._fast_window_end:
            return self._contract_refresh_fast_interval_s
        return self._contract_refresh_interval_s

    async def run(self) -> None:
        """Runs forever (until close()): initial contract resolution +
        subscription, then concurrently refreshes contracts periodically
        and flushes completed bars periodically."""
        await self._refresh_and_subscribe()
        await asyncio.gather(self._refresh_loop(), self._flush_loop())

    async def _refresh_and_subscribe(self) -> None:
        await self._resolve_underlying_symbols()
        diff = await self._contract_manager.refresh()

        # Same callback for both — see module docstring for why this has
        # to be one shared handler rather than two separate subscriptions.
        # Underlyings go out under their *feed* symbols, not their config
        # tickers — see _resolve_underlying_symbols.
        all_quote_symbols = list(diff.added.keys()) + list(self._underlying_streamer_symbols.values())
        if all_quote_symbols:
            await self._source.subscribe_quotes(all_quote_symbols, self._on_quote)
        if diff.added:
            await self._source.subscribe_greeks(list(diff.added.keys()), self._on_option_greeks)
            log.info("Subscribed to %d new option contract(s).", len(diff.added))
        if diff.removed:
            await self._source.unsubscribe(list(diff.removed))
            log.info("Unsubscribed from %d expired/removed contract(s).", len(diff.removed))

    async def _refresh_loop(self) -> None:
        while not self._closed:
            interval = self._current_refresh_interval_s()
            await asyncio.sleep(interval)
            if self._closed:
                return
            try:
                await self._refresh_and_subscribe()
            except Exception:
                log.exception("Contract refresh cycle failed — will retry next interval.")

    async def _flush_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self._flush_interval_s)
            self._log_ingestion_counters()
            try:
                await self._flush_ready_bars()
            except Exception:
                log.exception("Bar flush cycle failed — will retry next interval.")

    async def _flush_ready_bars(self) -> None:
        now = self._clock()
        option_ready = self._option_bars.pop_ready(now)
        underlying_ready = self._underlying_bars.pop_ready(now)
        # Options and underlyings are written in separate transactions and
        # fail independently. They used to share one session and one
        # commit, which meant any failure in the option path — by far the
        # larger, higher-churn one, hundreds of contracts' worth of rows —
        # rolled back the handful of underlying bars in the same batch
        # too. Underlying data was collateral damage to a problem that had
        # nothing to do with it, and it was the underlying data that
        # actually went missing.
        await self._write_option_bars(option_ready)
        await self._write_underlying_bars(underlying_ready)

    async def _write_option_bars(self, option_ready) -> None:
        if not option_ready:
            return
        rows = []
        for contract_id, minute, bucket in option_ready:
            resolved = self._contract_manager.get_resolved(contract_id)
            if resolved is None:
                log.warning(
                    "Dropping bar for %s @ %s — not (or no longer) a tracked contract.",
                    contract_id, minute,
                )
                continue
            rows.append({
                "time": datetime.fromtimestamp(minute, tz=timezone.utc),
                "contract_id": contract_id,
                "underlying_ticker": resolved.underlying_ticker,
                "expiration_date": resolved.expiration_date,
                "strike": resolved.strike,
                "right": resolved.right,
                "open": bucket.open, "high": bucket.high, "low": bucket.low, "close": bucket.close,
                "bid": bucket.bid, "ask": bucket.ask,
                "delta": bucket.delta, "gamma": bucket.gamma, "theta": bucket.theta,
                "vega": bucket.vega, "rho": bucket.rho, "iv": bucket.iv,
                "greeks_source": GreeksSource.LIVE if bucket.has_greeks else None,
            })
            # DEBUG, not INFO — intentionally opt-in (LOG_LEVEL=DEBUG), one
            # line per bar with just the fields useful for spot-checking
            # ingestion (contract identity, delta, price), not the raw
            # event payload. See PLAN.md Section 7 for the *actual* source
            # of the "way too much logging" problem this was originally
            # meant to address — it wasn't application-level logging at
            # all, it was the `tastytrade` package's own internal debug
            # logging of every raw websocket message, now silenced in
            # service/logging_config.py. This per-bar line is a genuinely
            # useful opt-in on top of that fix, not a replacement for it.
            if log.isEnabledFor(logging.DEBUG):
                log.debug(
                    "bar %s %s %s exp=%s strike=%s delta=%s close=%s",
                    resolved.underlying_ticker, contract_id, resolved.right,
                    resolved.expiration_date, resolved.strike, bucket.delta, bucket.close,
                )
        try:
            async with self._session_factory() as session:
                # Conflict-tolerant: a (time, contract_id) row may already
                # exist if backfill overlapped this flush, or if a previous
                # flush attempt failed and is being retried. See
                # service/db/upsert.py.
                await insert_ignore(session, OptionBar1m.__table__, rows)
                await session.commit()
        except Exception:
            # Put the buckets back so the next flush retries them. Without
            # this, a failed write silently and permanently discards every
            # bar it had already popped out of memory — nothing re-creates
            # a bucket for a minute that has already passed, so the data
            # would be gone for good, recoverable only by a manual
            # backfill. The old log message claimed the next interval would
            # retry, which was never true.
            self._option_bars.restore(option_ready)
            log.exception(
                "Failed to write %d option bar(s) — returned to the aggregator and will be "
                "retried on the next flush cycle.", len(option_ready),
            )
            return
        log.info("Flushed %d option bar(s).", len(option_ready))

    async def _write_underlying_bars(self, underlying_ready) -> None:
        if not underlying_ready:
            return
        rows = [
            {
                "time": datetime.fromtimestamp(minute, tz=timezone.utc),
                "ticker": ticker,
                "open": bucket.open, "high": bucket.high, "low": bucket.low, "close": bucket.close,
            }
            for ticker, minute, bucket in underlying_ready
        ]
        try:
            async with self._session_factory() as session:
                # Conflict-tolerant for the same reason as option bars:
                # backfill writes these same (time, ticker) rows, and an
                # overlapping run must not turn this flush into a failure.
                await insert_ignore(session, UnderlyingBar1m.__table__, rows)
                await session.commit()
        except Exception:
            self._underlying_bars.restore(underlying_ready)
            log.exception(
                "Failed to write %d underlying bar(s) — returned to the aggregator and will be "
                "retried on the next flush cycle.", len(underlying_ready),
            )
            return
        log.info("Flushed %d underlying bar(s).", len(underlying_ready))

    def _log_ingestion_counters(self) -> None:
        """Periodic, cheap visibility into whether the feed is actually
        delivering anything, split by symbol class.

        Added because a subscription that resolves to a symbol the feed
        doesn't recognize produces no error and no events — from the
        outside that's indistinguishable from a market simply being quiet,
        and it's the single hardest failure in this pipeline to diagnose
        from data alone. A persistent underlying count of zero here is a
        direct, unambiguous signal that the underlying subscription is
        wrong, long before anyone notices missing rows in the database.
        """
        now = self._clock()
        if (now - self._counters_logged_at).total_seconds() < self._COUNTER_LOG_INTERVAL_S:
            return
        self._counters_logged_at = now
        pending = self._option_bars.pending_count() + self._underlying_bars.pending_count()
        log.info(
            "Ingestion activity over the last ~%ds: %d underlying quote event(s), "
            "%d option quote event(s); %d bar(s) awaiting flush.",
            self._COUNTER_LOG_INTERVAL_S,
            self._quote_events["underlying"], self._quote_events["option"], pending,
        )
        self._quote_events["underlying"] = 0
        self._quote_events["option"] = 0

    async def _on_quote(self, event) -> None:
        symbol = get_attr_any(event, "event_symbol")
        if not symbol:
            return
        bid = get_attr_any(event, "bid_price")
        ask = get_attr_any(event, "ask_price")
        now = self._clock()  # see module/bar_aggregator docstring re: why wall-clock
        # Route on the *feed* symbol, mapping back to the configured ticker
        # — the two differ for some instruments (see
        # _resolve_underlying_symbols).
        ticker = self._streamer_to_ticker.get(symbol)
        if ticker is not None:
            self._quote_events["underlying"] += 1
            self._underlying_bars.on_quote(ticker, bid, ask, now)
        else:
            self._quote_events["option"] += 1
            # Anything else is assumed to be a tracked option contract. If
            # it isn't (e.g. a stale event for something just unsubscribed),
            # the bucket just gets dropped at flush time — no matching
            # ResolvedContract, logged and skipped, not an error.
            self._option_bars.on_quote(symbol, bid, ask, now)

    async def _on_option_greeks(self, event) -> None:
        symbol = get_attr_any(event, "event_symbol")
        if not symbol:
            return
        self._option_bars.on_greeks(
            symbol,
            get_attr_any(event, "delta"),
            get_attr_any(event, "gamma"),
            get_attr_any(event, "theta"),
            get_attr_any(event, "vega"),
            get_attr_any(event, "rho"),
            get_attr_any(event, "volatility"),  # IV is called `volatility` on the Greeks event
            self._clock(),
        )

    async def close(self) -> None:
        self._closed = True
        # Flush whatever's currently bucketed, even if its minute hasn't
        # technically elapsed yet — better to persist partial-minute data
        # on shutdown than silently lose it.
        try:
            far_future = datetime.fromtimestamp(2**31 - 1, tz=timezone.utc)
            option_ready = self._option_bars.pop_ready(far_future)
            underlying_ready = self._underlying_bars.pop_ready(far_future)
            await self._write_option_bars(option_ready)
            await self._write_underlying_bars(underlying_ready)
        except Exception:
            log.exception("Error flushing bars during shutdown.")
