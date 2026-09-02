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
from service.ingestion.bar_aggregator import BarAggregator
from service.ingestion.contract_manager import ContractManager
from service.sources._async_utils import get_attr_any
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)


class IngestionPipeline:
    _MARKET_TZ = ZoneInfo("America/New_York")

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
        self._underlying_tickers: set[str] = {
            t.ticker for t in settings.tickers if t.capture_underlying_bars
        }
        self._closed = False
        # Injectable so tests can control bucket timing deterministically
        # instead of needing to sleep real wall-clock seconds.
        self._clock = clock

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
        diff = await self._contract_manager.refresh()

        # Same callback for both — see module docstring for why this has
        # to be one shared handler rather than two separate subscriptions.
        all_quote_symbols = list(diff.added.keys()) + list(self._underlying_tickers)
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
            try:
                await self._flush_ready_bars()
            except Exception:
                log.exception("Bar flush cycle failed — will retry next interval.")

    async def _flush_ready_bars(self) -> None:
        now = self._clock()
        option_ready = self._option_bars.pop_ready(now)
        underlying_ready = self._underlying_bars.pop_ready(now)
        await self._write_bars(option_ready, underlying_ready)

    async def _write_bars(self, option_ready, underlying_ready) -> None:
        if not option_ready and not underlying_ready:
            return

        async with self._session_factory() as session:
            for contract_id, minute, bucket in option_ready:
                resolved = self._contract_manager.get_resolved(contract_id)
                if resolved is None:
                    log.warning(
                        "Dropping bar for %s @ %s — not (or no longer) a tracked contract.",
                        contract_id, minute,
                    )
                    continue
                session.add(
                    OptionBar1m(
                        time=datetime.fromtimestamp(minute, tz=timezone.utc),
                        contract_id=contract_id,
                        underlying_ticker=resolved.underlying_ticker,
                        expiration_date=resolved.expiration_date,
                        strike=resolved.strike,
                        right=resolved.right,
                        open=bucket.open, high=bucket.high, low=bucket.low, close=bucket.close,
                        bid=bucket.bid, ask=bucket.ask,
                        delta=bucket.delta, gamma=bucket.gamma, theta=bucket.theta,
                        vega=bucket.vega, rho=bucket.rho, iv=bucket.iv,
                        greeks_source=GreeksSource.LIVE if bucket.has_greeks else None,
                    )
                )
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
            for ticker, minute, bucket in underlying_ready:
                session.add(
                    UnderlyingBar1m(
                        time=datetime.fromtimestamp(minute, tz=timezone.utc),
                        ticker=ticker,
                        open=bucket.open, high=bucket.high, low=bucket.low, close=bucket.close,
                    )
                )
            await session.commit()

        if option_ready or underlying_ready:
            log.info(
                "Flushed %d option bar(s), %d underlying bar(s).",
                len(option_ready), len(underlying_ready),
            )

    async def _on_quote(self, event) -> None:
        symbol = get_attr_any(event, "event_symbol")
        if not symbol:
            return
        bid = get_attr_any(event, "bid_price")
        ask = get_attr_any(event, "ask_price")
        now = self._clock()  # see module/bar_aggregator docstring re: why wall-clock
        if symbol in self._underlying_tickers:
            self._underlying_bars.on_quote(symbol, bid, ask, now)
        else:
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
            await self._write_bars(option_ready, underlying_ready)
        except Exception:
            log.exception("Error flushing bars during shutdown.")
