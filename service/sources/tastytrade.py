"""
TastyTrade implementation of MarketDataSource, using DXLink for streaming.

Design notes:

- **Auth is OAuth2-only** (client_secret + refresh_token) — TastyTrade
  discontinued username/password session auth Dec 1, 2025. See PLAN.md
  Section 3.
- **One callback per event type, not per symbol.** `subscribe_quotes()` and
  `subscribe_greeks()` register a single callback each; calling either
  again replaces the previous callback (symbols accumulate, the callback
  doesn't multiplex). This matches how Task 4's ingestion pipeline actually
  uses this: one handler for all quote events, one for all Greeks events,
  each dispatching internally by the event's own `event_symbol` field as
  needed. If a future caller genuinely needs per-symbol callback routing,
  that's a deliberate simplification to revisit then, not an oversight.
- **Candle requests are one-off, not persistent.** `request_candles()`
  subscribes, collects whatever comes back, and unsubscribes within the
  same call — unlike quotes/Greeks, which stay subscribed indefinitely.
  `unsubscribe()` only affects the persistent quote/Greeks subscriptions.
- **Reconnection is automatic** with capped exponential backoff, and
  resubscribes every currently-tracked symbol on reconnect. Both listener
  loops (quotes, Greeks) share one reconnect path guarded by a lock, so a
  single dropped connection doesn't trigger two redundant reconnect
  attempts.
- Every hang-proofing pattern used here (`with_timeout`, `collect_events`,
  the defensive `subscribe_candle` signature detection) was validated
  against a real TastyTrade connection during Task 0 — see
  service/sources/_async_utils.py and PLAN.md Section 7 for the bugs that
  shaped this.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import date, datetime
from typing import Any, Awaitable, Callable

from service.sources._async_utils import chunked, collect_events, get_attr_any, with_timeout
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)

# Capped exponential-ish backoff schedule for reconnect attempts (seconds).
# Repeats the last value indefinitely if more retries are needed — this is
# a long-lived, unattended service, so we never just give up.
_RECONNECT_BACKOFF_S = [1, 2, 5, 10, 20, 30, 60]

# Conservative batch size for subscribe/unsubscribe calls — see
# _async_utils.chunked()'s docstring. A single subscribe call with too
# many symbols in one message can exceed DXLink's ~64KB max frame size and
# get the connection reset by the server (see PLAN.md Section 7).
_SUBSCRIBE_BATCH_SIZE = 200


class TastyTradeSource(MarketDataSource):
    def __init__(self, client_secret: str, refresh_token: str, use_sandbox: bool = False):
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._use_sandbox = use_sandbox

        self._session: Any = None  # tastytrade.Session
        self._streamer: Any = None  # tastytrade.DXLinkStreamer, manually entered/exited
        self._streamer_lock = asyncio.Lock()
        self._reconnect_lock = asyncio.Lock()
        self._closed = False

        # Tracked subscriptions, so we can resubscribe everything after a
        # reconnect without the caller needing to do anything.
        self._quote_symbols: set[str] = set()
        self._greeks_symbols: set[str] = set()
        self._quote_callback: Callable[[Any], Awaitable[None]] | None = None
        self._greeks_callback: Callable[[Any], Awaitable[None]] | None = None

        self._quote_listener_task: asyncio.Task | None = None
        self._greeks_listener_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    async def authenticate(self) -> None:
        """Creates (or re-creates) the TastyTrade session.

        The SDK's Session constructor performs a blocking network call to
        validate/exchange the refresh token — confirmed during Task 0's
        debugging, where a bad refresh token raised synchronously right
        here (a TastytradeError, not a connection-refused or similar) — so
        this runs it in a thread rather than blocking the event loop
        directly, which would otherwise stall any other concurrent work
        (like websocket keepalives) for the duration of that HTTP call.
        """
        from tastytrade import Session

        self._session = await asyncio.to_thread(
            Session, self._client_secret, self._refresh_token, is_test=self._use_sandbox
        )
        log.info("TastyTrade session established (sandbox=%s).", self._use_sandbox)

    def _require_session(self):
        if self._session is None:
            raise RuntimeError("authenticate() must be called before using TastyTradeSource.")
        return self._session

    # ------------------------------------------------------------------
    # Option chain lookup
    # ------------------------------------------------------------------

    async def get_option_chain(self, ticker: str) -> dict[date, list[Any]]:
        from tastytrade.instruments import get_option_chain as _get_option_chain

        session = self._require_session()
        result = _get_option_chain(session, ticker)
        # Some SDK versions return this synchronously, some as a coroutine —
        # same defensive handling validated in Task 0's spike script, kept
        # here since the same version uncertainty applies.
        chain = await result if inspect.isawaitable(result) else result
        return chain

    # ------------------------------------------------------------------
    # Streamer connection management
    # ------------------------------------------------------------------

    async def _ensure_streamer(self):
        """Returns the current streamer, connecting if necessary. Safe to
        call concurrently — e.g. from both listener loops on startup."""
        async with self._streamer_lock:
            if self._streamer is None:
                from tastytrade import DXLinkStreamer

                session = self._require_session()
                self._streamer = await DXLinkStreamer(session).__aenter__()
                log.info("DXLink streamer connected.")
            return self._streamer

    async def _mark_disconnected(self) -> None:
        """Called by a listener loop when it detects the connection died.
        Safe to call more than once (e.g. both listener loops noticing the
        same drop) — the second call is just a no-op."""
        old_streamer = self._streamer
        self._streamer = None
        if old_streamer is not None:
            try:
                await old_streamer.__aexit__(None, None, None)
            except Exception:
                pass  # already dead; nothing more we can do with it

    async def _subscribe_batched(self, streamer, event_class, symbols, timeout_s: float = 10.0) -> None:
        """Subscribes in batches to stay under DXLink's ~64KB max frame
        size — see _async_utils.chunked()'s docstring and PLAN.md Section 7
        for the incident this fixes (a single subscribe call with too many
        symbols got the connection reset by the server)."""
        for batch in chunked(symbols, _SUBSCRIBE_BATCH_SIZE):
            await with_timeout(
                streamer.subscribe(event_class, batch), timeout_s,
                f"subscribe {event_class.__name__} (batch of {len(batch)})",
            )

    async def _unsubscribe_batched(self, streamer, event_class, symbols, timeout_s: float = 10.0) -> None:
        for batch in chunked(symbols, _SUBSCRIBE_BATCH_SIZE):
            await with_timeout(
                streamer.unsubscribe(event_class, batch), timeout_s,
                f"unsubscribe {event_class.__name__} (batch of {len(batch)})",
            )

    async def _reconnect(self) -> None:
        """Tears down and re-establishes the connection, resubscribing
        every tracked symbol. Guarded so that if both listener loops notice
        the same disconnect at once, only one of them actually does the
        work — the other just waits for the lock and finds nothing left
        to do."""
        async with self._reconnect_lock:
            if self._streamer is not None:
                return  # someone else already fixed it while we waited

            attempt = 0
            while not self._closed:
                attempt += 1
                if attempt > 1:
                    delay = _RECONNECT_BACKOFF_S[min(attempt - 2, len(_RECONNECT_BACKOFF_S) - 1)]
                    await asyncio.sleep(delay)
                try:
                    await self.authenticate()  # re-auth too, in case that's what went stale
                    await self._ensure_streamer()
                    await self._resubscribe_all()
                    log.info("Reconnected successfully (attempt %d).", attempt)
                    return
                except Exception as e:
                    log.warning("Reconnect attempt %d failed: %s", attempt, e)

    async def _resubscribe_all(self) -> None:
        streamer = self._streamer
        if streamer is None:
            return
        from tastytrade.dxfeed import Greeks, Quote

        if self._quote_symbols:
            await self._subscribe_batched(streamer, Quote, list(self._quote_symbols))
            log.info("Resubscribed %d quote symbol(s).", len(self._quote_symbols))
        if self._greeks_symbols:
            await self._subscribe_batched(streamer, Greeks, list(self._greeks_symbols))
            log.info("Resubscribed %d greeks symbol(s).", len(self._greeks_symbols))

    # ------------------------------------------------------------------
    # Quote / Greeks subscriptions (persistent, callback-driven)
    # ------------------------------------------------------------------

    async def subscribe_quotes(
        self, symbols: list[str], callback: Callable[[Any], Awaitable[None]]
    ) -> None:
        from tastytrade.dxfeed import Quote

        new_symbols = set(symbols) - self._quote_symbols
        self._quote_symbols |= set(symbols)
        self._quote_callback = callback  # replaces any previous — see module docstring

        streamer = await self._ensure_streamer()
        if new_symbols:
            await self._subscribe_batched(streamer, Quote, list(new_symbols))

        if self._quote_listener_task is None or self._quote_listener_task.done():
            self._quote_listener_task = asyncio.ensure_future(
                self._listen_loop(Quote, lambda: self._quote_callback)
            )

    async def subscribe_greeks(
        self, symbols: list[str], callback: Callable[[Any], Awaitable[None]]
    ) -> None:
        from tastytrade.dxfeed import Greeks

        new_symbols = set(symbols) - self._greeks_symbols
        self._greeks_symbols |= set(symbols)
        self._greeks_callback = callback

        streamer = await self._ensure_streamer()
        if new_symbols:
            await self._subscribe_batched(streamer, Greeks, list(new_symbols))

        if self._greeks_listener_task is None or self._greeks_listener_task.done():
            self._greeks_listener_task = asyncio.ensure_future(
                self._listen_loop(Greeks, lambda: self._greeks_callback)
            )

    async def snapshot_greeks(self, symbols: list[str], timeout_s: float = 15.0) -> dict[str, Any]:
        """One-off Greeks read for `symbols` — subscribes, collects
        whatever arrives within `timeout_s`, then unsubscribes. Deliberately
        separate from subscribe_greeks()'s persistent callback slot: this
        method must not touch `self._greeks_callback` at all, or it would
        clobber whatever Task 4's ingestion pipeline has registered there.
        Always waits close to the full `timeout_s` (doesn't try to bail out
        the instant every symbol has one event in) — that's a deliberate
        simplification to avoid re-using a single listen() async generator
        across multiple collect_events() calls, which risks leaving it
        unusable if a prior call's internal cancellation caught it
        mid-iteration.
        """
        from tastytrade.dxfeed import Greeks

        streamer = await self._ensure_streamer()
        symbols_set = set(symbols)

        await self._subscribe_batched(streamer, Greeks, symbols)
        try:
            events = await collect_events(
                streamer.listen(Greeks),
                timeout_s=timeout_s,
                max_count=len(symbols) * 3,
                event_filter=lambda ev: get_attr_any(ev, "event_symbol") in symbols_set,
                idle_timeout_s=3.0,
            )
        finally:
            try:
                await self._unsubscribe_batched(streamer, Greeks, symbols)
            except Exception as e:
                log.warning("snapshot_greeks unsubscribe didn't complete cleanly: %s", e)

        result: dict[str, Any] = {}
        for ev in events:
            sym = get_attr_any(ev, "event_symbol")
            if sym:
                result[sym] = ev  # last event wins if more than one arrived
        return result

    async def _listen_loop(self, event_type, get_callback: Callable[[], Callable | None]) -> None:
        """Runs for the lifetime of the source, redispatching events of
        `event_type` to whatever callback is currently registered.
        `get_callback` is a lookup function (not a captured value) so
        re-subscribing with a new callback takes effect immediately without
        needing to restart this loop. Reconnects automatically on
        disconnect via `_reconnect()`."""
        while not self._closed:
            try:
                streamer = await self._ensure_streamer()
                async for event in streamer.listen(event_type):
                    cb = get_callback()
                    if cb is None:
                        continue
                    try:
                        await cb(event)
                    except Exception:
                        log.exception(
                            "Error in %s callback — continuing (bad event handled, not "
                            "propagated, so one bad callback invocation doesn't kill the "
                            "whole listener loop).",
                            event_type.__name__,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._closed:
                    return
                log.warning(
                    "%s listener disconnected (%s) — reconnecting...", event_type.__name__, e
                )
                await self._mark_disconnected()
                await self._reconnect()

    # ------------------------------------------------------------------
    # Candles (one-off historical pulls, not persistent subscriptions)
    # ------------------------------------------------------------------

    async def request_candles(
        self, symbol: str, period: str, start_time: datetime,
        timeout_s: float = 240.0, max_count: int = 100_000,
    ) -> list[Any]:
        """One-off historical candle pull. Per Task 0 findings: 1-minute
        option candle depth is capped at ~6 weeks regardless of how far
        back `start_time` requests, but requesting further back than that
        is harmless (just returns whatever's actually available).

        **`timeout_s`/`max_count` bug fix (found investigating Task 9's
        underlying-backfill issue — see PLAN.md Section 7):** the original
        defaults here (30s / 5000 events) were sized around a single
        option contract's candle history, which is naturally sparse (an
        option often has no quote activity in a given minute). An
        underlying index/ETF ticker, by contrast, is continuously quoted
        essentially every market minute — a 60-day lookback is up to
        ~60 * 390 ≈ 23,400 1-minute candles, several times the old 5000
        cap, and potentially takes longer than 30s to fully stream over
        the shared websocket. Whichever of the two limits was hit first
        silently truncated the result to whatever had arrived so far,
        with no error — indistinguishable from "that's just all the data
        there is" from the caller's side. Both bumped well above any
        realistic single-symbol candle count for the lookback windows this
        service actually uses (`idle_timeout_s` below is still what ends a
        typical, sparse, option-contract call quickly — these are just the
        safety-net ceiling for the dense case, not the common-case
        stopping condition).
        """
        from tastytrade.dxfeed import Candle

        streamer = await self._ensure_streamer()

        subscribed_via = await self._subscribe_candle_compat(streamer, Candle, symbol, period, start_time)
        try:
            events = await collect_events(
                streamer.listen(Candle),
                timeout_s=timeout_s,
                max_count=max_count,  # safety net: an in-progress bar can otherwise stream forever
                event_filter=lambda ev: symbol in str(get_attr_any(ev, "event_symbol", default="")),
                idle_timeout_s=3.0,  # the real stopping condition in practice — see collect_events docstring
            )
        finally:
            try:
                await self._unsubscribe_candle_compat(streamer, Candle, symbol, period, subscribed_via)
            except Exception as e:
                log.warning("Candle unsubscribe for %s didn't complete cleanly: %s", symbol, e)

        if len(events) >= max_count:
            log.warning(
                "request_candles(%s) hit max_count=%d — result may be truncated. "
                "If this symbol legitimately has more history than that, raise max_count.",
                symbol, max_count,
            )

        return events

    async def _subscribe_candle_compat(self, streamer, Candle, symbol: str, period: str, start_time: datetime) -> str:
        """Same defensive signature-detection as Task 0's spike script —
        the `tastytrade` package's subscribe_candle() signature has shifted
        across versions, so this tries the dedicated method first and falls
        back to the raw symbol-suffix approach if it doesn't match. Returns
        which path was taken, so _unsubscribe_candle_compat can mirror it
        exactly — see that method's docstring for why that match matters."""
        sub_symbol = f"{symbol}{{={period}}}"
        try:
            sig = inspect.signature(streamer.subscribe_candle)
            params = sig.parameters
            symbol_param = next((p for p in ("symbols", "symbol") if p in params), None)
            candle_type_param = next((p for p in ("interval", "candle_type", "period") if p in params), None)
            start_param = next((p for p in ("start_time", "from_time") if p in params), None)
            if symbol_param and candle_type_param and start_param:
                call_args = {symbol_param: [symbol], candle_type_param: period, start_param: start_time}
                await with_timeout(
                    streamer.subscribe_candle(**call_args), 10.0, f"subscribe_candle {symbol}"
                )
                return "subscribe_candle"
            raise TypeError(f"subscribe_candle signature not recognized: {sig}")
        except Exception as e:
            log.info("subscribe_candle() unavailable/failed (%s) — using fallback subscribe().", e)
            await with_timeout(streamer.subscribe(Candle, [sub_symbol]), 10.0, f"subscribe {symbol}")
            return "subscribe_fallback"

    async def _unsubscribe_candle_compat(
        self, streamer, Candle, symbol: str, period: str, subscribed_via: str
    ) -> None:
        """Mirrors whichever path _subscribe_candle_compat actually took —
        this is not optional symmetry, it's required for correctness.

        Real bug found running the backfill job for several minutes (see
        PLAN.md Section 7): `subscribe_candle()` and the generic
        `subscribe()` construct genuinely DIFFERENT wire-level symbol
        strings for what looks like the same logical request — confirmed
        by reading the installed `tastytrade` SDK's actual source.
        `subscribe_candle()` appends a `,tho=true` suffix by default that
        a manually-constructed `f"{symbol}{{={period}}}"` string doesn't
        include. Calling the generic `unsubscribe()` with that
        non-matching string after subscribing via `subscribe_candle()`
        sends a `remove` the server can't match to anything — it silently
        does nothing, the real subscription never gets removed, and it
        leaks. Across a long backfill run processing hundreds of
        contracts sequentially, those leaked subscriptions accumulate
        until the server refuses further changes with a "subscription
        size ... too big" error. The SDK ships a dedicated
        `unsubscribe_candle()` specifically to avoid this — its own
        docstring says as much ("For candles, use unsubscribe_candle
        instead") — so this uses that whenever `subscribe_candle` was the
        path taken, rather than trying to replicate the SDK's internal
        symbol formatting by hand (which is exactly the kind of detail
        that drifts across SDK versions, per repeated experience on this
        project).
        """
        if subscribed_via == "subscribe_candle" and hasattr(streamer, "unsubscribe_candle"):
            await with_timeout(
                streamer.unsubscribe_candle(symbol, period), 10.0, f"unsubscribe_candle {symbol}"
            )
            return
        if subscribed_via == "subscribe_candle":
            log.warning(
                "subscribe_candle() was used but this streamer has no unsubscribe_candle() — "
                "falling back to generic unsubscribe(), which may not actually clear the "
                "subscription server-side. Check the installed tastytrade SDK version."
            )
        sub_symbol = f"{symbol}{{={period}}}"
        await with_timeout(streamer.unsubscribe(Candle, [sub_symbol]), 10.0, f"unsubscribe candle {symbol}")

    # ------------------------------------------------------------------
    # Unsubscribe / shutdown
    # ------------------------------------------------------------------

    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribes `symbols` from the persistent Quote/Greeks
        subscriptions. Candle subscriptions are self-contained per-call in
        request_candles() and aren't tracked here — nothing to do for
        those."""
        symbols_set = set(symbols)
        streamer = self._streamer
        if streamer is None:
            self._quote_symbols -= symbols_set
            self._greeks_symbols -= symbols_set
            return

        from tastytrade.dxfeed import Greeks, Quote

        to_unsub_quotes = symbols_set & self._quote_symbols
        to_unsub_greeks = symbols_set & self._greeks_symbols
        if to_unsub_quotes:
            try:
                await self._unsubscribe_batched(streamer, Quote, list(to_unsub_quotes))
            except Exception as e:
                log.warning("Quote unsubscribe failed for %s: %s", to_unsub_quotes, e)
        if to_unsub_greeks:
            try:
                await self._unsubscribe_batched(streamer, Greeks, list(to_unsub_greeks))
            except Exception as e:
                log.warning("Greeks unsubscribe failed for %s: %s", to_unsub_greeks, e)

        self._quote_symbols -= symbols_set
        self._greeks_symbols -= symbols_set

    async def close(self) -> None:
        self._closed = True
        for task in (self._quote_listener_task, self._greeks_listener_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        if self._streamer is not None:
            try:
                await self._streamer.__aexit__(None, None, None)
            except Exception:
                pass
            self._streamer = None
        log.info("TastyTradeSource closed.")
