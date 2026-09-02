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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from service.sources._async_utils import chunked, collect_events, get_attr_any, with_timeout
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)


def _candle_event_time(ev: Any) -> datetime | None:
    """Extracts a Candle event's own `time` (ms since epoch) as a UTC
    datetime. Currently unused by `request_candles` itself — it was
    written for a "stop once caught up to live" optimization that was
    added and then reverted after causing a real regression (see
    PLAN.md Section 7, and `request_candles`'s own docstring for the full
    story: that optimization assumed oldest-first delivery order, which
    isn't confirmed and looks likely wrong). Left in place, small and
    independently tested, in case delivery order gets confirmed later and
    a corrected version of that optimization becomes worth re-adding."""
    ms = get_attr_any(ev, "time")
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


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
        timeout_s: float = 240.0, max_count: int = 100_000, idle_timeout_s: float = 3.0,
        diagnostics: dict | None = None,
    ) -> list[Any]:
        """One-off historical candle pull. Per Task 0 findings: 1-minute
        option candle depth is capped at ~6 weeks regardless of how far
        back `start_time` requests, but requesting further back than that
        is harmless (just returns whatever's actually available).

        `diagnostics`, if given a dict, gets populated with everything
        needed to troubleshoot a slow/incomplete result without needing
        the raw candle list or verbose logs — `stop_reason`, `elapsed_s`,
        `event_count` (from `collect_events`), plus request context
        (`symbol`, `requested_start`, the effective `timeout_s`/
        `idle_timeout_s`/`max_count`) and result shape (`earliest_candle`,
        `latest_candle`, `hit_max_count`). Built for `BackfillJob`'s
        diagnostic report — see PLAN.md Section 7.

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
        service actually uses.

        **`idle_timeout_s` is also caller-tunable now, for the same
        underlying reason (found investigating a follow-up report: gap
        reconciliation over a multi-week span for a dense underlying still
        wasn't recovering all of it, even after the fix above).**
        `collect_events`'s idle-timeout mechanism stops the collection as
        soon as no new *matching* event has arrived for `idle_timeout_s`
        seconds — a good, important optimization for a typically-sparse
        option contract (see that function's docstring), but a real risk
        for a large historical replay: a multi-week pull for a
        continuously-quoted underlying is tens of thousands of events, and
        it's plausible the server delivers that in internally-batched
        bursts with an occasional pause between them exceeding a few
        seconds — which this function would previously (fixed 3.0s idle
        timeout) misread as "no more data," ending the collection early
        with a *partial* result and no error, functionally identical to
        the original bug this same investigation already fixed once.

        **Confirmed against a real connection** (via
        `scripts/underlying_retention_probe.py` — see PLAN.md Section 7):
        for a continuously-quoted underlying, the *entire* historical
        replay is apparently delivered slowly/throttled — individual
        events keep trickling in with gaps well under `idle_timeout_s`
        the whole time, so `idle_timeout_s` never actually triggers, and
        every single call was running for the *full* `timeout_s` — ~15
        real minutes at the values that investigation used — regardless
        of how much history was actually being asked for.

        **A "stop once caught up to live" third stopping condition was
        added here to fix that slowness, then REVERTED after it caused a
        real regression** (see PLAN.md Section 7) — it compared each
        event's own timestamp against wall-clock "now," which rests on an
        assumption about delivery *order* that was never actually
        confirmed and looked likely wrong given how it failed.

        **Root cause actually found, from raw DEBUG logs of a real
        connection (see PLAN.md Section 7):** the "trickle" isn't the
        historical replay running slowly — it's that once the replay
        finishes, the subscription doesn't stop; it keeps re-delivering
        the *current, in-progress* candle over and over as new ticks
        arrive within that same minute (same `time` value, incrementing
        `count` field), indefinitely, roughly every 10-20 seconds. That
        live tail has no natural end and arrives faster than any
        reasonable `idle_timeout_s`, so idle-timeout alone can never tell
        "still receiving real history" apart from "done, now just
        watching the current candle update in place." **Fix: a third
        stopping condition, `stop_on_repeated_key`, keyed on each candle's
        own `time` field** — a genuinely historical, closed candle is only
        ever reported once; only the live/in-progress one gets re-sent, so
        a repeated `time` value is a strong signal that history is
        exhausted.

        **That signal turned out to be too eager — found from real report
        data, not speculation (see PLAN.md Section 7).** At least one real
        `gap-reconcile` run against a ~6-week-wide gap stopped after
        receiving real, correctly-deduplicated data for only the most
        recent ~20 days — a report field (`distinct_days_in_result`) that
        didn't exist until this same investigation made it clear something
        was cutting the reply short well before the older history (which
        the retention probe had already proven is genuinely retrievable
        with enough patience) ever arrived. Something in the middle of a
        large historical replay apparently *can* repeat a key before the
        true, final live-tail repeat shows up — an unconditional
        first-repeat-wins rule stops right there, discarding everything
        older that would have followed. **Fix: gate the repeat check on
        recency** (`repeat_key_min_value`, computed here as "within the
        last 5 minutes of wall-clock now," converted to the same raw
        millisecond units as the candle's own `time` field so the
        comparison is apples-to-apples) — a repeat of an old, already-
        closed candle no longer ends anything by itself; only a repeat
        that's actually plausibly *the* live candle does. This costs
        back some of the speed the unconditional version bought (a
        request may need to ride out more of `idle_timeout_s`/`timeout_s`
        again for the genuinely slow/throttled portions of a large
        replay), but speed was never the point if it meant silently
        returning incomplete data — see this function's own earlier,
        reverted "catch up to live" attempt for the same lesson learned a
        different way.
        """
        from tastytrade.dxfeed import Candle

        streamer = await self._ensure_streamer()
        request_started = datetime.now(timezone.utc)
        # Raw ms-since-epoch, matching event_key's own units (a Candle's
        # `time` field) — see the recency-gating note above.
        repeat_key_min_value = int(
            (request_started - timedelta(minutes=5)).timestamp() * 1000
        )

        subscribed_via = await self._subscribe_candle_compat(streamer, Candle, symbol, period, start_time)
        try:
            events = await collect_events(
                streamer.listen(Candle),
                timeout_s=timeout_s,
                max_count=max_count,  # safety net: an in-progress bar can otherwise stream forever
                event_filter=lambda ev: symbol in str(get_attr_any(ev, "event_symbol", default="")),
                idle_timeout_s=idle_timeout_s,
                stop_on_repeated_key=True,
                event_key=lambda ev: get_attr_any(ev, "time"),
                repeat_key_min_value=repeat_key_min_value,
                diagnostics=diagnostics,
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

        if diagnostics is not None:
            # collect_events already populated stop_reason/elapsed_s/event_count —
            # add the request-level context that makes those numbers actually
            # interpretable (what was asked for, what came back) without
            # needing the raw candle list itself. Built specifically for
            # BackfillJob's diagnostic report — see PLAN.md Section 7 for the
            # investigation that made clear a summary like this was needed
            # (several rounds of "share a huge raw log" that a structured
            # report like this replaces).
            times = [t for t in (_candle_event_time(ev) for ev in events) if t is not None]
            diagnostics.update({
                "symbol": symbol,
                "requested_start": start_time.isoformat(),
                "requested_at": request_started.isoformat(),
                "timeout_s": timeout_s,
                "idle_timeout_s": idle_timeout_s,
                "max_count": max_count,
                "hit_max_count": len(events) >= max_count,
                "earliest_candle": min(times).isoformat() if times else None,
                "latest_candle": max(times).isoformat() if times else None,
            })

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
