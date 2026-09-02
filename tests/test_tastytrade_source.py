"""
Unit tests for TastyTradeSource.

These use fakes for tastytrade.Session / DXLinkStreamer rather than hitting
a real TastyTrade connection — the goal here is to validate the bookkeeping
logic this module owns (subscription tracking, reconnect-and-resubscribe,
callback dispatch, candle request lifecycle), not to re-verify the SDK
itself. The fakes' shapes were checked against the real, currently-pinned
`tastytrade` package (v13.2.0) — see PLAN.md Task 2 notes for the exact
signatures confirmed.

A real-credentials smoke test lives separately at
scripts/task2_smoke_test.py, for validating this actually works end-to-end
against TastyTrade's servers (something these unit tests can't do).
"""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from service.sources.tastytrade import TastyTradeSource


class FakeEvent:
    def __init__(self, event_symbol: str, **kwargs):
        self.event_symbol = event_symbol
        for k, v in kwargs.items():
            setattr(self, k, v)


class FakeStreamer:
    """Mimics just enough of DXLinkStreamer's shape for these tests:
    subscribe/unsubscribe bookkeeping, and a controllable listen() async
    generator per event type that the test can push events into (or make
    raise, to simulate a disconnect)."""

    def __init__(self):
        self.subscribed: dict[type, set[str]] = {}
        self.subscribe_calls: list[tuple[type, list[str]]] = []
        self.unsubscribed_calls: list[tuple[type, list[str]]] = []
        self.subscribe_candle_calls: list[tuple[str, str]] = []  # (symbol, interval)
        self.unsubscribe_candle_calls: list[tuple[str, str]] = []  # (ticker, interval)
        self._queues: dict[type, asyncio.Queue] = {}
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True

    async def subscribe(self, event_class, symbols, refresh_interval=0.1):
        symbols = list(symbols)
        self.subscribe_calls.append((event_class, symbols))
        self.subscribed.setdefault(event_class, set()).update(symbols)

    async def unsubscribe(self, event_class, symbols):
        self.unsubscribed_calls.append((event_class, list(symbols)))
        self.subscribed.setdefault(event_class, set()).difference_update(symbols)

    async def subscribe_candle(self, symbols, interval, start_time=None, extended_trading_hours=False, refresh_interval=0.1):
        for s in symbols:
            self.subscribe_candle_calls.append((s, interval))
            # Matches the real SDK's actual wire-level symbol format
            # (confirmed by reading its source — see PLAN.md Section 7):
            # subscribe_candle() appends ",tho=true" by default, which a
            # manually-constructed f"{symbol}{{={interval}}}" string does
            # NOT include. Deliberately different from that naive format
            # here so a test using the wrong symbol to unsubscribe would
            # actually be caught, the way it wasn't in the real bug.
            suffix = interval if extended_trading_hours else f"{interval},tho=true"
            self.subscribed.setdefault("candle", set()).add(f"{s}{{={suffix}}}")

    async def unsubscribe_candle(self, ticker, interval=None, extended_trading_hours=False):
        self.unsubscribe_candle_calls.append((ticker, interval))
        suffix = interval if extended_trading_hours else f"{interval},tho=true"
        self.subscribed.setdefault("candle", set()).discard(f"{ticker}{{={suffix}}}")

    def _queue_for(self, event_class) -> asyncio.Queue:
        if event_class not in self._queues:
            self._queues[event_class] = asyncio.Queue()
        return self._queues[event_class]

    def push_event(self, event_class, event):
        """Test helper: makes `event` show up in listen(event_class)."""
        self._queue_for(event_class).put_nowait(("event", event))

    def push_failure(self, event_class, exc: Exception):
        """Test helper: makes listen(event_class) raise `exc`."""
        self._queue_for(event_class).put_nowait(("error", exc))

    async def listen(self, event_class):
        queue = self._queue_for(event_class)
        while True:
            kind, payload = await queue.get()
            if kind == "error":
                raise payload
            yield payload


@pytest.fixture
def fake_streamer():
    return FakeStreamer()


@pytest.fixture
def source(monkeypatch, fake_streamer):
    """A TastyTradeSource wired up to the fake streamer/session, already
    "authenticated" without touching the network."""
    src = TastyTradeSource(client_secret="secret", refresh_token="token", use_sandbox=True)
    src._session = object()  # anything non-None; authenticate() itself isn't under test here
    src._streamer = fake_streamer

    async def fake_authenticate():
        src._session = object()

    monkeypatch.setattr(src, "authenticate", fake_authenticate)
    return src


@pytest.mark.asyncio
async def test_subscribe_quotes_dispatches_events_to_callback(source, fake_streamer):
    received = []

    async def callback(event):
        received.append(event)

    await source.subscribe_quotes(["SPY"], callback)
    await asyncio.sleep(0)  # let the listener task start

    from tastytrade.dxfeed import Quote

    assert fake_streamer.subscribed[Quote] == {"SPY"}

    fake_streamer.push_event(Quote, FakeEvent("SPY", bid_price=1.0))
    await asyncio.sleep(0.05)

    assert len(received) == 1
    assert received[0].event_symbol == "SPY"

    await source.close()


@pytest.mark.asyncio
async def test_resubscribing_replaces_callback_not_symbols(source, fake_streamer):
    from tastytrade.dxfeed import Quote

    received_a, received_b = [], []

    await source.subscribe_quotes(["SPY"], lambda e: received_a.append(e))
    await asyncio.sleep(0)
    await source.subscribe_quotes(["QQQ"], _make_async_appender(received_b))
    await asyncio.sleep(0)

    # Symbols accumulate...
    assert fake_streamer.subscribed[Quote] == {"SPY", "QQQ"}

    # ...but the callback was replaced, not multiplexed (documented design
    # decision — see tastytrade.py module docstring).
    fake_streamer.push_event(Quote, FakeEvent("SPY"))
    await asyncio.sleep(0.05)
    assert received_a == []  # old callback never fires again
    assert len(received_b) == 1

    await source.close()


def _make_async_appender(lst):
    async def _cb(event):
        lst.append(event)
    return _cb


@pytest.mark.asyncio
async def test_unsubscribe_removes_tracked_symbols_and_calls_streamer(source, fake_streamer):
    from tastytrade.dxfeed import Quote

    await source.subscribe_quotes(["SPY", "QQQ"], _make_async_appender([]))
    await asyncio.sleep(0)

    await source.unsubscribe(["SPY"])

    assert "SPY" not in source._quote_symbols
    assert "QQQ" in source._quote_symbols
    assert (Quote, ["SPY"]) in fake_streamer.unsubscribed_calls

    await source.close()


@pytest.mark.asyncio
async def test_disconnect_triggers_reconnect_and_resubscribe(source, fake_streamer, monkeypatch):
    from tastytrade.dxfeed import Quote

    received = []
    await source.subscribe_quotes(["SPY"], _make_async_appender(received))
    await asyncio.sleep(0)

    # Replace _ensure_streamer so reconnect "succeeds" with a fresh fake
    # streamer, and speed up the backoff so the test doesn't actually wait.
    new_streamer = FakeStreamer()

    async def fake_ensure_streamer():
        source._streamer = new_streamer
        return new_streamer

    monkeypatch.setattr(source, "_ensure_streamer", fake_ensure_streamer)
    monkeypatch.setattr(
        "service.sources.tastytrade._RECONNECT_BACKOFF_S", [0],
    )

    # Simulate the connection dying.
    fake_streamer.push_failure(Quote, ConnectionError("simulated drop"))
    await asyncio.sleep(0.2)  # let the listener notice, reconnect, resubscribe

    assert new_streamer.subscribed.get(Quote) == {"SPY"}, (
        "resubscription after reconnect should include the symbol that was "
        "tracked before the disconnect"
    )

    # New streamer should now deliver events to the same callback.
    new_streamer.push_event(Quote, FakeEvent("SPY"))
    await asyncio.sleep(0.05)
    assert len(received) == 1

    await source.close()


@pytest.mark.asyncio
async def test_request_candles_collects_and_unsubscribes(source, fake_streamer):
    from tastytrade.dxfeed import Candle

    async def fake_collect_events(listen_iter, **kwargs):
        return [FakeEvent(".SPY260716C750", time=1, close=1.23)]

    import service.sources.tastytrade as ttmod
    orig = ttmod.collect_events

    async def patched(*args, **kwargs):
        return await fake_collect_events(*args, **kwargs)

    ttmod.collect_events = patched
    try:
        events = await source.request_candles(
            ".SPY260716C750", "1m", datetime.now(timezone.utc)
        )
    finally:
        ttmod.collect_events = orig

    assert len(events) == 1
    assert events[0].close == 1.23
    # Real regression: must use unsubscribe_candle (matching how it was
    # subscribed), NOT the generic unsubscribe with a manually-built
    # symbol string that doesn't match what subscribe_candle() actually
    # registered server-side — see PLAN.md Section 7 for the incident.
    assert fake_streamer.unsubscribe_candle_calls == [(".SPY260716C750", "1m")]
    assert not any(call[0] == Candle for call in fake_streamer.unsubscribed_calls)

    await source.close()


@pytest.mark.asyncio
async def test_request_candles_unsubscribe_does_not_leak_when_subscribed_via_subscribe_candle(
    source, fake_streamer
):
    """Direct regression test for the real bug: subscribing via
    subscribe_candle() then unsubscribing via the generic unsubscribe()
    with a manually-built symbol string silently fails to remove the
    subscription server-side, because subscribe_candle() registers a
    different wire-level symbol (appends ",tho=true" by default) than the
    naive f"{symbol}{{={period}}}" string. Across many contracts processed
    sequentially (a real backfill run), those leaked subscriptions
    accumulate until the server refuses further changes. This test proves
    the fix: after request_candles() completes, nothing should remain in
    the fake streamer's tracked candle subscriptions at all."""
    from tastytrade.dxfeed import Candle

    async def fake_collect_events(listen_iter, **kwargs):
        return []

    import service.sources.tastytrade as ttmod

    orig = ttmod.collect_events
    ttmod.collect_events = fake_collect_events
    try:
        await source.request_candles(".SPY260716C750", "1m", datetime.now(timezone.utc))
    finally:
        ttmod.collect_events = orig

    assert fake_streamer.subscribed.get("candle", set()) == set(), (
        "candle subscription leaked — unsubscribe didn't actually clear it server-side"
    )

    await source.close()


@pytest.mark.asyncio
async def test_request_candles_wires_repeated_key_condition(source, fake_streamer):
    """Confirms request_candles() wires collect_events()'s
    stop_on_repeated_key condition, keyed on each candle's own `time`
    field — the fix for the real, log-confirmed root cause of the
    "backfill takes forever" bug (see PLAN.md Section 7): once historical
    replay finishes, the current candle gets re-broadcast indefinitely
    with the same `time` value, which this condition detects to stop
    promptly rather than waiting out idle_timeout_s/timeout_s."""
    captured_kwargs = {}

    async def fake_collect_events(listen_iter, **kwargs):
        captured_kwargs.update(kwargs)
        return []

    import service.sources.tastytrade as ttmod

    orig = ttmod.collect_events
    ttmod.collect_events = fake_collect_events
    try:
        await source.request_candles(".SPY260716C750", "1m", datetime.now(timezone.utc))
    finally:
        ttmod.collect_events = orig

    assert captured_kwargs["stop_on_repeated_key"] is True
    ev = FakeEvent(".SPY260716C750", time=12345)
    assert captured_kwargs["event_key"](ev) == 12345

    await source.close()


@pytest.mark.asyncio
async def test_request_candles_gates_repeat_on_recency(source, fake_streamer):
    """Confirms request_candles() computes repeat_key_min_value as "now
    minus 5 minutes" in the same raw-millisecond units as a candle's own
    `time` field — the fix for a real regression where the unconditional
    repeated-key stop fired on a repeat of an OLD key, cutting a large
    historical replay short well before the true live tail. See PLAN.md
    Section 7."""
    captured_kwargs = {}

    async def fake_collect_events(listen_iter, **kwargs):
        captured_kwargs.update(kwargs)
        return []

    import service.sources.tastytrade as ttmod

    orig = ttmod.collect_events
    ttmod.collect_events = fake_collect_events
    try:
        before = datetime.now(timezone.utc)
        await source.request_candles(".SPY260716C750", "1m", datetime.now(timezone.utc))
        after = datetime.now(timezone.utc)
    finally:
        ttmod.collect_events = orig

    threshold_ms = captured_kwargs["repeat_key_min_value"]
    expected_before = int((before - timedelta(minutes=5)).timestamp() * 1000)
    expected_after = int((after - timedelta(minutes=5)).timestamp() * 1000)
    assert expected_before <= threshold_ms <= expected_after

    await source.close()


@pytest.mark.asyncio
async def test_request_candles_populates_diagnostics(source, fake_streamer):
    """Confirms request_candles() fills in the request-level context
    (symbol, requested_start, effective timeout/idle_timeout/max_count,
    earliest/latest candle, hit_max_count) on top of whatever
    collect_events() itself already populated (stop_reason, elapsed_s,
    event_count) — built specifically so a diagnostic report can explain
    a slow/incomplete result without needing the raw candle list or
    verbose logs. See PLAN.md Section 7."""

    async def fake_collect_events(listen_iter, diagnostics=None, **kwargs):
        if diagnostics is not None:
            diagnostics["stop_reason"] = "repeated_key"
            diagnostics["elapsed_s"] = 1.23
            diagnostics["event_count"] = 2
        return [
            FakeEvent(".SPY260716C750", time=1_700_000_000_000),
            FakeEvent(".SPY260716C750", time=1_700_000_060_000),
        ]

    import service.sources.tastytrade as ttmod

    orig = ttmod.collect_events
    ttmod.collect_events = fake_collect_events
    try:
        requested_start = datetime.now(timezone.utc)
        diag: dict = {}
        events = await source.request_candles(
            ".SPY260716C750", "1m", requested_start,
            timeout_s=99.0, idle_timeout_s=5.0, max_count=50,
            diagnostics=diag,
        )
    finally:
        ttmod.collect_events = orig

    assert len(events) == 2
    # From collect_events itself:
    assert diag["stop_reason"] == "repeated_key"
    assert diag["elapsed_s"] == 1.23
    assert diag["event_count"] == 2
    # From request_candles' own augmentation:
    assert diag["symbol"] == ".SPY260716C750"
    assert diag["requested_start"] == requested_start.isoformat()
    assert diag["timeout_s"] == 99.0
    assert diag["idle_timeout_s"] == 5.0
    assert diag["max_count"] == 50
    assert diag["hit_max_count"] is False
    assert diag["earliest_candle"] == datetime.fromtimestamp(1_700_000_000_000 / 1000, tz=timezone.utc).isoformat()
    assert diag["latest_candle"] == datetime.fromtimestamp(1_700_000_060_000 / 1000, tz=timezone.utc).isoformat()

    await source.close()


@pytest.mark.asyncio
async def test_request_candles_diagnostics_is_optional(source, fake_streamer):
    """Not passing diagnostics at all (the common case) shouldn't error —
    confirms the feature is purely additive."""

    async def fake_collect_events(listen_iter, **kwargs):
        return []

    import service.sources.tastytrade as ttmod

    orig = ttmod.collect_events
    ttmod.collect_events = fake_collect_events
    try:
        events = await source.request_candles(".SPY260716C750", "1m", datetime.now(timezone.utc))
    finally:
        ttmod.collect_events = orig

    assert events == []
    await source.close()


def test_candle_event_time_extracts_ms_timestamp():
    from service.sources.tastytrade import _candle_event_time

    ev = FakeEvent(".SPY260716C750", time=1_800_000_000_000)  # ms since epoch
    result = _candle_event_time(ev)

    assert result == datetime.fromtimestamp(1_800_000_000_000 / 1000, tz=timezone.utc)


def test_candle_event_time_returns_none_when_time_missing():
    from service.sources.tastytrade import _candle_event_time

    ev = FakeEvent(".SPY260716C750")  # no `time` attribute set

    assert _candle_event_time(ev) is None


@pytest.mark.asyncio
async def test_close_cancels_listener_tasks(source):
    await source.subscribe_quotes(["SPY"], _make_async_appender([]))
    await asyncio.sleep(0)

    task = source._quote_listener_task
    assert task is not None and not task.done()

    await source.close()

    assert task.done()
    assert source._closed is True


@pytest.mark.asyncio
async def test_large_symbol_list_is_subscribed_in_batches(source, fake_streamer):
    """Regression test for the real bug hit running the backfill job: a
    single subscribe() call with too many symbols exceeded DXLink's ~64KB
    max frame size and got the connection reset. Symbol lists larger than
    the batch size must be split into multiple subscribe() calls."""
    from tastytrade.dxfeed import Quote

    many_symbols = [f"SYM{i}" for i in range(450)]  # > 200, the batch size
    await source.subscribe_quotes(many_symbols, _make_async_appender([]))

    quote_calls = [c for c in fake_streamer.subscribe_calls if c[0] == Quote]
    assert len(quote_calls) == 3, "450 symbols at batch size 200 should be 3 calls (200+200+50)"
    assert all(len(symbols) <= 200 for _, symbols in quote_calls)
    # Every symbol still ends up subscribed, regardless of batching.
    assert fake_streamer.subscribed[Quote] == set(many_symbols)

    await source.close()


@pytest.mark.asyncio
async def test_snapshot_greeks_batches_large_symbol_lists(source, fake_streamer):
    """Same batching protection applied to snapshot_greeks specifically —
    this was the exact call site in the traceback that crashed the real
    backfill job (ContractManager resolving a wide option chain)."""
    from tastytrade.dxfeed import Greeks

    many_symbols = [f"SYM{i}" for i in range(450)]

    await source.snapshot_greeks(many_symbols, timeout_s=0.1)

    greeks_calls = [c for c in fake_streamer.subscribe_calls if c[0] == Greeks]
    assert len(greeks_calls) == 3
    assert all(len(symbols) <= 200 for _, symbols in greeks_calls)

    await source.close()
