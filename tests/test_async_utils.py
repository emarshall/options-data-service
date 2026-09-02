"""
Unit tests for service/sources/_async_utils.py — the shared hang-proof
helpers. See the module's own docstring for the three bugs these guard
against; each has a direct regression test here.
"""

import asyncio

import pytest

from service.sources._async_utils import chunked, collect_events, with_timeout


@pytest.mark.asyncio
async def test_with_timeout_returns_normally_on_success():
    async def ok():
        return 42

    assert await with_timeout(ok(), timeout_s=1.0, label="test") == 42


@pytest.mark.asyncio
async def test_with_timeout_raises_timeout_error_on_slow_coro():
    async def slow():
        await asyncio.sleep(10)

    with pytest.raises(asyncio.TimeoutError):
        await with_timeout(slow(), timeout_s=0.1, label="test", cancel_grace_s=0.1)


@pytest.mark.asyncio
async def test_with_timeout_reraises_ordinary_exceptions():
    async def fails():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await with_timeout(fails(), timeout_s=1.0, label="test")


@pytest.mark.asyncio
async def test_with_timeout_converts_internally_cancelled_task_to_connection_error():
    """Regression test for the bug found running the real backfill job:
    when the DXLink connection got reset mid-subscribe (a too-large
    message exceeding the server's frame size limit), the underlying
    library surfaced this as a bare CancelledError from inside the
    shielded task — not from our own timeout. Left unconverted, that
    CancelledError (a BaseException) bypassed ContractManager's
    `except Exception:` handler and crashed the entire backfill job
    instead of being caught and logged as one failed ticker. See PLAN.md
    Section 7 for the full incident."""

    async def internally_cancelled():
        raise asyncio.CancelledError()

    with pytest.raises(ConnectionError):
        await with_timeout(internally_cancelled(), timeout_s=1.0, label="test")


@pytest.mark.asyncio
async def test_with_timeout_still_propagates_genuine_external_cancellation():
    """A with_timeout() call that's itself cancelled from the outside
    (e.g. its enclosing task being cancelled, as during a real shutdown)
    should still raise CancelledError — only an *internal* cancellation
    (the shielded task cancelling itself) gets converted."""

    async def slow():
        await asyncio.sleep(10)

    outer_task = asyncio.ensure_future(with_timeout(slow(), timeout_s=5.0, label="test"))
    await asyncio.sleep(0.05)
    outer_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await outer_task


def test_chunked_splits_into_batches_of_at_most_size():
    items = list(range(450))
    batches = chunked(items, 200)
    assert [len(b) for b in batches] == [200, 200, 50]
    assert [x for batch in batches for x in batch] == items


def test_chunked_empty_input():
    assert chunked([], 200) == []


def test_chunked_input_smaller_than_batch_size():
    assert chunked([1, 2, 3], 200) == [[1, 2, 3]]


def test_chunked_exact_multiple_of_batch_size():
    items = list(range(400))
    batches = chunked(items, 200)
    assert [len(b) for b in batches] == [200, 200]


@pytest.mark.asyncio
async def test_collect_events_idle_timeout_returns_early_when_stream_goes_quiet():
    """Core regression test for the real backfill-slowness bug: a stream
    that delivers everything quickly, then goes quiet, should return in
    roughly idle_timeout_s — NOT wait the full (much larger) timeout_s.
    This is exactly what made every request_candles() call take a fixed
    30s regardless of how fast the actual data arrived (see PLAN.md
    Section 7)."""

    async def bursty_then_quiet():
        for i in range(5):
            yield i
        await asyncio.sleep(1000)  # never yields again within any reasonable test timeout
        yield "should never reach this"

    start = asyncio.get_event_loop().time()
    events = await collect_events(
        bursty_then_quiet(), timeout_s=30.0, idle_timeout_s=0.2,
    )
    elapsed = asyncio.get_event_loop().time() - start

    assert events == [0, 1, 2, 3, 4]
    assert elapsed < 2.0, (
        f"took {elapsed:.2f}s — should return shortly after idle_timeout_s (0.2s), "
        f"nowhere near the 30s overall timeout_s"
    )


@pytest.mark.asyncio
async def test_collect_events_without_idle_timeout_waits_full_timeout():
    """Sanity check that the old behavior (no idle_timeout_s given) is
    unchanged — still waits the full timeout_s when the stream doesn't
    naturally end, confirming idle_timeout_s is opt-in, not a silent
    behavior change for existing callers that don't pass it."""

    async def never_ends():
        yield 1
        await asyncio.sleep(1000)

    start = asyncio.get_event_loop().time()
    events = await collect_events(never_ends(), timeout_s=0.3)
    elapsed = asyncio.get_event_loop().time() - start

    assert events == [1]
    assert elapsed >= 0.25  # waited close to the full timeout_s, not idle-exited early


@pytest.mark.asyncio
async def test_collect_events_idle_timeout_respects_event_filter():
    """Events filtered out shouldn't count as "activity" that resets the
    idle clock — matches how request_candles()/snapshot_greeks() actually
    use event_filter (only symbols they care about should count). Proven
    here by having filtered-out events trickle in continuously, faster
    than idle_timeout_s — a naive "any event resets the clock"
    implementation would never idle-exit at all in this scenario."""

    async def one_keeper_then_endless_noise():
        yield "keep"
        while True:
            await asyncio.sleep(0.05)  # faster than idle_timeout_s below
            yield "drop"

    start = asyncio.get_event_loop().time()
    events = await collect_events(
        one_keeper_then_endless_noise(), timeout_s=5.0, idle_timeout_s=0.3,
        event_filter=lambda ev: ev == "keep",
    )
    elapsed = asyncio.get_event_loop().time() - start

    assert events == ["keep"]
    assert elapsed < 1.0, (
        f"took {elapsed:.2f}s — continuous filtered-out 'drop' events should not have "
        f"kept resetting the idle clock and preventing early exit"
    )


@pytest.mark.asyncio
async def test_collect_events_stops_on_repeated_key_even_with_continuous_trickle():
    """Core regression test for the real "backfill is taking way too
    long" bug and its actual, log-confirmed root cause (see PLAN.md
    Section 7): once a Candle historical replay finishes, TastyTrade
    keeps re-delivering the *same* in-progress candle (same `time`
    value) indefinitely as new ticks arrive, faster than idle_timeout_s
    can ever catch — so idle_timeout_s alone never fires, and every
    request used to run the full timeout_s regardless of how much
    history was actually needed. A stream that yields NEW keys, then
    starts repeating one, should stop as soon as the repeat is seen —
    not run for the full timeout_s."""

    async def historical_then_live_tail():
        for k in range(5):  # "historical" — each key seen once
            await asyncio.sleep(0.02)
            yield k
        while True:  # "live tail" — same key repeated forever, like the real candle re-broadcast
            await asyncio.sleep(0.02)
            yield 4

    start = asyncio.get_event_loop().time()
    events = await collect_events(
        historical_then_live_tail(), timeout_s=30.0, idle_timeout_s=5.0,
        stop_on_repeated_key=True, event_key=lambda ev: ev,
    )
    elapsed = asyncio.get_event_loop().time() - start

    assert events == [0, 1, 2, 3, 4, 4]  # all 5 historical keys, plus the one repeat that triggered the stop
    assert elapsed < 2.0, (
        f"took {elapsed:.2f}s — should have stopped as soon as a key repeated, nowhere near "
        f"the 30s overall timeout_s or even the 5s idle_timeout_s (which a continuous trickle "
        f"of 'live' repeats never gives a chance to fire)"
    )


@pytest.mark.asyncio
async def test_collect_events_repeated_key_condition_is_opt_in():
    """Without both stop_on_repeated_key and event_key supplied, behavior
    is unchanged — confirms this is additive, not a change to any existing
    caller that doesn't pass these two new params."""

    async def endless():
        i = 0
        while True:
            await asyncio.sleep(0.01)
            yield i
            i += 1

    events = await collect_events(endless(), timeout_s=0.2)
    assert len(events) >= 1  # ordinary timeout-bound behavior, no early exit attempted


@pytest.mark.asyncio
async def test_collect_events_repeated_key_condition_ignores_events_with_no_key():
    """An event whose key extractor returns None (e.g. a malformed event
    missing the expected field) shouldn't ever count as a "repeat" —
    only idle_timeout_s/timeout_s/max_count end the collection in that
    case."""

    async def all_keyless():
        for _ in range(3):
            yield "no-time-field"

    events = await collect_events(
        all_keyless(), timeout_s=0.3, idle_timeout_s=0.1,
        stop_on_repeated_key=True, event_key=lambda ev: None,
    )
    assert events == ["no-time-field"] * 3  # ran out via idle_timeout, never "repeated" a real key


@pytest.mark.asyncio
async def test_collect_events_repeated_key_ignores_distinct_keys_forever():
    """A stream of purely distinct keys (the normal, all-historical case
    — nothing ever repeats) should never trigger this condition; it
    should end via idle_timeout_s/timeout_s as before."""

    async def all_distinct():
        for k in range(4):
            yield k

    events = await collect_events(
        all_distinct(), timeout_s=0.3, idle_timeout_s=0.1,
        stop_on_repeated_key=True, event_key=lambda ev: ev,
    )
    assert events == [0, 1, 2, 3]


# --- repeat_key_min_value: gate the repeated-key stop on recency ---
# Added after real report data showed the unconditional version firing too early: a large
# historical replay stopped after only ~20 recent days instead of the full ~6 weeks requested,
# because something repeated an old key before the true, final live-tail repeat arrived. See
# PLAN.md Section 7.


@pytest.mark.asyncio
async def test_repeat_of_an_old_key_does_not_stop_when_gated_by_recency():
    """The core regression test: a repeat of a key well below the recency
    threshold should NOT end collection — only a repeat of a key at or
    above the threshold should."""

    async def historical_repeat_then_real_data_then_live_repeat():
        yield 1  # old historical key
        yield 1  # a repeat of the OLD key — should NOT stop us (below threshold)
        yield 2  # more genuine historical data that would've been lost before this fix
        yield 3
        yield 100  # a "live" key, at/above the recency threshold
        yield 100  # repeat of the recent key — SHOULD stop us

    events = await collect_events(
        historical_repeat_then_real_data_then_live_repeat(),
        timeout_s=1.0, stop_on_repeated_key=True, event_key=lambda ev: ev,
        repeat_key_min_value=100,
    )

    assert events == [1, 1, 2, 3, 100, 100]


@pytest.mark.asyncio
async def test_repeat_of_a_recent_key_stops_immediately_when_gated_by_recency():
    async def stream():
        yield 50
        yield 100
        yield 100  # repeat, and 100 >= the threshold — should stop right here
        yield 999  # should never be reached

    events = await collect_events(
        stream(), timeout_s=1.0, stop_on_repeated_key=True, event_key=lambda ev: ev,
        repeat_key_min_value=100,
    )

    assert events == [50, 100, 100]


@pytest.mark.asyncio
async def test_repeat_key_min_value_defaults_to_unconditional_behavior():
    """Not passing repeat_key_min_value at all (the pre-existing default)
    preserves the original, unconditional first-repeat-wins behavior —
    confirms this is a strictly additive, opt-in refinement."""

    async def stream():
        yield 1
        yield 1  # a repeat of an "old" key, but no threshold was given — should still stop
        yield 999

    events = await collect_events(
        stream(), timeout_s=1.0, stop_on_repeated_key=True, event_key=lambda ev: ev,
    )

    assert events == [1, 1]


@pytest.mark.asyncio
async def test_repeat_key_below_threshold_does_not_retrigger_on_its_own_later_repeat():
    """A key that repeated once below the threshold shouldn't cause a
    second repeat of that same (still-old) key to behave any
    differently — it's recorded as seen either way, just never allowed
    to stop things by itself while below the threshold."""

    async def stream():
        yield 1
        yield 1  # repeat #1 of an old key — below threshold, doesn't stop
        yield 1  # repeat #2 of the same old key — still below threshold, still doesn't stop
        yield 100
        yield 100  # this one is at the threshold — should stop

    events = await collect_events(
        stream(), timeout_s=1.0, stop_on_repeated_key=True, event_key=lambda ev: ev,
        repeat_key_min_value=100,
    )

    assert events == [1, 1, 1, 100, 100]
