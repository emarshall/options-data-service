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
