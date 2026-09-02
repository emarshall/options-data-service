"""
Hang-proof asyncio helpers shared across source implementations.

These aren't TastyTrade-specific — any DXLink/websocket-based source would
need the same protections — so they live here rather than inside
tastytrade.py. Extracted from scripts/task0_spike/task0_candle_depth_spike.py,
where the first two bugs below were actually hit and fixed against a real
TastyTrade connection; the third was hit later, running the real backfill
job (see PLAN.md Section 7 for the full incident writeups):

1. A plain `asyncio.wait_for(coro, timeout=...)` does not guarantee return
   within `timeout` seconds — if the awaited coroutine doesn't respond
   cleanly to cancellation (e.g. draining a live event stream that never
   naturally ends), the cancellation itself can hang.
2. `asyncio.CancelledError` inherits from `BaseException`, not `Exception`,
   in modern Python — a bare `except Exception:` around a cancellation
   grace period will NOT catch it, so a *successful* cancellation (the
   expected, desired outcome) can crash the caller instead of being treated
   as success.
3. A *shielded* task can still raise `CancelledError` on its own — not
   because anything cancelled it from the outside (shield specifically
   prevents that), but because something internal to the operation failed
   in a way the underlying library surfaces as a cancellation (e.g. a
   websocket connection getting forcibly reset). Left uncaught, this bare
   CancelledError bypasses ordinary `except Exception:` handlers upstream
   (same root issue as #2) and can crash a much larger operation than the
   one that actually failed — this is exactly what happened when a
   too-large subscription message got the DXLink connection reset mid
   `ContractManager.refresh()`, crashing the entire backfill job instead
   of being caught and logged as "failed to resolve this one ticker."

`with_timeout` and `collect_events` below handle all three correctly. Any
new code in this codebase that awaits something on a websocket connection
with a timeout should use one of these rather than a bare `asyncio.wait_for`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Callable, Iterable, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


async def with_timeout(coro, timeout_s: float, label: str, cancel_grace_s: float = 2.0):
    """Await `coro` with a hang-proof timeout.

    Guaranteed to return (or raise) within roughly `timeout_s + cancel_grace_s`
    seconds, regardless of whether the underlying transport responds cleanly
    to cancellation. Raises `asyncio.TimeoutError` on timeout (after
    abandoning a task that wouldn't cancel cleanly within the grace period),
    re-raises whatever ordinary exception `coro` itself raised, re-raises
    `asyncio.CancelledError` normally if *we* (this call's own enclosing
    task) are being cancelled from outside (e.g. a real shutdown), or
    raises `ConnectionError` if the shielded task instead raised
    CancelledError on its own — see point 3 in the module docstring for
    why that specific case needs converting rather than propagating as-is.

    Distinguishing those last two cases (both surface as CancelledError at
    the same line, since shield() only stops external cancellation from
    reaching the *inner* task — it doesn't change what exception the
    *outer* await raises) uses `Task.cancelling()` (Python 3.11+): if our
    own current task has a pending cancellation request, this is genuine
    external cancellation; otherwise the CancelledError must have come
    from the shielded task itself.
    """
    task = asyncio.ensure_future(coro)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)
    except asyncio.TimeoutError:
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=cancel_grace_s)
        except asyncio.CancelledError:
            pass  # expected — cancellation completed cleanly
        except asyncio.TimeoutError:
            log.warning(
                "%s didn't stop within %.1fs of cancellation — abandoning it.",
                label, cancel_grace_s,
            )
        except Exception as e:
            log.warning("Unexpected error while cancelling %s: %s", label, e)
        raise asyncio.TimeoutError(f"{label} timed out after {timeout_s}s")
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling() > 0:
            # Genuine external cancellation of this with_timeout() call
            # itself (e.g. real shutdown) — propagate normally. Cancel the
            # still-shielded inner task too, since we're bailing and
            # nothing else will clean it up.
            task.cancel()
            raise
        log.warning(
            "%s failed: the underlying operation was cancelled internally "
            "(commonly a connection reset) rather than by our own timeout "
            "or an external cancellation.",
            label,
        )
        raise ConnectionError(f"{label} failed: cancelled internally (connection issue?)")


async def collect_events(
    listen_iter: AsyncIterator[Any],
    timeout_s: float,
    max_count: int | None = None,
    cancel_grace_s: float = 2.0,
    event_filter: Callable[[Any], bool] | None = None,
    idle_timeout_s: float | None = None,
    stop_on_repeated_key: bool = False,
    event_key: Callable[[Any], Any | None] | None = None,
    repeat_key_min_value: Any | None = None,
    diagnostics: dict | None = None,
) -> list[Any]:
    """Drain an async event generator for up to timeout_s seconds, or until
    max_count events are collected, whichever comes first. See module
    docstring for why this exists instead of a plain `asyncio.wait_for`.

    `idle_timeout_s`, if given, adds a second, usually-shorter stopping
    condition: return as soon as no new *matching* event has arrived for
    that long, rather than always waiting the full `timeout_s`. This
    matters a lot in practice for anything backed by a DXLink event
    stream that never naturally ends (e.g. `Candle` events for the
    in-progress bar keep streaming indefinitely) — without it, every call
    unconditionally takes the full `timeout_s`, even once all the actual
    data of interest arrived in the first second or two.

    The idle clock only resets on events that pass `event_filter` (i.e.
    ones actually kept) — an unrelated event arriving on the same channel
    doesn't count as "still active" for the thing this call actually cares
    about. Tracked as an absolute deadline recomputed after each kept
    event, not by naively re-arming a fresh `idle_timeout_s` window on
    every raw event regardless of whether it passed the filter.

    `stop_on_repeated_key`/`event_key`, if both given, add a THIRD
    stopping condition -- added after a real, *directly observed* problem
    (raw DEBUG logs of an actual TastyTrade connection -- see PLAN.md
    Section 7): once a `Candle` historical replay finishes, the
    subscription doesn't just quietly stop -- it keeps delivering the
    *current, in-progress* candle over and over as new ticks arrive within
    that same minute (the same `time` value repeated, with an incrementing
    `count` field each time), indefinitely, roughly every 10-20 seconds.
    That live tail has no natural end and arrives faster than any
    reasonable `idle_timeout_s`, so idle-timeout alone can never
    distinguish "still receiving real historical data" from "done, now
    just watching the current candle update in place." `event_key(ev)`
    extracts a comparable value from each *kept* event (its own `time`
    field, for Candles); the first time the *same* key is seen twice,
    collection stops.

    **`repeat_key_min_value`, if given, gates that stop** -- a repeated
    key only ends collection if the key itself is
    `>= repeat_key_min_value`; a repeat of an *older* key is recorded (so
    a later repeat of the same key won't re-trigger anything) but
    collection continues. Added after finding, from real report data (see
    PLAN.md Section 7), that the unconditional version above was firing
    too early: a large historical replay apparently isn't always
    delivered as a single clean run from old to new -- at least once, a
    request that should have covered ~6 weeks of history stopped after
    receiving real data for only the most recent ~20 days, because
    *something* in the older portion of the stream repeated a key before
    the genuinely-final live-tail repeat ever arrived. Gating on recency
    (the caller typically passes something like "within the last few
    minutes of wall-clock now") means only a repeat of what's actually
    plausibly the live, in-progress candle can end the collection -- a
    repeat of an old, already-closed historical candle no longer
    short-circuits anything, which is exactly the failure this was built
    to prevent. Without this parameter (the default), behavior is
    unchanged from the original, unconditional version -- this is an
    additive safety refinement, not a replacement.

    `diagnostics`, if given a dict, gets populated (in place) with
    `stop_reason` (one of `"max_count"`, `"outer_timeout"`,
    `"idle_timeout"`, `"repeated_key"`, `"stream_ended"`), `elapsed_s`,
    and `event_count` -- added specifically so callers troubleshooting a
    slow or incomplete result (e.g. `request_candles`, and in turn
    `BackfillJob`'s diagnostic report -- see PLAN.md Section 7) can report
    *which* stopping condition actually ended a given call without having
    to infer it from indirect evidence the way every earlier round of this
    investigation had to.

    Safe to let a per-item wait_for time out and effectively abandon
    `listen_iter` afterward (rather than trying to keep it usable) -- every
    caller of this function creates a fresh `streamer.listen(...)`
    generator per call and never reuses one across multiple
    collect_events() invocations.
    """
    events: list[Any] = []
    stop_reason = "stream_ended"
    start_wall = asyncio.get_event_loop().time()

    async def _drain():
        nonlocal stop_reason
        loop = asyncio.get_event_loop()
        idle_deadline = loop.time() + idle_timeout_s if idle_timeout_s is not None else None
        seen_keys: set[Any] = set()
        while True:
            try:
                if idle_deadline is not None:
                    remaining = idle_deadline - loop.time()
                    if remaining <= 0:
                        stop_reason = "idle_timeout"
                        return
                    ev = await asyncio.wait_for(listen_iter.__anext__(), timeout=remaining)
                else:
                    ev = await listen_iter.__anext__()
            except StopAsyncIteration:
                stop_reason = "stream_ended"
                return
            except asyncio.TimeoutError:
                stop_reason = "idle_timeout"
                return  # gone idle for idle_timeout_s -- nothing new coming soon, stop early
            if event_filter is not None and not event_filter(ev):
                continue  # doesn't count as activity -- idle_deadline is untouched
            events.append(ev)
            if idle_timeout_s is not None:
                idle_deadline = loop.time() + idle_timeout_s  # reset only on a kept event
            if stop_on_repeated_key and event_key is not None:
                k = event_key(ev)
                if k is not None:
                    if k in seen_keys:
                        if repeat_key_min_value is None or k >= repeat_key_min_value:
                            stop_reason = "repeated_key"
                            return  # this key was already reported once and looks recent enough
                        # else: a repeat of an old, already-closed key -- not the live tail,
                        # just an ordinary duplicate; keep going.
                    seen_keys.add(k)
            if max_count is not None and len(events) >= max_count:
                stop_reason = "max_count"
                return


    task = asyncio.ensure_future(_drain())
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)
    except asyncio.TimeoutError:
        stop_reason = "outer_timeout"
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=cancel_grace_s)
        except asyncio.CancelledError:
            pass  # expected — cancellation completed cleanly
        except asyncio.TimeoutError:
            log.warning(
                "Event drain didn't stop within %.1fs of cancellation — abandoning it.",
                cancel_grace_s,
            )
        except Exception as e:
            log.warning("Unexpected error while cancelling event drain: %s", e)
    except asyncio.CancelledError:
        pass  # e.g. shield itself got cancelled from further up
    except Exception:
        pass

    if diagnostics is not None:
        diagnostics["stop_reason"] = stop_reason
        diagnostics["elapsed_s"] = round(asyncio.get_event_loop().time() - start_wall, 2)
        diagnostics["event_count"] = len(events)

    return events


def get_attr_any(obj: Any, *names: str, default=None):
    """Try several possible attribute names in order — useful for tolerating
    minor field-naming differences across SDK versions (learned the hard way
    in Task 0, where e.g. IV shows up as `imp_volatility` on some event
    types)."""
    for name in names:
        if hasattr(obj, name):
            val = getattr(obj, name)
            if val is not None:
                return val
    return default


def chunked(items: Iterable[T], size: int) -> list[list[T]]:
    """Splits `items` into batches of at most `size`. Used to keep DXLink
    subscribe/unsubscribe messages under the server's ~64KB max frame size
    — a single `streamer.subscribe(Greeks, symbols)` call with a large
    enough `symbols` list (e.g. every candidate contract across a wide
    option chain) can produce a subscription message that exceeds this,
    causing the server to reject it and reset the connection (see the
    "Max frame length ... exceeded" incident in PLAN.md Section 7). `size`
    is a conservative, not empirically-tuned-to-the-byte-limit, choice —
    plenty of headroom below the actual frame limit rather than cutting it
    close.
    """
    items = list(items)
    return [items[i : i + size] for i in range(0, len(items), size)]
