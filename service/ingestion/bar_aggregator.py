"""
Buckets live Quote/Greeks events into 1-minute OHLC(+Greeks) bars.

Two real findings from Task 0 directly shape this design:

1. **Quote events don't carry a usable timestamp.** Task 0's captured
   sample data showed `event_time`/`bid_time`/`ask_time` all as `0` on real
   Quote events — dxfeed just doesn't populate them on this event type in
   practice. Greeks events *do* have a real `time` field, but for
   consistency (and since the asymmetry doesn't matter at 1-minute
   granularity — network latency is sub-second) this bucket by **wall-clock
   receipt time** for both event types, not any field on the event itself.
2. **There's no explicit "mark price" field.** Mark is computed here as
   the bid/ask midpoint (the standard definition, and what TastyTrade's own
   platform uses) — resolving the "mark vs. last trade price" open question
   from PLAN.md Task 4 in favor of mark, now that we know Quote events
   reliably carry bid/ask.

This class is agnostic to whether `key` is an option contract_id or an
underlying ticker — the pipeline (pipeline.py) decides which bucket set a
given symbol's events belong in.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from service.sources._async_utils import is_nan


@dataclass
class Bucket:
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    bid: float | None = None
    ask: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None
    iv: float | None = None
    has_greeks: bool = False

    def update_price(self, mark: float, bid: float | None, ask: float | None) -> None:
        if self.open is None:
            self.open = mark
        self.high = mark if self.high is None else max(self.high, mark)
        self.low = mark if self.low is None else min(self.low, mark)
        self.close = mark
        self.bid = bid
        self.ask = ask

    def merge_older(self, older: "Bucket") -> None:
        """Folds a bucket covering *earlier* data for the same minute into
        this one, preserving correct OHLC semantics.

        Only called from `restore()`, where the direction is always fixed:
        the bucket being restored was popped before this one started
        accumulating, so `self` is the newer of the two. That matters —
        `self`'s close/bid/ask and its Greeks snapshot are the later
        observations and must win, while the open and the high/low
        extremes have to still account for the older data or the bar would
        silently understate its own range.

        Overwriting either bucket outright instead of merging would
        silently discard real observed data on one side or the other.
        """
        # `open` is the *first observed* price, not the minimum — so the
        # older bucket's value wins outright. It must not be folded in with
        # min()/max() the way high/low are: a bar whose first tick was 120
        # and whose later ticks fell to 100 opens at 120, and a min() here
        # would silently report 100.
        if older.open is not None:
            self.open = older.open
        if older.high is not None:
            self.high = older.high if self.high is None else max(self.high, older.high)
        if older.low is not None:
            self.low = older.low if self.low is None else min(self.low, older.low)
        # Close/bid/ask: this bucket's own values are the later ones and
        # win; only fall back to the older bucket if we have none at all.
        if self.close is None:
            self.close = older.close
        if self.bid is None:
            self.bid = older.bid
        if self.ask is None:
            self.ask = older.ask
        # Greeks: same reasoning — ours is the newer snapshot.
        if not self.has_greeks and older.has_greeks:
            self.delta, self.gamma, self.theta = older.delta, older.gamma, older.theta
            self.vega, self.rho, self.iv = older.vega, older.rho, older.iv
            self.has_greeks = True

    def update_greeks(self, delta, gamma, theta, vega, rho, iv) -> None:
        self.delta, self.gamma, self.theta, self.vega, self.rho, self.iv = delta, gamma, theta, vega, rho, iv
        self.has_greeks = True


class BarAggregator:
    """Buckets by (key, minute_epoch_seconds). A bucket is considered
    "ready to flush" once its minute has fully elapsed plus a grace period
    for late-arriving events — see pop_ready()."""

    def __init__(self, flush_grace_seconds: float = 5.0):
        self._buckets: dict[tuple[str, int], Bucket] = {}
        self._flush_grace_seconds = flush_grace_seconds

    @staticmethod
    def _minute_epoch(ts: datetime) -> int:
        return int(ts.timestamp() // 60 * 60)

    @staticmethod
    def _mark(bid: float | None, ask: float | None) -> float | None:
        """Mark = bid/ask midpoint. Falls back to whichever side is
        present if only one is — e.g. deep OTM contracts sometimes quote
        one-sided. Returns None if neither side is present (nothing to
        record).

        NaN is normalized to None here as a second line of defense —
        `get_attr_any` already filters NaN on the way in, but this is the
        single choke point every price passes through, and a NaN reaching
        a `Numeric` column is silently corrupt rather than loudly wrong.
        """
        if is_nan(bid):
            bid = None
        if is_nan(ask):
            ask = None
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        return bid if bid is not None else ask

    def on_quote(self, key: str, bid: float | None, ask: float | None, received_at: datetime) -> None:
        mark = self._mark(bid, ask)
        if mark is None:
            return
        minute = self._minute_epoch(received_at)
        bucket = self._buckets.setdefault((key, minute), Bucket())
        bucket.update_price(mark, bid, ask)

    def on_greeks(
        self, key: str, delta, gamma, theta, vega, rho, iv, received_at: datetime
    ) -> None:
        minute = self._minute_epoch(received_at)
        bucket = self._buckets.setdefault((key, minute), Bucket())
        bucket.update_greeks(delta, gamma, theta, vega, rho, iv)

    def pop_ready(self, now: datetime) -> list[tuple[str, int, Bucket]]:
        """Returns and removes every bucket whose minute has fully elapsed
        (past its 60s window plus the grace period). Callers should treat
        the returned `minute` as epoch seconds, already aligned to the
        minute boundary — convert with
        `datetime.fromtimestamp(minute, tz=timezone.utc)`."""
        now_ts = now.timestamp()
        ready = []
        for k in list(self._buckets.keys()):
            _key, minute = k
            if now_ts >= minute + 60 + self._flush_grace_seconds:
                ready.append((k[0], k[1], self._buckets.pop(k)))
        return ready

    def restore(self, ready: list[tuple[str, int, Bucket]]) -> None:
        """Puts buckets previously returned by `pop_ready()` back into
        memory, for use when writing them out failed.

        Without this, `pop_ready()`'s removal is destructive and
        unrecoverable: a failed commit would discard those bars entirely
        and nothing would ever re-emit them, because the only thing that
        creates a bucket is a live event for that exact minute — which
        will not come again once the minute has passed. The next
        `pop_ready()` call re-emits them (they still satisfy the
        ready-bucket predicate), so this is a genuine retry, not just a
        way to avoid losing the data.

        If a bucket for the same (key, minute) has since started
        accumulating again — possible because awaiting the failed write
        yields control and more events can arrive in that window — the
        two are merged rather than one overwriting the other.
        """
        for key, minute, bucket in ready:
            existing = self._buckets.get((key, minute))
            if existing is None:
                self._buckets[(key, minute)] = bucket
            else:
                existing.merge_older(bucket)

    def pending_count(self) -> int:
        """Number of buckets currently held in memory, not yet flushed —
        useful for monitoring/tests."""
        return len(self._buckets)
