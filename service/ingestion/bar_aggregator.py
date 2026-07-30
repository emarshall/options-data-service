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
        record)."""
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

    def pending_count(self) -> int:
        """Number of buckets currently held in memory, not yet flushed —
        useful for monitoring/tests."""
        return len(self._buckets)
