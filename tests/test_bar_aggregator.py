"""
Unit tests for BarAggregator. Pure logic, no I/O — these run fast and
cover the bucketing/OHLC/flush-timing behavior thoroughly.
"""

from datetime import datetime, timedelta, timezone

from service.ingestion.bar_aggregator import BarAggregator


def _t(seconds_offset: int, base: datetime | None = None) -> datetime:
    base = base or datetime(2026, 7, 18, 14, 30, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=seconds_offset)


def test_mark_is_bid_ask_midpoint():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=102.0, received_at=_t(0))
    ready = agg.pop_ready(_t(200))
    assert len(ready) == 1
    _, _, bucket = ready[0]
    assert bucket.open == 101.0
    assert bucket.close == 101.0
    assert bucket.bid == 100.0
    assert bucket.ask == 102.0


def test_mark_falls_back_to_one_sided_quote():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=None, ask=5.0, received_at=_t(0))
    ready = agg.pop_ready(_t(200))
    assert ready[0][2].close == 5.0


def test_quote_with_neither_side_is_ignored():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=None, ask=None, received_at=_t(0))
    assert agg.pending_count() == 0


def test_ohlc_tracks_high_low_open_close_across_multiple_quotes():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))   # mark 100
    agg.on_quote("SPY", bid=105.0, ask=105.0, received_at=_t(10))  # mark 105 (high)
    agg.on_quote("SPY", bid=95.0, ask=95.0, received_at=_t(20))    # mark 95 (low)
    agg.on_quote("SPY", bid=102.0, ask=102.0, received_at=_t(30))  # mark 102 (close)

    ready = agg.pop_ready(_t(200))
    bucket = ready[0][2]
    assert bucket.open == 100.0
    assert bucket.high == 105.0
    assert bucket.low == 95.0
    assert bucket.close == 102.0


def test_events_in_different_minutes_go_to_different_buckets():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))    # minute 0
    agg.on_quote("SPY", bid=200.0, ask=200.0, received_at=_t(65))   # minute 1

    ready = agg.pop_ready(_t(200))
    assert len(ready) == 2
    closes = {b.close for _, _, b in ready}
    assert closes == {100.0, 200.0}


def test_different_keys_are_independent():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))
    agg.on_quote("QQQ", bid=50.0, ask=50.0, received_at=_t(0))

    ready = agg.pop_ready(_t(200))
    assert len(ready) == 2
    by_key = {k: b for k, _, b in ready}
    assert by_key["SPY"].close == 100.0
    assert by_key["QQQ"].close == 50.0


def test_greeks_and_quotes_merge_into_same_bucket():
    agg = BarAggregator()
    agg.on_quote("SPY_C450", bid=1.0, ask=1.2, received_at=_t(0))
    agg.on_greeks("SPY_C450", delta=0.4, gamma=0.01, theta=-0.05, vega=0.1, rho=0.02, iv=0.35, received_at=_t(5))

    ready = agg.pop_ready(_t(200))
    assert len(ready) == 1
    bucket = ready[0][2]
    assert bucket.close == 1.1  # from the quote
    assert bucket.delta == 0.4  # from the greeks
    assert bucket.has_greeks is True


def test_bucket_with_only_greeks_no_quote_has_no_price():
    agg = BarAggregator()
    agg.on_greeks("SPY_C450", delta=0.4, gamma=0.01, theta=-0.05, vega=0.1, rho=0.02, iv=0.35, received_at=_t(0))
    ready = agg.pop_ready(_t(200))
    bucket = ready[0][2]
    assert bucket.open is None
    assert bucket.has_greeks is True


def test_bucket_not_ready_until_minute_plus_grace_elapsed():
    agg = BarAggregator(flush_grace_seconds=5.0)
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))  # minute epoch = t+0, closes at t+60

    # Not ready yet: only 50s after the bucket's minute started.
    assert agg.pop_ready(_t(50)) == []
    # Not ready yet: minute elapsed (60s) but grace period hasn't.
    assert agg.pop_ready(_t(62)) == []
    # Ready: minute + grace period both elapsed.
    ready = agg.pop_ready(_t(66))
    assert len(ready) == 1


def test_pop_ready_removes_flushed_buckets():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))
    assert agg.pending_count() == 1

    agg.pop_ready(_t(200))
    assert agg.pending_count() == 0
    # Calling again shouldn't re-return the same bucket.
    assert agg.pop_ready(_t(300)) == []


def test_late_event_for_already_flushed_minute_starts_a_new_bucket():
    """If an event for an old minute arrives after that minute's bucket
    was already flushed, it should start a fresh bucket rather than being
    silently dropped or erroring — acceptable, documented behavior (a
    genuinely late/out-of-order event just becomes its own late bar)."""
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))
    agg.pop_ready(_t(200))  # flushes and removes it
    assert agg.pending_count() == 0

    agg.on_quote("SPY", bid=999.0, ask=999.0, received_at=_t(0))  # "late" for the same minute
    assert agg.pending_count() == 1


# --- Pop-before-commit data loss: restore() ---
#
# `pop_ready()` removes buckets from memory. Before `restore()` existed, a
# failed DB write meant those bars were gone for good — nothing re-creates
# a bucket for a minute that has already passed — so the pipeline could
# permanently lose a minute of data. These cover putting them back.


def test_restore_reinstates_popped_buckets_for_retry():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=102.0, received_at=_t(0))
    popped = agg.pop_ready(_t(200))
    assert len(popped) == 1
    assert agg.pending_count() == 0

    agg.restore(popped)

    # Back in memory, and re-emitted by the next pop_ready() call — this is
    # a real retry, not just a way to avoid discarding the data.
    assert agg.pending_count() == 1
    again = agg.pop_ready(_t(300))
    assert len(again) == 1
    assert again[0][0] == "SPY"
    assert again[0][2].close == 101.0


def test_restore_merges_when_the_same_minute_started_accumulating_again():
    """Awaiting a failed write yields control, so more events for that same
    minute can arrive before the restore happens. Merging must preserve
    correct OHLC rather than letting one bucket clobber the other."""
    agg = BarAggregator()
    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))  # mark 100
    popped = agg.pop_ready(_t(200))
    popped_minute = popped[0][1]

    # A new event lands in that same minute while the failed write settles.
    agg.on_quote("SPY", bid=120.0, ask=120.0, received_at=_t(0))
    assert agg.pending_count() == 1

    agg.restore(popped)

    assert agg.pending_count() == 1, "merge, don't leave two buckets for one minute"
    merged = agg.pop_ready(_t(300))[0][2]
    # Earliest open wins, latest close wins, high/low widen to cover both.
    assert merged.open == 100.0
    assert merged.close == 120.0
    assert merged.high == 120.0
    assert merged.low == 100.0
    assert popped_minute is not None


def test_restore_merge_open_is_earliest_tick_not_lowest_price():
    """The companion case to the test above, which only covers a rising
    price. `open` is the first *observed* price of the minute, so a
    falling minute must still open at the earlier, higher price — folding
    it in with min() the way high/low are folded in would report the
    minute's low as its open."""
    agg = BarAggregator()
    agg.on_quote("SPY", bid=120.0, ask=120.0, received_at=_t(0))  # mark 120
    popped = agg.pop_ready(_t(200))

    agg.on_quote("SPY", bid=100.0, ask=100.0, received_at=_t(0))  # mark 100
    agg.restore(popped)

    merged = agg.pop_ready(_t(300))[0][2]
    assert merged.open == 120.0, "open is the first tick, not the minimum"
    assert merged.close == 100.0
    assert merged.high == 120.0
    assert merged.low == 100.0


def test_restore_merge_keeps_greeks_from_either_side():
    agg = BarAggregator()
    agg.on_greeks("SPY", 0.1, 0.01, -0.1, 0.2, 0.3, 0.4, received_at=_t(0))
    popped = agg.pop_ready(_t(200))

    agg.on_greeks("SPY", 0.9, 0.02, -0.2, 0.3, 0.4, 0.5, received_at=_t(0))
    agg.restore(popped)

    merged = agg.pop_ready(_t(300))[0][2]
    assert merged.has_greeks
    assert merged.delta == 0.9  # the later snapshot wins for Greeks


def test_restore_of_empty_list_is_a_noop():
    agg = BarAggregator()
    agg.restore([])
    assert agg.pending_count() == 0


# --- NaN handling ---
#
# dxfeed uses NaN (not None) for "field present but no value", and the
# SDK's Quote dataclass declares bid_price/ask_price as required Decimal
# fields, so they are never None. Before NaN was normalized, a NaN mark
# could be computed, bucketed, and written into a Numeric column.


def test_nan_bid_and_ask_produce_no_bucket_at_all():
    agg = BarAggregator()
    agg.on_quote("SPY", bid=float("nan"), ask=float("nan"), received_at=_t(0))
    assert agg.pending_count() == 0
    assert agg.pop_ready(_t(200)) == []


def test_nan_on_one_side_falls_back_to_the_real_side():
    """One-sided quoting is normal and should still record — but only from
    the side that actually has a value, not from the NaN one."""
    agg = BarAggregator()
    agg.on_quote("SPY", bid=float("nan"), ask=5.0, received_at=_t(0))
    ready = agg.pop_ready(_t(200))
    assert len(ready) == 1
    assert ready[0][2].close == 5.0


def test_decimal_nan_is_also_treated_as_missing():
    from decimal import Decimal

    agg = BarAggregator()
    agg.on_quote("SPY", bid=Decimal("NaN"), ask=Decimal("NaN"), received_at=_t(0))
    assert agg.pending_count() == 0


def test_a_finite_decimal_quote_is_unaffected():
    from decimal import Decimal

    agg = BarAggregator()
    agg.on_quote("SPY", bid=Decimal("100.0"), ask=Decimal("102.0"), received_at=_t(0))
    _, _, bucket = agg.pop_ready(_t(200))[0]
    assert bucket.close == 101.0
