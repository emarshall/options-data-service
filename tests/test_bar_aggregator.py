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
