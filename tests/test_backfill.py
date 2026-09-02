"""
Unit tests for BackfillJob. Fake source (candle data supplied by the test),
real in-memory SQLite for persistence — the idempotency logic (skip rows
that already exist) is exactly the kind of thing worth testing against a
real engine, not a mock.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import logging

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.config.settings import AppConfig, TickerConfig
from service.db.models import Base, Contract, OptionBar1m, UnderlyingBar1m
from service.ingestion.backfill import BackfillJob


@dataclass
class FakeOption:
    streamer_symbol: str
    expiration_date: date
    strike_price: float
    option_type: str
    settlement_type: str = "PM"
    days_to_expiration: int = 10


@dataclass
class FakeGreeksSnapshot:
    event_symbol: str
    delta: float


class FakeCandle:
    def __init__(self, time_ms, open=1.0, high=1.5, low=0.9, close=1.2,
                 volume=100, open_interest=500, vwap=1.1, bid_volume=40,
                 ask_volume=60, imp_volatility=0.35):
        self.time = time_ms
        self.open, self.high, self.low, self.close = open, high, low, close
        self.volume, self.open_interest, self.vwap = volume, open_interest, vwap
        self.bid_volume, self.ask_volume = bid_volume, ask_volume
        self.imp_volatility = imp_volatility


class FakeSource:
    def __init__(self):
        self.chains: dict[str, dict] = {}
        self.greeks_snapshot: dict[str, float] = {}
        self.candles: dict[str, list] = {}  # symbol -> list of FakeCandle
        self.candle_requests: list[tuple[str, str, datetime]] = []
        self.candle_request_kwargs: list[dict] = []

    async def get_option_chain(self, ticker):
        return self.chains.get(ticker, {})

    async def snapshot_greeks(self, symbols, timeout_s):
        return {
            s: FakeGreeksSnapshot(event_symbol=s, delta=self.greeks_snapshot[s])
            for s in symbols
            if s in self.greeks_snapshot
        }

    async def request_candles(self, symbol, period, start_time, **kwargs):
        self.candle_requests.append((symbol, period, start_time))
        self.candle_request_kwargs.append(kwargs)
        result = self.candles.get(symbol, [])
        # Mirror the real request_candles' diagnostics population (see
        # service/sources/tastytrade.py) closely enough for report tests
        # to exercise realistic-shaped data without needing the real
        # TastyTrade SDK.
        diag = kwargs.get("diagnostics")
        if diag is not None:
            times = [
                datetime.fromtimestamp(c.time / 1000, tz=timezone.utc)
                for c in result if getattr(c, "time", None) is not None
            ]
            diag.update({
                "stop_reason": "repeated_key" if result else "idle_timeout",
                "elapsed_s": 0.01,
                "event_count": len(result),
                "symbol": symbol,
                "requested_start": start_time.isoformat(),
                "earliest_candle": min(times).isoformat() if times else None,
                "latest_candle": max(times).isoformat() if times else None,
            })
        return result


@pytest.fixture
async def db_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _settings(**ticker_overrides) -> AppConfig:
    defaults = dict(
        ticker="SPY", call_delta_min=0.15, call_delta_max=0.85,
        put_delta_min=-0.85, put_delta_max=-0.15, max_days_to_expiration=45,
        capture_underlying_bars=True,
    )
    defaults.update(ticker_overrides)
    return AppConfig(tickers=[TickerConfig(**defaults)])


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


@pytest.mark.asyncio
async def test_backfills_option_bars_with_iv_and_volume_but_no_greeks(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}

    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    source.candles["SPY_C450"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["option_bars_written"] == 1
    assert summary["contracts_failed"] == 0

    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_C450"})
        assert row is not None
        assert float(row.close) == 1.2
        assert float(row.iv) == 0.35
        assert row.volume == 100
        assert row.open_interest == 500
        # Deliberately NULL — see backfill.py module docstring.
        assert row.delta is None
        assert row.greeks_source is None


@pytest.mark.asyncio
async def test_backfills_underlying_bars(db_session_factory):
    source = FakeSource()
    source.chains["SPY"] = {}  # no option contracts, just testing underlying path
    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    source.candles["SPY"] = [FakeCandle(_ms(bar_time), close=440.5, volume=10000)]

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["underlying_bars_written"] == 1

    async with db_session_factory() as session:
        row = await session.get(UnderlyingBar1m, {"time": bar_time, "ticker": "SPY"})
        assert row is not None
        assert float(row.close) == 440.5
        assert row.volume == 10000


@pytest.mark.asyncio
async def test_does_not_backfill_underlying_when_disabled(db_session_factory):
    source = FakeSource()
    source.chains["SPY"] = {}
    source.candles["SPY"] = [FakeCandle(_ms(datetime(2026, 7, 1, tzinfo=timezone.utc)))]

    job = BackfillJob(source, db_session_factory, _settings(capture_underlying_bars=False))
    summary = await job.run()

    assert summary["underlyings_attempted"] == 0
    assert summary["underlying_bars_written"] == 0


@pytest.mark.asyncio
async def test_skips_rows_that_already_exist_does_not_overwrite(db_session_factory):
    """Core idempotency guarantee: a pre-existing row (e.g. from live
    ingestion) must be left completely untouched, not overwritten."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}

    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    source.candles["SPY_C450"] = [FakeCandle(_ms(bar_time), close=999.0)]  # would overwrite to 999 if allowed

    from service.db.models import OptionRight

    async with db_session_factory() as session:
        session.add(
            OptionBar1m(
                time=bar_time, contract_id="SPY_C450", underlying_ticker="SPY",
                expiration_date=exp, strike=450.0, right=OptionRight.CALL,
                close=1.23, delta=0.55,  # simulates a live-ingested row
            )
        )
        await session.commit()

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["option_bars_written"] == 0  # nothing new written, row already existed

    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_C450"})
        assert float(row.close) == 1.23  # untouched, NOT overwritten to 999
        assert float(row.delta) == 0.55  # live Greeks preserved


@pytest.mark.asyncio
async def test_rerunning_backfill_is_idempotent(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}
    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    source.candles["SPY_C450"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings())
    summary1 = await job.run()
    summary2 = await job.run()

    assert summary1["option_bars_written"] == 1
    assert summary2["option_bars_written"] == 0  # already there, nothing new

    async with db_session_factory() as session:
        result = await session.execute(select(OptionBar1m))
        assert len(result.scalars().all()) == 1  # not duplicated


@pytest.mark.asyncio
async def test_lookback_window_passed_to_request_candles(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}

    job = BackfillJob(source, db_session_factory, _settings(), lookback_days=60)
    await job.run()

    calls = [c for c in source.candle_requests if c[0] == "SPY_C450"]
    assert len(calls) == 1
    _, period, start_time = calls[0]
    assert period == "1m"
    expected = datetime.now(timezone.utc) - timedelta(days=60)
    assert abs((start_time - expected).total_seconds()) < 5


@pytest.mark.asyncio
async def test_recovers_contract_that_expired_during_downtime(db_session_factory):
    """Core scenario the fix addresses: a contract was tracked before an
    outage, fully expired during it (so it's no longer live-resolvable —
    dropped from the chain, no live Greeks), but its candle history should
    still get backfilled since it's already known to the `contracts` table."""
    from service.db.models import OptionRight

    exp = date.today() - timedelta(days=2)  # already expired

    # Simulate it having been tracked before the outage: a row already
    # exists in `contracts`, but the live chain no longer returns it (as if
    # it expired and dropped off) — this is exactly what ContractManager
    # alone can't recover from.
    async with db_session_factory() as session:
        session.add(
            Contract(
                contract_id="SPY_EXPIRED", underlying_ticker="SPY",
                expiration_date=exp, strike=440.0, right=OptionRight.CALL,
                settlement_type=None,
                first_seen=datetime.now(timezone.utc) - timedelta(days=5),
                last_seen=datetime.now(timezone.utc) - timedelta(days=2),
            )
        )
        await session.commit()

    source = FakeSource()
    source.chains["SPY"] = {}  # current chain no longer includes it — "expired"
    bar_time = datetime.combine(exp, datetime.min.time(), tzinfo=timezone.utc) + timedelta(hours=14, minutes=30)
    source.candles["SPY_EXPIRED"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["contracts_recovered_from_db"] == 1
    assert summary["option_bars_written"] == 1

    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_EXPIRED"})
        assert row is not None
        assert row.underlying_ticker == "SPY"
        assert float(row.strike) == 440.0


@pytest.mark.asyncio
async def test_does_not_recover_contracts_expired_before_lookback_window(db_session_factory):
    """A contract that expired long before the lookback window (e.g. months
    ago) shouldn't be pulled in as a candidate — its candle data wouldn't
    be in TastyTrade's retention window anyway, so there's no point
    attempting it."""
    from service.db.models import OptionRight

    long_ago = date.today() - timedelta(days=200)

    async with db_session_factory() as session:
        session.add(
            Contract(
                contract_id="SPY_ANCIENT", underlying_ticker="SPY",
                expiration_date=long_ago, strike=400.0, right=OptionRight.CALL,
                first_seen=datetime.now(timezone.utc) - timedelta(days=205),
                last_seen=datetime.now(timezone.utc) - timedelta(days=200),
            )
        )
        await session.commit()

    source = FakeSource()
    source.chains["SPY"] = {}

    job = BackfillJob(source, db_session_factory, _settings(), lookback_days=60)
    summary = await job.run()

    assert summary["contracts_recovered_from_db"] == 0
    assert "SPY_ANCIENT" not in {req[0] for req in source.candle_requests}


@pytest.mark.asyncio
async def test_live_resolved_contract_not_double_counted_as_recovered(db_session_factory):
    """A contract that's still live-resolvable shouldn't also show up as
    'recovered from DB' — the DB-sourced set should only add genuinely new
    ones, not double-count what ContractManager already found."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_LIVE", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_LIVE": 0.5}
    source.candles["SPY_LIVE"] = [FakeCandle(_ms(datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)))]

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["contracts_recovered_from_db"] == 0
    assert summary["contracts_attempted"] == 1


@pytest.mark.asyncio
async def test_one_contract_failing_does_not_block_the_rest(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {
        exp: [
            FakeOption("SPY_GOOD", exp, 450.0, "C", days_to_expiration=10),
            FakeOption("SPY_BAD", exp, 451.0, "C", days_to_expiration=10),
        ]
    }
    source.greeks_snapshot = {"SPY_GOOD": 0.5, "SPY_BAD": 0.5}
    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    source.candles["SPY_GOOD"] = [FakeCandle(_ms(bar_time))]

    async def failing_request_candles(symbol, period, start_time, **kwargs):
        if symbol == "SPY_BAD":
            raise RuntimeError("simulated failure")
        return source.candles.get(symbol, [])

    source.request_candles = failing_request_candles

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["contracts_failed"] == 1
    assert summary["option_bars_written"] == 1  # the good one still got written


@pytest.mark.asyncio
async def test_all_contracts_processed_under_bounded_concurrency(db_session_factory):
    """Regression test for the real "backfill taking hours" report (see
    PLAN.md Section 7): option contracts are now backfilled concurrently,
    bounded by contract_concurrency. This confirms the concurrency
    plumbing (asyncio.gather + semaphore) doesn't silently drop or
    double-count any contract — every one of several contracts, run with
    a concurrency limit smaller than the total count, gets processed
    exactly once."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    n_contracts = 12
    source.chains["SPY"] = {
        exp: [
            FakeOption(f"SPY_C{450 + i}", exp, 450.0 + i, "C", days_to_expiration=10)
            for i in range(n_contracts)
        ]
    }
    source.greeks_snapshot = {f"SPY_C{450 + i}": 0.5 for i in range(n_contracts)}
    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    for i in range(n_contracts):
        source.candles[f"SPY_C{450 + i}"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings(), contract_concurrency=3)
    summary = await job.run()

    assert summary["contracts_attempted"] == n_contracts
    assert summary["contracts_failed"] == 0
    assert summary["option_bars_written"] == n_contracts  # exactly one bar per contract, no dupes/drops

    async with db_session_factory() as session:
        rows = (await session.execute(select(OptionBar1m))).scalars().all()
    assert len(rows) == n_contracts
    assert {r.contract_id for r in rows} == {f"SPY_C{450 + i}" for i in range(n_contracts)}


@pytest.mark.asyncio
async def test_multiple_contract_failures_are_all_isolated_under_concurrency(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {
        exp: [FakeOption(f"SPY_C{450 + i}", exp, 450.0 + i, "C", days_to_expiration=10) for i in range(8)]
    }
    source.greeks_snapshot = {f"SPY_C{450 + i}": 0.5 for i in range(8)}
    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    for i in range(8):
        source.candles[f"SPY_C{450 + i}"] = [FakeCandle(_ms(bar_time))]

    failing = {"SPY_C451", "SPY_C453", "SPY_C455"}

    async def flaky_request_candles(symbol, period, start_time, **kwargs):
        if symbol in failing:
            raise RuntimeError("simulated failure")
        return source.candles.get(symbol, [])

    source.request_candles = flaky_request_candles

    job = BackfillJob(source, db_session_factory, _settings(), contract_concurrency=4)
    summary = await job.run()

    assert summary["contracts_failed"] == len(failing)
    assert summary["option_bars_written"] == 8 - len(failing)


# --- Incremental start time (efficiency fix) ---


@pytest.mark.asyncio
async def test_second_run_requests_from_latest_bar_not_full_lookback(db_session_factory):
    """The efficiency fix this is testing: once a contract has data on
    disk, a subsequent run should ask the source for candles starting
    from its latest known bar, not the full lookback window all over
    again."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}
    bar_time = datetime.now(timezone.utc) - timedelta(days=5)
    source.candles["SPY_C450"] = [FakeCandle(_ms(bar_time))]

    # capture_underlying_bars=False here specifically so this test only has
    # to reason about the one option contract's request count, not also the
    # separate underlying-ticker request that _settings()'s default would add.
    job = BackfillJob(
        source, db_session_factory, _settings(capture_underlying_bars=False), lookback_days=60
    )
    await job.run()  # first run: full lookback, writes the one bar

    calls_before = len([c for c in source.candle_requests if c[0] == "SPY_C450"])
    await job.run()  # second run: should request starting from bar_time, not 60 days ago
    option_calls_after = [c for c in source.candle_requests if c[0] == "SPY_C450"]

    assert len(option_calls_after) == calls_before + 1
    second_start_time = option_calls_after[-1][2]
    # Should be much closer to bar_time (5 days ago) than to the 60-day lookback start.
    assert abs((second_start_time - bar_time).total_seconds()) < 5


@pytest.mark.asyncio
async def test_full_rescan_flag_forces_full_lookback_every_run(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}
    bar_time = datetime.now(timezone.utc) - timedelta(days=5)
    source.candles["SPY_C450"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings(), lookback_days=60, full_rescan=True)
    await job.run()
    await job.run()

    lookback_start = datetime.now(timezone.utc) - timedelta(days=60)
    _, _, second_start_time = source.candle_requests[-1]
    assert abs((second_start_time - lookback_start).total_seconds()) < 5


@pytest.mark.asyncio
async def test_underlying_also_gets_incremental_start_time(db_session_factory):
    source = FakeSource()
    bar_time = datetime.now(timezone.utc) - timedelta(days=3)
    source.candles["SPY"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings(), lookback_days=60)
    await job.run()
    await job.run()

    underlying_calls = [c for c in source.candle_requests if c[0] == "SPY"]
    assert len(underlying_calls) == 2
    assert abs((underlying_calls[-1][2] - bar_time).total_seconds()) < 5


# --- idle_timeout_s/timeout_s wiring (underlying gets a longer tolerance) ---


@pytest.mark.asyncio
async def test_underlying_backfill_requests_a_longer_idle_timeout_than_options(db_session_factory):
    """Covers the fix for a real follow-up report: gap reconciliation over
    a multi-week span for a dense underlying still wasn't recovering all
    of it, even after the max_count/timeout_s fix — most likely because
    the (correctly short, for options) default idle_timeout_s was cutting
    off a large historical replay during a normal server-side batching
    pause. Underlying requests should ask for a longer idle tolerance;
    option contract requests should not (they're genuinely sparse, and a
    long idle timeout there would just slow down every contract's
    backfill for no benefit)."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]}
    source.greeks_snapshot = {"SPY_C450": 0.5}

    job = BackfillJob(source, db_session_factory, _settings())
    await job.run()

    option_kwargs = source.candle_request_kwargs[
        [c[0] for c in source.candle_requests].index("SPY_C450")
    ]
    underlying_kwargs = source.candle_request_kwargs[
        [c[0] for c in source.candle_requests].index("SPY")
    ]

    assert "idle_timeout_s" not in option_kwargs  # default (short) — not explicitly overridden
    assert underlying_kwargs["idle_timeout_s"] == BackfillJob._UNDERLYING_IDLE_TIMEOUT_S
    assert underlying_kwargs["idle_timeout_s"] > 3.0
    assert underlying_kwargs["timeout_s"] == BackfillJob._UNDERLYING_TIMEOUT_S


# --- Truncation-detection diagnostic ---


@pytest.mark.asyncio
async def test_warns_when_result_starts_well_after_requested_start(db_session_factory, caplog):
    """If the earliest candle actually received is much later than what
    was requested, that's most likely a truncated result (request_candles
    hit one of its internal limits), not genuinely-missing data — this
    should be logged loudly rather than silently accepted, since silent
    partial results are exactly what caused the original underlying-
    backfill bug to go unnoticed for a while."""
    source = FakeSource()
    requested_start = datetime.now(timezone.utc) - timedelta(days=40)
    actual_earliest = datetime.now(timezone.utc) - timedelta(days=5)  # 35 days later than requested
    source.candles["SPY"] = [FakeCandle(_ms(actual_earliest))]

    job = BackfillJob(source, db_session_factory, _settings())
    with caplog.at_level(logging.WARNING):
        await job._backfill_underlying("SPY", requested_start)

    assert any("truncated" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_no_truncation_warning_when_result_starts_near_requested_start(db_session_factory, caplog):
    source = FakeSource()
    requested_start = datetime.now(timezone.utc) - timedelta(days=40)
    actual_earliest = requested_start + timedelta(hours=1)  # normal — first trading minute after start
    source.candles["SPY"] = [FakeCandle(_ms(actual_earliest))]

    job = BackfillJob(source, db_session_factory, _settings())
    with caplog.at_level(logging.WARNING):
        await job._backfill_underlying("SPY", requested_start)

    assert not any("truncated" in r.message for r in caplog.records)


# --- Underlying gap reconciliation ---


@pytest.mark.asyncio
async def test_reconcile_underlying_gaps_reports_none_when_fully_covered(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=1)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    result = await job.reconcile_underlying_gaps("SPY", start, end)

    assert result["gaps_found"] == 0
    assert result["gaps_reconciled"] == 0
    assert result["bars_written"] == 0
    assert result["requested_start_before_retention"] is False
    assert source.candle_requests == []  # nothing requested — no gap, nothing to do


@pytest.mark.asyncio
async def test_reconcile_underlying_gaps_backfills_from_earliest_gap(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)
    assert len(minutes) > 20
    midpoint = minutes[len(minutes) // 2]

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        # Only the first half is present — a real gap starting at the midpoint.
        for m in minutes[: len(minutes) // 2]:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    source.candles["SPY"] = [FakeCandle(_ms(midpoint))]  # what the "server" has for the gap

    result = await job.reconcile_underlying_gaps("SPY", start, end)

    assert result["gaps_found"] >= 1
    assert result["bars_written"] == 1
    calls = [c for c in source.candle_requests if c[0] == "SPY"]
    assert len(calls) == 1
    assert abs((calls[0][2] - midpoint).total_seconds()) < 5  # requested from the gap, not `start`


# --- Retention-aware gap scanning (fixes a real false-alarm report) ---


@pytest.mark.asyncio
async def test_reconcile_clips_scan_start_to_retention_window(db_session_factory):
    """The actual bug report this covers: a user ran gap-reconcile and it
    requested candles starting well before TastyTrade's known retention
    window, then logged a spurious 'truncated' warning when the response
    (correctly) started at the retention edge instead. `start` older than
    retention should be silently raised to the retention cutoff, not
    treated as a real, actionable gap."""
    from service.ingestion.backfill import RETENTION_DAYS
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=RETENTION_DAYS + 20)  # well before retention
    end = now
    retention_cutoff = now - timedelta(days=RETENTION_DAYS)

    # Only the last day is present — everything else (including everything
    # before the retention cutoff, which could never have data anyway) is
    # "missing," but only the portion since the retention cutoff is a real,
    # actionable gap.
    minutes = expected_bar_minutes(retention_cutoff, end)
    present_from = end - timedelta(days=1)

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes:
            if m >= present_from:
                session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    result = await job.reconcile_underlying_gaps("SPY", start, end)

    assert result["requested_start_before_retention"] is True
    assert result["scan_start"] >= retention_cutoff - timedelta(seconds=5)
    # The one real gap requested should start at/after the retention cutoff, not `start`.
    calls = [c for c in source.candle_requests if c[0] == "SPY"]
    assert len(calls) == 1
    assert calls[0][2] >= retention_cutoff - timedelta(seconds=5)


@pytest.mark.asyncio
async def test_reconcile_does_not_clip_when_start_is_within_retention(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    result = await job.reconcile_underlying_gaps("SPY", start, end)

    assert result["requested_start_before_retention"] is False
    assert abs((result["scan_start"] - start).total_seconds()) < 5


@pytest.mark.asyncio
async def test_no_truncation_warning_when_data_starts_at_retention_edge(db_session_factory, caplog):
    """The other half of the same bug: even without going through
    reconcile_underlying_gaps, a plain request whose true earliest
    available data lands right at the retention boundary (because
    `requested_start` was further back than retention, which is normal
    and expected for `run()`'s harmless over-requesting) should not be
    flagged as truncated."""
    from service.ingestion.backfill import RETENTION_DAYS

    source = FakeSource()
    now = datetime.now(timezone.utc)
    requested_start = now - timedelta(days=RETENTION_DAYS + 20)  # well before retention, as usual
    actual_earliest = now - timedelta(days=RETENTION_DAYS) + timedelta(hours=2)  # right at the edge
    source.candles["SPY"] = [FakeCandle(_ms(actual_earliest))]

    job = BackfillJob(source, db_session_factory, _settings())
    with caplog.at_level(logging.WARNING):
        await job._backfill_underlying("SPY", requested_start)

    assert not any("truncated" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_warns_when_result_ends_well_before_now(db_session_factory, caplog):
    """The other real failure mode this diagnostic exists to catch (see
    PLAN.md Section 7): a request cut off by timeout_s before catching up
    to live looks completely fine on the earliest side while being
    silently short on the recent end. A single candle whose time is the
    only thing far from 'now' should trigger this specific warning."""
    source = FakeSource()
    now = datetime.now(timezone.utc)
    requested_start = now - timedelta(days=5)
    stale_latest = now - timedelta(days=3)  # far short of "now" — the request stopped early
    source.candles["SPY"] = [FakeCandle(_ms(stale_latest))]

    job = BackfillJob(source, db_session_factory, _settings())
    with caplog.at_level(logging.WARNING):
        await job._backfill_underlying("SPY", requested_start)

    assert any("cut off" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_no_cutoff_warning_when_latest_bar_is_near_now(db_session_factory, caplog):
    source = FakeSource()
    now = datetime.now(timezone.utc)
    requested_start = now - timedelta(days=5)
    recent_latest = now - timedelta(minutes=5)
    source.candles["SPY"] = [FakeCandle(_ms(recent_latest))]

    job = BackfillJob(source, db_session_factory, _settings())
    with caplog.at_level(logging.WARNING):
        await job._backfill_underlying("SPY", requested_start)

    assert not any("cut off" in r.message for r in caplog.records)


# --- Diagnostic report (BackfillJob.run(report_path=...), reconcile CLI aggregation) ---


@pytest.mark.asyncio
async def test_run_writes_report_with_underlying_diagnostics(db_session_factory, tmp_path):
    source = FakeSource()
    bar_time = datetime.now(timezone.utc) - timedelta(days=1)
    source.candles["SPY"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings())
    report_prefix = str(tmp_path / "myreport")
    await job.run(report_path=report_prefix)

    json_path = tmp_path / "myreport.json"
    md_path = tmp_path / "myreport.md"
    assert json_path.exists()
    assert md_path.exists()

    import json

    report = json.loads(json_path.read_text())
    assert report["run_type"] == "backfill"
    assert "summary" in report
    assert len(report["underlyings"]) == 1
    underlying_diag = report["underlyings"][0]
    assert underlying_diag["ticker"] == "SPY"
    assert underlying_diag["bars_written"] == 1
    assert underlying_diag["stop_reason"] == "repeated_key"

    md_text = md_path.read_text()
    assert "SPY" in md_text
    assert "Summary" in md_text


@pytest.mark.asyncio
async def test_run_without_report_path_writes_nothing(db_session_factory, tmp_path):
    source = FakeSource()
    bar_time = datetime.now(timezone.utc) - timedelta(days=1)
    source.candles["SPY"] = [FakeCandle(_ms(bar_time))]

    job = BackfillJob(source, db_session_factory, _settings())
    await job.run()  # no report_path — default behavior unaffected

    assert list(tmp_path.iterdir()) == []  # nothing written anywhere unexpected


@pytest.mark.asyncio
async def test_run_report_includes_only_option_contracts_with_warnings_or_failures(
    db_session_factory, tmp_path
):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {
        exp: [
            FakeOption("SPY_GOOD", exp, 450.0, "C", days_to_expiration=10),
            FakeOption("SPY_BAD", exp, 451.0, "C", days_to_expiration=10),
        ]
    }
    source.greeks_snapshot = {"SPY_GOOD": 0.5, "SPY_BAD": 0.5}
    bar_time = datetime.now(timezone.utc) - timedelta(hours=1)  # recent — no truncation warning
    source.candles["SPY_GOOD"] = [FakeCandle(_ms(bar_time))]

    async def flaky(symbol, period, start_time, **kwargs):
        if symbol == "SPY_BAD":
            raise RuntimeError("boom")
        return source.candles.get(symbol, [])

    source.request_candles = flaky

    job = BackfillJob(
        source, db_session_factory, _settings(capture_underlying_bars=False), lookback_days=1
    )
    report_prefix = str(tmp_path / "r")
    await job.run(report_path=report_prefix)

    import json

    report = json.loads((tmp_path / "r.json").read_text())
    contract_ids = {d["contract_id"] for d in report["option_contracts_of_interest"]}
    assert contract_ids == {"SPY_BAD"}  # the good, unremarkable contract isn't listed individually
    bad_entry = next(d for d in report["option_contracts_of_interest"] if d["contract_id"] == "SPY_BAD")
    assert "boom" in bad_entry["error"]


@pytest.mark.asyncio
async def test_report_write_failure_does_not_break_run(db_session_factory, monkeypatch, caplog):
    """A diagnostic aid failing to write shouldn't fail the actual backfill
    — best-effort, logged not raised."""
    source = FakeSource()
    job = BackfillJob(source, db_session_factory, _settings(capture_underlying_bars=False))

    with caplog.at_level(logging.ERROR):
        # An invalid path (embedded null byte) reliably fails Path operations
        # cross-platform without depending on filesystem permissions.
        await job.run(report_path="/nonexistent-dir-xyz\0/report")

    assert any("Failed to write diagnostic report" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_reconcile_underlying_gaps_collect_diagnostics(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)
    midpoint = minutes[len(minutes) // 2]

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes[: len(minutes) // 2]:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    source.candles["SPY"] = [FakeCandle(_ms(midpoint))]

    result = await job.reconcile_underlying_gaps("SPY", start, end, collect_diagnostics=True)

    assert "request_diagnostics" in result
    assert result["request_diagnostics"]["stop_reason"] == "repeated_key"
    assert result["gaps_preview"] is not None
    assert len(result["gaps_preview"]) >= 1
    assert result["gaps_preview"][0]["minutes"] >= 1


@pytest.mark.asyncio
async def test_reconcile_underlying_gaps_no_diagnostics_by_default(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)
    midpoint = minutes[len(minutes) // 2]

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes[: len(minutes) // 2]:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    source.candles["SPY"] = [FakeCandle(_ms(midpoint))]

    result = await job.reconcile_underlying_gaps("SPY", start, end)

    assert "request_diagnostics" not in result
    assert result["gaps_preview"] is None


# --- New diagnostics: skipped_existing / skipped_no_time_field / distinct_days_in_result ---
# Added after real report data showed a huge event_count (8002) vs a tiny bars_written (9) for a
# gap-reconcile run, with no way to tell whether that gap was "legitimately mostly duplicates" or
# something else — see PLAN.md Section 7.


@pytest.mark.asyncio
async def test_diagnostics_report_skipped_existing_count(db_session_factory):
    # A "clean" (zero-microsecond) timestamp — datetime.now() has
    # sub-millisecond precision that a real ms-since-epoch round trip
    # would truncate anyway (see _candle_time), which would make this
    # test's own pre-populated row fail to match by a few microseconds
    # for reasons unrelated to what's actually being tested here.
    bar_time = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    source = FakeSource()
    source.candles["SPY"] = [FakeCandle(_ms(bar_time)), FakeCandle(_ms(bar_time + timedelta(minutes=1)))]

    job = BackfillJob(source, db_session_factory, _settings())
    # Pre-populate one of the two candles so the second run sees it as already existing.
    async with db_session_factory() as session:
        session.add(UnderlyingBar1m(time=bar_time, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    diag: dict = {}
    written = await job._backfill_underlying("SPY", bar_time, diagnostics=diag)

    assert written == 1
    assert diag["skipped_existing"] == 1
    assert diag["skipped_no_time_field"] == 0
    assert diag["distinct_days_in_result"] == 1  # both candles land on the same calendar day


@pytest.mark.asyncio
async def test_diagnostics_report_skipped_no_time_field_count(db_session_factory):
    bar_time = datetime.now(timezone.utc) - timedelta(days=1)
    source = FakeSource()
    source.candles["SPY"] = [FakeCandle(None), FakeCandle(_ms(bar_time))]  # first has no usable time

    job = BackfillJob(source, db_session_factory, _settings())
    diag: dict = {}
    written = await job._backfill_underlying("SPY", bar_time, diagnostics=diag)

    assert written == 1
    assert diag["skipped_existing"] == 0
    assert diag["skipped_no_time_field"] == 1


@pytest.mark.asyncio
async def test_diagnostics_report_distinct_days_spans_multiple_days(db_session_factory):
    source = FakeSource()
    day1 = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    day2 = datetime(2026, 7, 5, 14, 30, tzinfo=timezone.utc)
    day3 = datetime(2026, 7, 10, 14, 30, tzinfo=timezone.utc)
    source.candles["SPY"] = [FakeCandle(_ms(t)) for t in (day1, day2, day3)]

    job = BackfillJob(source, db_session_factory, _settings())
    diag: dict = {}
    await job._backfill_underlying("SPY", day1, diagnostics=diag)

    assert diag["distinct_days_in_result"] == 3


@pytest.mark.asyncio
async def test_diagnostics_flow_into_gap_reconcile_report(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)
    midpoint = minutes[len(minutes) // 2]

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes[: len(minutes) // 2]:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    source.candles["SPY"] = [FakeCandle(_ms(midpoint)), FakeCandle(_ms(minutes[0]))]  # one new, one pre-existing

    result = await job.reconcile_underlying_gaps("SPY", start, end, collect_diagnostics=True)

    rd = result["request_diagnostics"]
    assert "skipped_existing" in rd
    assert "skipped_no_time_field" in rd
    assert "distinct_days_in_result" in rd


# --- candles_per_day density diagnostic ---
# Added after distinct_days_in_result alone wasn't enough to explain a real report where it stayed
# identical (22/19/12) across two runs — one that stopped in ~1s, one that patiently rode out ~650s
# more (after the recency-gating fix) — while event_count grew substantially in between. That meant
# whatever extra data arrived landed in days already represented, not new ones, and the only way to
# actually see whether an old date has real dense coverage or just a stray boundary-marker event is a
# per-day count. See PLAN.md Section 7.


@pytest.mark.asyncio
async def test_candles_per_day_reflects_density_not_just_presence(db_session_factory):
    source = FakeSource()
    sparse_day = datetime(2026, 7, 7, 14, 56, tzinfo=timezone.utc)  # one lone event
    dense_day = datetime(2026, 8, 15, 14, 0, tzinfo=timezone.utc)  # many events, same day

    candles = [FakeCandle(_ms(sparse_day))]
    candles += [FakeCandle(_ms(dense_day + timedelta(minutes=i))) for i in range(50)]
    source.candles["SPY"] = candles

    job = BackfillJob(source, db_session_factory, _settings())
    diag: dict = {}
    await job._backfill_underlying("SPY", sparse_day, diagnostics=diag)

    per_day = diag["candles_per_day"]
    assert per_day[sparse_day.date().isoformat()] == 1
    assert per_day[dense_day.date().isoformat()] == 50
    assert diag["distinct_days_in_result"] == 2  # same count either way — density is the new signal


@pytest.mark.asyncio
async def test_candles_per_day_flows_into_gap_reconcile_report(db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    source = FakeSource()
    start = datetime.now(timezone.utc) - timedelta(days=2)
    end = datetime.now(timezone.utc)
    minutes = expected_bar_minutes(start, end)
    midpoint = minutes[len(minutes) // 2]

    job = BackfillJob(source, db_session_factory, _settings())
    async with db_session_factory() as session:
        for m in minutes[: len(minutes) // 2]:
            session.add(UnderlyingBar1m(time=m, ticker="SPY", open=1, high=1, low=1, close=1))
        await session.commit()

    source.candles["SPY"] = [FakeCandle(_ms(midpoint))]

    result = await job.reconcile_underlying_gaps("SPY", start, end, collect_diagnostics=True)

    assert "candles_per_day" in result["request_diagnostics"]
    assert sum(result["request_diagnostics"]["candles_per_day"].values()) >= 1


def test_report_to_markdown_includes_candles_per_day_section():
    from service.ingestion.backfill import _report_to_markdown

    report = {
        "run_type": "gap_reconcile",
        "started_at": "2026-08-19T00:00:00+00:00",
        "finished_at": "2026-08-19T00:01:00+00:00",
        "elapsed_s": 60.0,
        "tickers": [
            {
                "ticker": "SPX", "scan_start": "x", "scan_end": "y", "gaps_found": 1, "bars_written": 2,
                "requested_start_before_retention": False,
                "request_diagnostics": {
                    "stop_reason": "repeated_key", "elapsed_s": 1.0, "event_count": 3,
                    "distinct_days_in_result": 2, "skipped_existing": 1, "skipped_no_time_field": 0,
                    "hit_max_count": False, "earliest_candle": "e", "latest_candle": "l",
                    "candles_per_day": {"2026-07-07": 1, "2026-08-15": 2},
                },
            }
        ],
    }

    md = _report_to_markdown(report)

    assert "Candles per day for SPX" in md
    assert "2026-07-07: 1" in md
    assert "2026-08-15: 2" in md
