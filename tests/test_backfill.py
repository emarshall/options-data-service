"""
Unit tests for BackfillJob. Fake source (candle data supplied by the test),
real in-memory SQLite for persistence — the idempotency logic (skip rows
that already exist) is exactly the kind of thing worth testing against a
real engine, not a mock.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

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

    async def get_option_chain(self, ticker):
        return self.chains.get(ticker, {})

    async def snapshot_greeks(self, symbols, timeout_s):
        return {
            s: FakeGreeksSnapshot(event_symbol=s, delta=self.greeks_snapshot[s])
            for s in symbols
            if s in self.greeks_snapshot
        }

    async def request_candles(self, symbol, period, start_time):
        self.candle_requests.append((symbol, period, start_time))
        return self.candles.get(symbol, [])


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

    async def failing_request_candles(symbol, period, start_time):
        if symbol == "SPY_BAD":
            raise RuntimeError("simulated failure")
        return source.candles.get(symbol, [])

    source.request_candles = failing_request_candles

    job = BackfillJob(source, db_session_factory, _settings())
    summary = await job.run()

    assert summary["contracts_failed"] == 1
    assert summary["option_bars_written"] == 1  # the good one still got written


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

    assert result == {"ticker": "SPY", "gaps_found": 0, "gaps_reconciled": 0, "bars_written": 0}
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
