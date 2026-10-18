"""
Integration tests for the query API. Real HTTP requests (via httpx's
ASGITransport, no actual network) against the real FastAPI app, backed by
a real in-memory SQLite DB with actual rows — not mocked at any layer
except swapping the DB session dependency, which is exactly what
service/api/deps.py's get_session() was structured to make easy.
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.util import await_only

from service.api.deps import get_session
from service.api.main import app
from service.db.models import Base, Contract, OptionBar1m, OptionRight, UnderlyingBar1m


# --- SQLite shims for TimescaleDB's aggregation functions -----------------
#
# The query API aggregates option/underlying bars on the fly with
# TimescaleDB's time_bucket()/first()/last() (see service/db/aggregations.py).
# SQLite has none of those, so the test engine gets Python shims registered
# on every connection — enough to exercise the real route/query end-to-end
# against seeded 1m rows. Real TimescaleDB aggregation is what production
# runs; this is purely so the dispatch + aggregation logic is testable here.

_INTERVAL_SECONDS = {
    "5 minutes": 300,
    "15 minutes": 900,
    "30 minutes": 1800,
    "1 hour": 3600,
    "1 day": 86400,
    "1 week": 604800,
}
_SQLITE_DT_FMT = "%Y-%m-%d %H:%M:%S.%f"


def _parse_sqlite_dt(value):
    if isinstance(value, datetime):
        return value
    return datetime.strptime(str(value), _SQLITE_DT_FMT)


def _time_bucket(interval, value):
    if value is None:
        return None
    seconds = _INTERVAL_SECONDS[interval]
    epoch = datetime(1970, 1, 1)
    floored = int((_parse_sqlite_dt(value) - epoch).total_seconds()) // seconds * seconds
    return (epoch + timedelta(seconds=floored)).strftime(_SQLITE_DT_FMT)


class _FirstValue:
    """Aggregate picking the value whose companion timestamp is earliest."""

    def __init__(self):
        self._time = None
        self._value = None

    def step(self, value, time):
        if time is not None and (self._time is None or time < self._time):
            self._time = time
            self._value = value

    def finalize(self):
        return self._value


class _LastValue(_FirstValue):
    """Aggregate picking the value whose companion timestamp is latest."""

    def step(self, value, time):
        if time is not None and (self._time is None or time > self._time):
            self._time = time
            self._value = value


def _register_timescaledb_shims(engine):
    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_conn, _record):
        inner = dbapi_conn.driver_connection
        await_only(inner.create_function("time_bucket", 2, _time_bucket))
        # aiosqlite exposes create_function but not create_aggregate; register
        # aggregates through the same worker-thread queue create_function uses.
        await_only(inner._execute(inner._conn.create_aggregate, "first", 2, _FirstValue))
        await_only(inner._execute(inner._conn.create_aggregate, "last", 2, _LastValue))


@pytest.fixture
async def db_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    _register_timescaledb_shims(engine)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest.fixture
async def client(db_session_factory):
    async def override_get_session():
        async with db_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


def _t(minutes_offset: int) -> datetime:
    base = datetime(2026, 7, 1, 14, 30, tzinfo=timezone.utc)
    return base + timedelta(minutes=minutes_offset)


async def _seed_option_bars(session, contract_id="SPY_C450", ticker="SPY", right=OptionRight.CALL, count=5):
    exp = date(2026, 7, 10)
    for i in range(count):
        session.add(
            OptionBar1m(
                time=_t(i), contract_id=contract_id, underlying_ticker=ticker,
                expiration_date=exp, strike=450.0, right=right,
                open=1.0 + i, high=1.5 + i, low=0.9 + i, close=1.2 + i,
                delta=0.4 + i * 0.01,
            )
        )
    await session.commit()


@pytest.mark.asyncio
async def test_get_option_bars_returns_seeded_rows(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session)

    resp = await client.get(
        "/options/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat()},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 5
    assert len(body["bars"]) == 5
    assert body["bars"][0]["contract_id"] == "SPY_C450"
    assert body["bars"][0]["right"] == "call"


@pytest.mark.asyncio
async def test_get_option_bars_filters_by_ticker(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session, contract_id="SPY_C450", ticker="SPY")
        await _seed_option_bars(session, contract_id="QQQ_C380", ticker="QQQ")

    resp = await client.get(
        "/options/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat()},
    )
    body = resp.json()
    assert all(b["underlying_ticker"] == "SPY" for b in body["bars"])


@pytest.mark.asyncio
async def test_get_option_bars_filters_by_right(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session, contract_id="SPY_C450", right=OptionRight.CALL)
        await _seed_option_bars(session, contract_id="SPY_P450", right=OptionRight.PUT)

    resp = await client.get(
        "/options/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat(),
            "right": "put",
        },
    )
    body = resp.json()
    assert body["returned"] == 5
    assert all(b["right"] == "put" for b in body["bars"])


@pytest.mark.asyncio
async def test_get_option_bars_filters_by_delta_range(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session, count=10)  # deltas 0.40 .. 0.49

    resp = await client.get(
        "/options/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(20).isoformat(),
            "min_delta": 0.44, "max_delta": 0.46,
        },
    )
    body = resp.json()
    assert body["returned"] == 3  # 0.44, 0.45, 0.46
    assert all(0.44 <= b["delta"] <= 0.46 for b in body["bars"])


@pytest.mark.asyncio
async def test_get_option_bars_filters_by_contract_id(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session, contract_id="SPY_C450")
        await _seed_option_bars(session, contract_id="SPY_C460")

    resp = await client.get(
        "/options/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat(),
            "contract_id": "SPY_C460",
        },
    )
    body = resp.json()
    assert all(b["contract_id"] == "SPY_C460" for b in body["bars"])


@pytest.mark.asyncio
async def test_get_option_bars_pagination(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session, count=10)

    resp1 = await client.get(
        "/options/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(20).isoformat(), "limit": 4},
    )
    body1 = resp1.json()
    assert body1["returned"] == 4
    assert len(body1["bars"]) == 4

    resp2 = await client.get(
        "/options/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(20).isoformat(),
            "limit": 4, "offset": 4,
        },
    )
    body2 = resp2.json()
    assert body2["returned"] == 4
    # no overlap between pages
    ids1 = {b["time"] for b in body1["bars"]}
    ids2 = {b["time"] for b in body2["bars"]}
    assert ids1.isdisjoint(ids2)


@pytest.mark.asyncio
async def test_get_option_bars_invalid_agg_returns_400(client):
    resp = await client.get(
        "/options/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat(), "agg": "3m"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_option_bars_start_after_end_returns_400(client):
    resp = await client.get(
        "/options/bars",
        params={"ticker": "SPY", "start": _t(10).isoformat(), "end": _t(0).isoformat()},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_option_bars_missing_required_param_returns_422(client):
    resp = await client.get("/options/bars", params={"start": _t(0).isoformat(), "end": _t(10).isoformat()})
    assert resp.status_code == 422  # ticker is required


@pytest.mark.asyncio
async def test_get_underlying_bars(client, db_session_factory):
    async with db_session_factory() as session:
        for i in range(3):
            session.add(
                UnderlyingBar1m(time=_t(i), ticker="SPY", open=440.0, high=441.0, low=439.0, close=440.5)
            )
        await session.commit()

    resp = await client.get(
        "/underlying/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat()},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 3


@pytest.mark.asyncio
async def test_get_underlying_bars_agg_5m_aggregates_raw_rows(client, db_session_factory):
    """agg=5m aggregates the raw 1m rows on the fly (time_bucket + OHLCV),
    rather than reading a pre-materialized view."""
    async with db_session_factory() as session:
        for i in range(5):  # 14:30..14:34, all inside the 14:30 five-minute bucket
            session.add(
                UnderlyingBar1m(
                    time=_t(i), ticker="SPY",
                    open=440.0 + i, high=441.0 + i, low=439.0 + i, close=440.5 + i,
                    volume=1000,
                )
            )
        await session.commit()

    resp = await client.get(
        "/underlying/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(4).isoformat(), "agg": "5m"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 1
    bar = body["bars"][0]
    assert bar["open"] == 440.0   # first
    assert bar["high"] == 445.0   # max
    assert bar["low"] == 439.0    # min
    assert bar["close"] == 444.5  # last
    assert bar["volume"] == 5000  # sum


@pytest.mark.asyncio
async def test_get_underlying_bars_invalid_agg_returns_400(client):
    resp = await client.get(
        "/underlying/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat(),
            "agg": "3m",
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_option_bars_agg_5m_aggregates_raw_rows(client, db_session_factory):
    """Same on-the-fly aggregation as the underlying endpoint, for the
    option bars endpoint — OHLC is first/max/min/last, Greeks/volume are
    snapshot/summed, and the right/delta filters still apply."""
    exp = date(2026, 7, 10)
    async with db_session_factory() as session:
        for i in range(5):  # all inside the 14:30 five-minute bucket
            session.add(
                OptionBar1m(
                    time=_t(i), contract_id="SPY_C450", underlying_ticker="SPY",
                    expiration_date=exp, strike=450.0, right=OptionRight.CALL,
                    open=1.0 + i, high=2.0 + i, low=0.5 + i, close=1.5 + i,
                    delta=0.40 + i * 0.02, volume=1,
                )
            )
        await session.commit()

    resp = await client.get(
        "/options/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(4).isoformat(),
            "agg": "5m", "right": "call",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 1
    bar = body["bars"][0]
    assert bar["open"] == 1.0
    assert bar["high"] == 6.0
    assert bar["low"] == 0.5
    assert bar["close"] == 5.5
    assert bar["delta"] == pytest.approx(0.48)
    assert bar["volume"] == 5


@pytest.mark.asyncio
async def test_get_contracts(client, db_session_factory):
    now = datetime.now(timezone.utc)
    async with db_session_factory() as session:
        session.add(
            Contract(
                contract_id="SPY_C450", underlying_ticker="SPY",
                expiration_date=date(2026, 7, 10), strike=450.0, right=OptionRight.CALL,
                first_seen=now, last_seen=now,
            )
        )
        session.add(
            Contract(
                contract_id="SPY_P440", underlying_ticker="SPY",
                expiration_date=date(2026, 7, 17), strike=440.0, right=OptionRight.PUT,
                first_seen=now, last_seen=now,
            )
        )
        await session.commit()

    resp = await client.get("/contracts", params={"ticker": "SPY"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 2


@pytest.mark.asyncio
async def test_get_contracts_filters_by_expiration_range(client, db_session_factory):
    now = datetime.now(timezone.utc)
    async with db_session_factory() as session:
        session.add(
            Contract(
                contract_id="SPY_A", underlying_ticker="SPY",
                expiration_date=date(2026, 7, 10), strike=450.0, right=OptionRight.CALL,
                first_seen=now, last_seen=now,
            )
        )
        session.add(
            Contract(
                contract_id="SPY_B", underlying_ticker="SPY",
                expiration_date=date(2026, 9, 1), strike=450.0, right=OptionRight.CALL,
                first_seen=now, last_seen=now,
            )
        )
        await session.commit()

    resp = await client.get(
        "/contracts", params={"ticker": "SPY", "start": "2026-07-01", "end": "2026-07-31"}
    )
    body = resp.json()
    assert body["returned"] == 1
    assert body["contracts"][0]["contract_id"] == "SPY_A"


@pytest.mark.asyncio
async def test_get_metadata_summarizes_tickers_and_db_size(client, db_session_factory):
    async with db_session_factory() as session:
        await _seed_option_bars(session, contract_id="SPY_C450", ticker="SPY", count=3)
        await _seed_option_bars(session, contract_id="QQQ_C380", ticker="QQQ", count=2)

    resp = await client.get("/metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert sorted(body["tickers"]) == ["QQQ", "SPY"]
    assert body["total_db_rows_estimate"] == 5
    assert body["date_ranges_by_ticker"]["SPY"]["start"].startswith("2026-07-01")
    # Disk size is a real byte count on every dialect the endpoint supports
    # (Postgres: pg_database_size; SQLite in tests: page_count * page_size),
    # reported in both bytes and GB.
    assert isinstance(body["db_disk_size_bytes"], int)
    assert body["db_disk_size_bytes"] > 0
    assert body["db_disk_size_gb"] == round(body["db_disk_size_bytes"] / 1024**3, 3)
    # Can't be > 0 here: the in-memory SQLite test DB is only tens of KB,
    # which rounds to 0.0 GB at 3 decimal places (production is many GB).
    assert body["db_disk_size_gb"] >= 0


@pytest.mark.asyncio
async def test_get_metadata_reports_disk_size_on_postgres():
    """The SQLite-backed test above only proves the field's shape; the value
    production actually serves comes from `pg_database_size`, so check the
    dialect dispatch issues that statement for a postgresql bind too."""
    from service.api.routes import _db_disk_size_bytes

    calls: list[str] = []

    class _FakeSession:  # duck-typed: the helper only uses get_bind/execute
        def get_bind(self):
            class _Dialect:
                name = "postgresql"

            class _Bind:
                dialect = _Dialect()

            return _Bind()

        async def execute(self, stmt):
            calls.append(str(stmt))

            class _Result:
                def scalar_one(self):
                    return 12345

            return _Result()

    assert await _db_disk_size_bytes(_FakeSession()) == 12345
    assert any("pg_database_size" in c for c in calls)


@pytest.mark.asyncio
async def test_health_check_reports_ok_when_db_reachable(client, monkeypatch):
    """/health deliberately checks the real configured DB connection
    directly (service.db.session.check_connection), not through the
    overridable get_session route dependency — that's the whole point of
    a health check, to verify the actual configured connection works, not
    whatever's been swapped in for testing routes. So this test patches
    that specific function to simulate "DB reachable" rather than relying
    on one actually being available in the test environment."""
    from service.api import main as main_module

    async def fake_check_connection():
        return True

    monkeypatch.setattr(main_module, "check_connection", fake_check_connection)

    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["database"] == "ok"


@pytest.mark.asyncio
async def test_health_check_reports_degraded_when_db_unreachable(client, monkeypatch):
    from service.api import main as main_module

    async def fake_check_connection():
        return False

    monkeypatch.setattr(main_module, "check_connection", fake_check_connection)

    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"
    assert resp.json()["database"] == "unreachable"


@pytest.mark.asyncio
async def test_api_key_required_when_configured(client, monkeypatch):
    from service.config.settings import get_settings

    get_settings.cache_clear()
    monkeypatch.setenv("API_KEY", "secret123")
    get_settings.cache_clear()
    try:
        resp = await client.get("/health")  # /health has no auth dependency — should still work
        assert resp.status_code == 200

        resp2 = await client.get("/contracts", params={"ticker": "SPY"})
        assert resp2.status_code == 401

        resp3 = await client.get(
            "/contracts", params={"ticker": "SPY"}, headers={"X-API-Key": "secret123"}
        )
        assert resp3.status_code == 200
    finally:
        monkeypatch.delenv("API_KEY", raising=False)
        get_settings.cache_clear()


# --- GET /gaps ---


@pytest.mark.asyncio
async def test_get_gaps_reports_none_for_fully_covered_range(client, db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    start = datetime(2026, 7, 22, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 22, 23, 59, tzinfo=timezone.utc)
    minutes = expected_bar_minutes(start, end)

    async with db_session_factory() as session:
        for m in minutes:
            session.add(UnderlyingBar1m(time=m, ticker="SPX", open=1, high=1, low=1, close=1))
        await session.commit()

    resp = await client.get(
        "/gaps", params={"ticker": "SPX", "start": start.isoformat(), "end": end.isoformat()}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == "SPX"
    assert body["gap_count"] == 0
    assert body["gaps"] == []
    assert body["total_missing_minutes"] == 0


@pytest.mark.asyncio
async def test_get_gaps_reports_a_missing_range(client, db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    # 9a-11a present, nothing again until 12p — the exact bug-report scenario.
    start = datetime(2026, 7, 22, 9, 30, tzinfo=timezone.utc).astimezone(timezone.utc)
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    session_start = datetime(2026, 7, 22, 9, 30, tzinfo=et).astimezone(timezone.utc)
    session_end = datetime(2026, 7, 22, 16, 0, tzinfo=et).astimezone(timezone.utc)
    morning_cutoff = datetime(2026, 7, 22, 11, 0, tzinfo=et).astimezone(timezone.utc)
    afternoon_start = datetime(2026, 7, 22, 12, 0, tzinfo=et).astimezone(timezone.utc)

    minutes = expected_bar_minutes(session_start, session_end)

    async with db_session_factory() as session:
        for m in minutes:
            if m <= morning_cutoff or m >= afternoon_start:
                session.add(UnderlyingBar1m(time=m, ticker="SPX", open=1, high=1, low=1, close=1))
        await session.commit()

    resp = await client.get(
        "/gaps",
        params={
            "ticker": "SPX",
            "start": session_start.isoformat(),
            "end": session_end.isoformat(),
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["gap_count"] == 1
    assert body["total_missing_minutes"] == 59
    assert body["gaps"][0]["minutes"] == 59


@pytest.mark.asyncio
async def test_get_gaps_does_not_report_data_for_a_different_ticker(client, db_session_factory):
    from service.ingestion.gap_detection import expected_bar_minutes

    start = datetime(2026, 7, 22, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 22, 23, 59, tzinfo=timezone.utc)
    minutes = expected_bar_minutes(start, end)

    async with db_session_factory() as session:
        for m in minutes:
            session.add(UnderlyingBar1m(time=m, ticker="NDX", open=1, high=1, low=1, close=1))
        await session.commit()

    resp = await client.get(
        "/gaps", params={"ticker": "SPX", "start": start.isoformat(), "end": end.isoformat()}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["gap_count"] == 1  # SPX has NDX's bars, so nothing at all counts as "existing" for SPX


@pytest.mark.asyncio
async def test_get_gaps_start_after_end_returns_400(client):
    resp = await client.get(
        "/gaps",
        params={
            "ticker": "SPX",
            "start": "2026-07-22T12:00:00Z",
            "end": "2026-07-22T09:00:00Z",
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_gaps_range_too_wide_returns_400(client):
    resp = await client.get(
        "/gaps",
        params={
            "ticker": "SPX",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-12-31T00:00:00Z",
        },
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_gaps_flags_a_gap_entirely_before_retention(client, db_session_factory):
    from service.ingestion.gap_detection import RETENTION_DAYS

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=RETENTION_DAYS + 30)
    end = now - timedelta(days=RETENTION_DAYS + 10)  # entirely before the retention cutoff

    resp = await client.get(
        "/gaps", params={"ticker": "SPX", "start": start.isoformat(), "end": end.isoformat()}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["gap_count"] >= 1
    assert all(g["before_retention"] for g in body["gaps"])


@pytest.mark.asyncio
async def test_get_gaps_reports_retention_cutoff(client):
    resp = await client.get(
        "/gaps",
        params={
            "ticker": "SPX",
            "start": "2026-07-22T00:00:00Z",
            "end": "2026-07-22T23:59:00Z",
        },
    )
    assert resp.status_code == 200
    assert "retention_cutoff" in resp.json()


@pytest.mark.asyncio
async def test_get_gaps_respects_min_gap_minutes(client, db_session_factory):
    from zoneinfo import ZoneInfo

    from service.ingestion.gap_detection import expected_bar_minutes

    et = ZoneInfo("America/New_York")
    start = datetime(2026, 7, 22, 9, 30, tzinfo=et).astimezone(timezone.utc)
    end = datetime(2026, 7, 22, 9, 40, tzinfo=et).astimezone(timezone.utc)
    minutes = expected_bar_minutes(start, end)

    async with db_session_factory() as session:
        for m in minutes:
            if m != minutes[5]:  # one missing minute
                session.add(UnderlyingBar1m(time=m, ticker="SPX", open=1, high=1, low=1, close=1))
        await session.commit()

    resp_default = await client.get(
        "/gaps", params={"ticker": "SPX", "start": start.isoformat(), "end": end.isoformat()}
    )
    resp_filtered = await client.get(
        "/gaps",
        params={
            "ticker": "SPX", "start": start.isoformat(), "end": end.isoformat(),
            "min_gap_minutes": 2,
        },
    )

    assert resp_default.json()["gap_count"] == 1
    assert resp_filtered.json()["gap_count"] == 0
