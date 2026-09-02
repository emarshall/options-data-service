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
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.api.deps import get_session
from service.api.main import app
from service.db.models import Base, Contract, OptionBar1m, OptionRight, UnderlyingBar1m
from service.db.views import OPTION_BARS_VIEW_TABLES, UNDERLYING_BARS_VIEW_TABLES, _metadata as _views_metadata


@pytest.fixture
async def db_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Also create the (otherwise TimescaleDB-only) continuous aggregate
        # views' table shapes here, as plain tables — lets us test the
        # actual route/query-dispatch logic for agg=5m etc. end-to-end,
        # even though SQLite obviously can't do real continuous
        # aggregation. Real aggregate *content* is validated separately
        # against actual Postgres (see PLAN.md Task 7/8 notes) — this is
        # purely about proving the API selects the right table and filters
        # correctly, not about TimescaleDB's own materialization behavior.
        await conn.run_sync(_views_metadata.create_all)
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
async def test_get_underlying_bars_agg_5m_queries_the_view_table(client, db_session_factory):
    """Proves the route actually selects and queries the option_bars_5m-
    style view table for agg=5m, not just the 1m hypertable — the view
    table won't exist as ORM-mapped rows, so this inserts directly via
    Core against the lightweight Table definition from service/db/views.py."""
    view_table = UNDERLYING_BARS_VIEW_TABLES["5m"]
    async with db_session_factory() as session:
        await session.execute(
            view_table.insert().values(
                time=_t(0), ticker="SPY", open=440.0, high=442.0, low=439.0, close=441.0, volume=10000,
            )
        )
        await session.commit()

    resp = await client.get(
        "/underlying/bars",
        params={"ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat(), "agg": "5m"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 1
    assert body["bars"][0]["close"] == 441.0


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
async def test_get_option_bars_agg_5m_queries_the_view_table(client, db_session_factory):
    """Same proof as above, for the option bars endpoint — confirms the
    5m view table gets queried (with the right/delta filters intact),
    not just the underlying one."""
    view_table = OPTION_BARS_VIEW_TABLES["5m"]
    async with db_session_factory() as session:
        await session.execute(
            view_table.insert().values(
                time=_t(0), contract_id="SPY_C450", underlying_ticker="SPY",
                expiration_date=date(2026, 7, 10), strike=450.0, right="call",
                close=1.5, delta=0.42,
            )
        )
        await session.commit()

    resp = await client.get(
        "/options/bars",
        params={
            "ticker": "SPY", "start": _t(0).isoformat(), "end": _t(10).isoformat(),
            "agg": "5m", "right": "call",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["returned"] == 1
    assert body["bars"][0]["close"] == 1.5
    assert body["bars"][0]["delta"] == 0.42


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
