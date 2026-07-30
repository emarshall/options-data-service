"""
Validates Enum column serialization against a REAL Postgres instance.

This exists because of a real bug (see PLAN.md Section 7): SQLAlchemy's
`Enum(python_enum_class)` defaults to persisting the member's `.name`
("CALL") rather than `.value` ("call"), and this mismatch against a
separately-defined native Postgres enum type (created via raw SQL using
lowercase `.value` strings) is **structurally invisible** to the rest of
this project's test suite, which runs against SQLite — SQLite has no
native enum type, so SQLAlchemy just generates a CHECK constraint from
whichever convention it's using, making encoding and validation
automatically self-consistent regardless of which one it picks. The bug
only surfaced against a real deployment.

Skipped by default (no real Postgres in most environments this test suite
runs in). To actually run it:
    TEST_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/dbname pytest tests/test_enum_serialization_postgres.py

A docker-compose TimescaleDB instance works fine for this — it's plain
Postgres underneath for anything not TimescaleDB-hypertable-specific,
which this test doesn't touch at all (no hypertables needed, just the
enum types + a plain table, created directly rather than via Alembic so
this has no dependency on the TimescaleDB extension being present).
"""

import os
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from service.db.models import Contract, GreeksSource, OptionRight, SettlementType

_TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="Set TEST_DATABASE_URL to run this against a real Postgres instance.",
)


@pytest.fixture
async def pg_session_factory():
    engine = create_async_engine(_TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS contracts"))
        await conn.execute(text("DROP TYPE IF EXISTS option_right"))
        await conn.execute(text("DROP TYPE IF EXISTS settlement_type"))
        await conn.execute(text("DROP TYPE IF EXISTS greeks_source"))
        # Matches alembic/versions/0001_initial_schema.py's raw SQL exactly
        # (lowercase values) — this is the thing being validated against.
        await conn.execute(text("CREATE TYPE option_right AS ENUM ('call', 'put')"))
        await conn.execute(text("CREATE TYPE settlement_type AS ENUM ('am', 'pm')"))
        await conn.execute(text("CREATE TYPE greeks_source AS ENUM ('live', 'computed')"))
        await conn.execute(
            text(
                """
                CREATE TABLE contracts (
                    contract_id VARCHAR PRIMARY KEY,
                    underlying_ticker VARCHAR NOT NULL,
                    expiration_date DATE NOT NULL,
                    strike NUMERIC(12,4) NOT NULL,
                    "right" option_right NOT NULL,
                    settlement_type settlement_type,
                    first_seen TIMESTAMPTZ NOT NULL,
                    last_seen TIMESTAMPTZ NOT NULL
                )
                """
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS contracts"))
        await conn.execute(text("DROP TYPE IF EXISTS option_right"))
        await conn.execute(text("DROP TYPE IF EXISTS settlement_type"))
        await conn.execute(text("DROP TYPE IF EXISTS greeks_source"))
    await engine.dispose()


@pytest.mark.asyncio
async def test_call_option_inserts_and_reads_back_against_real_postgres_enum(pg_session_factory):
    async with pg_session_factory() as session:
        session.add(
            Contract(
                contract_id=".SPY260720C742", underlying_ticker="SPY",
                expiration_date=date(2026, 7, 20), strike=742.0,
                right=OptionRight.CALL, settlement_type=SettlementType.PM,
                first_seen=datetime.now(timezone.utc), last_seen=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    async with pg_session_factory() as session:
        row = await session.get(Contract, ".SPY260720C742")
        assert row.right == OptionRight.CALL
        assert row.settlement_type == SettlementType.PM


@pytest.mark.asyncio
async def test_put_option_inserts_and_reads_back_against_real_postgres_enum(pg_session_factory):
    async with pg_session_factory() as session:
        session.add(
            Contract(
                contract_id=".SPY260720P742", underlying_ticker="SPY",
                expiration_date=date(2026, 7, 20), strike=742.0,
                right=OptionRight.PUT, settlement_type=SettlementType.AM,
                first_seen=datetime.now(timezone.utc), last_seen=datetime.now(timezone.utc),
            )
        )
        await session.commit()

    async with pg_session_factory() as session:
        row = await session.get(Contract, ".SPY260720P742")
        assert row.right == OptionRight.PUT
        assert row.settlement_type == SettlementType.AM
