"""
FastAPI dependency for DB session injection.

Done via `Depends(get_session)` (not calling
service.db.session.get_session_factory() directly, the way the ingestion
side of the codebase does) specifically so tests can override it with
`app.dependency_overrides[get_session] = ...` and point requests at an
in-memory SQLite DB instead of a real Postgres — the standard FastAPI
testing pattern.

API key auth lives in service/api/auth.py, not here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from service.db.session import get_session as _get_session


async def get_session() -> AsyncIterator[AsyncSession]:
    async with _get_session() as session:
        yield session
