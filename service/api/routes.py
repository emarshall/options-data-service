"""
Query API routes: GET /options/bars, GET /underlying/bars, GET /contracts.

Pagination is limit/offset (see schemas.BarsResponse's docstring for why:
`returned < limit` unambiguously tells a client they've reached the end
without a separate COUNT query). `limit` is capped (see _MAX_LIMIT) to
keep any single request bounded regardless of what a client asks for.
"""

from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from service.api.auth import require_api_key
from service.api.deps import get_session
from service.api.schemas import (
    BarsResponse,
    ContractOut,
    ContractsResponse,
    OptionBarOut,
    UnderlyingBarOut,
    UnderlyingBarsResponse,
)
from service.db.models import Contract, OptionRight
from service.db.session import _engine
from service.db.views import AGG_PERIODS, get_option_bars_table, get_underlying_bars_table

router = APIRouter(dependencies=[Depends(require_api_key)])

_DEFAULT_LIMIT = 1000
_MAX_LIMIT = 20000


@router.get("/options/bars", response_model=BarsResponse)
async def get_option_bars(
    ticker: str = Query(..., description="Underlying ticker, e.g. SPY"),
    start: datetime = Query(..., description="Inclusive start of the time range (ISO 8601)"),
    end: datetime = Query(..., description="Inclusive end of the time range (ISO 8601)"),
    agg: str = Query("1m", description=f"Aggregation period. One of: {', '.join(AGG_PERIODS)}"),
    right: OptionRight | None = Query(None, description="Filter to calls or puts"),
    min_delta: float | None = Query(None, description="Minimum delta (inclusive)"),
    max_delta: float | None = Query(None, description="Maximum delta (inclusive)"),
    expiration: date | None = Query(None, description="Filter to a single exact expiration date"),
    contract_id: str | None = Query(None, description="Filter to a single exact contract"),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> BarsResponse:
    if agg not in AGG_PERIODS:
        raise HTTPException(status_code=400, detail=f"Invalid agg {agg!r}. Valid: {AGG_PERIODS}")
    if start > end:
        raise HTTPException(status_code=400, detail="start must be <= end")

    table = get_option_bars_table(agg)
    conditions = [
        table.c.underlying_ticker == ticker,
        table.c.time >= start,
        table.c.time <= end,
    ]
    if right is not None:
        conditions.append(table.c.right == right)
    if min_delta is not None:
        conditions.append(table.c.delta >= min_delta)
    if max_delta is not None:
        conditions.append(table.c.delta <= max_delta)
    if expiration is not None:
        conditions.append(table.c.expiration_date == expiration)
    if contract_id is not None:
        conditions.append(table.c.contract_id == contract_id)

    stmt = (
        select(table)
        .where(and_(*conditions))
        .order_by(table.c.time.asc())
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    rows = result.mappings().all()

    return BarsResponse(
        bars=[OptionBarOut.model_validate(dict(row)) for row in rows],
        limit=limit,
        offset=offset,
        returned=len(rows),
    )


@router.get("/underlying/bars", response_model=UnderlyingBarsResponse)
async def get_underlying_bars(
    ticker: str = Query(..., description="Underlying ticker, e.g. SPY"),
    start: datetime = Query(..., description="Inclusive start of the time range (ISO 8601)"),
    end: datetime = Query(..., description="Inclusive end of the time range (ISO 8601)"),
    agg: str = Query("1m", description=f"Aggregation period. One of: {', '.join(AGG_PERIODS)}"),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> UnderlyingBarsResponse:
    if agg not in AGG_PERIODS:
        raise HTTPException(status_code=400, detail=f"Invalid agg {agg!r}. Valid: {AGG_PERIODS}")
    if start > end:
        raise HTTPException(status_code=400, detail="start must be <= end")

    table = get_underlying_bars_table(agg)
    stmt = (
        select(table)
        .where(
            and_(
                table.c.ticker == ticker,
                table.c.time >= start,
                table.c.time <= end,
            )
        )
        .order_by(table.c.time.asc())
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    rows = result.mappings().all()

    return UnderlyingBarsResponse(
        bars=[UnderlyingBarOut.model_validate(dict(row)) for row in rows],
        limit=limit,
        offset=offset,
        returned=len(rows),
    )


@router.get("/contracts", response_model=ContractsResponse)
async def get_contracts(
    ticker: str = Query(..., description="Underlying ticker, e.g. SPY"),
    start: date | None = Query(None, description="Earliest expiration date (inclusive)"),
    end: date | None = Query(None, description="Latest expiration date (inclusive)"),
    right: OptionRight | None = Query(None, description="Filter to calls or puts"),
    limit: int = Query(_DEFAULT_LIMIT, ge=1, le=_MAX_LIMIT),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> ContractsResponse:
    if start is not None and end is not None and start > end:
        raise HTTPException(status_code=400, detail="start must be <= end")

    conditions = [Contract.underlying_ticker == ticker]
    if start is not None:
        conditions.append(Contract.expiration_date >= start)
    if end is not None:
        conditions.append(Contract.expiration_date <= end)
    if right is not None:
        conditions.append(Contract.right == right)

    stmt = (
        select(Contract)
        .where(and_(*conditions))
        .order_by(Contract.expiration_date.asc(), Contract.strike.asc())
        .limit(limit)
        .offset(offset)
    )
    result = await session.execute(stmt)
    rows = result.scalars().all()

    return ContractsResponse(
        contracts=[ContractOut.model_validate(row) for row in rows],
        limit=limit,
        offset=offset,
        returned=len(rows),
    )

@router.get("/metadata")
async def metadata(
    session: AsyncSession = Depends(get_session)
):
    """Returns summary metadata about tickers and data ranges."""

    query_tickers = """
        SELECT DISTINCT underlying_ticker FROM option_bars_1m;
    """

    query_date_ranges = """
        SELECT 
            t.underlying_ticker,
            MIN(t.time) AS start_date,
            MAX(t.time) AS end_date
        FROM option_bars_1m t
        GROUP BY t.underlying_ticker;
    """

    query_row_count = """
        SELECT COUNT(*) AS total_rows 
        FROM option_bars_1m;
    """

    result_tickers = await session.execute(text(query_tickers))
    tickers = [row.underlying_ticker for row in result_tickers.fetchall()]

    result_date_ranges = await session.execute(text(query_date_ranges))
    date_ranges = {
        row.underlying_ticker: {"start": str(row.start_date), "end": str(row.end_date)}
        for row in result_date_ranges.fetchall()
    }

    result_row_count = await session.execute(text(query_row_count))
    total_rows = result_row_count.fetchone()[0]

    return {
        "tickers": tickers,
        "date_ranges_by_ticker": date_ranges,
        "total_db_rows_estimate": total_rows
    }
