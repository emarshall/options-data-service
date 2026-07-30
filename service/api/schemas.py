"""
Response schemas for the query API.

Field set on OptionBarOut mirrors option_bars_1m / the continuous
aggregate views exactly (see service/db/models.py and
alembic/versions/0002_continuous_aggregates.py) — same columns regardless
of which aggregation period was queried.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from service.db.models import GreeksSource, OptionRight, SettlementType


class OptionBarOut(BaseModel):
    time: datetime
    contract_id: str
    underlying_ticker: str
    expiration_date: date
    strike: float
    right: OptionRight

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
    greeks_source: GreeksSource | None = None

    volume: int | None = None
    open_interest: int | None = None
    vwap: float | None = None
    bid_volume: int | None = None
    ask_volume: int | None = None

    model_config = {"from_attributes": True}


class UnderlyingBarOut(BaseModel):
    time: datetime
    ticker: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: int | None = None

    model_config = {"from_attributes": True}


class ContractOut(BaseModel):
    contract_id: str
    underlying_ticker: str
    expiration_date: date
    strike: float
    right: OptionRight
    settlement_type: SettlementType | None = None
    first_seen: datetime
    last_seen: datetime

    model_config = {"from_attributes": True}


class BarsResponse(BaseModel):
    """Wraps a list of bars with pagination info, rather than returning a
    bare array — makes it unambiguous to a client whether they've reached
    the end (`returned < limit`) without needing a separate count query."""

    bars: list[OptionBarOut]
    limit: int
    offset: int
    returned: int


class UnderlyingBarsResponse(BaseModel):
    bars: list[UnderlyingBarOut]
    limit: int
    offset: int
    returned: int


class ContractsResponse(BaseModel):
    contracts: list[ContractOut]
    limit: int
    offset: int
    returned: int
