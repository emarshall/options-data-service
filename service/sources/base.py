"""
Source abstraction. This is the boundary that keeps the rest of the system
independent of which broker/feed is actually supplying data — TastyTrade is
the first (and for now, only) implementation, built out in Task 2.

Deliberately just a shape/stub in Task 1 — no logic here yet, just the
interface, so later tasks (and anyone picking this repo up fresh) can see
what a second source implementation would need to provide.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Awaitable, Callable


class MarketDataSource(ABC):
    """Abstract interface a market data provider must implement.

    Implemented by: TastyTradeSource (Task 2). A second source (e.g. a paid
    historical vendor, or another broker) would implement this same
    interface and could be swapped in without touching the ingestion
    pipeline (Task 4) or contract manager (Task 3).
    """

    @abstractmethod
    async def authenticate(self) -> None:
        """Establish/refresh a session. Implementations should handle
        token refresh transparently on subsequent calls."""
        ...

    @abstractmethod
    async def get_option_chain(self, ticker: str) -> dict[date, list[Any]]:
        """Return the current option chain for `ticker`, keyed by
        expiration date."""
        ...

    @abstractmethod
    async def subscribe_quotes(
        self, symbols: list[str], callback: Callable[[Any], Awaitable[None]]
    ) -> None:
        """Subscribe to live bid/ask/mark updates for `symbols`. `callback`
        is invoked for each event received."""
        ...

    @abstractmethod
    async def subscribe_greeks(
        self, symbols: list[str], callback: Callable[[Any], Awaitable[None]]
    ) -> None:
        """Subscribe to live Greeks updates for `symbols`."""
        ...

    @abstractmethod
    async def snapshot_greeks(self, symbols: list[str], timeout_s: float) -> dict[str, Any]:
        """One-off Greeks read for `symbols` — returns whatever arrives
        within `timeout_s`, keyed by symbol. Added during Task 3: contract
        resolution needs a live delta reading to filter candidates, but
        that's a self-contained snapshot, not an ongoing subscription — it
        must NOT interact with subscribe_greeks()'s persistent callback
        slot (which Task 4's ingestion pipeline owns), or one would clobber
        the other."""
        ...

    @abstractmethod
    async def request_candles(
        self, symbol: str, period: str, start_time: datetime
    ) -> list[Any]:
        """Request historical OHLC candles for `symbol` at the given
        aggregation `period` (e.g. '1m', '1d'), starting from `start_time`.
        Used by the backfill job (Task 5). Note (per Task 0 findings): for
        TastyTrade specifically, 1-minute option candle depth is capped at
        ~6 weeks regardless of how far back `start_time` requests."""
        ...

    @abstractmethod
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from all event types for `symbols`."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Gracefully tear down any open connections. Added during Task 2 —
        not anticipated in the original Task 1 stub, but needed for clean
        shutdown of a long-lived service."""
        ...
