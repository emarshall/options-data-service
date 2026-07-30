"""
Contract resolution & delta-range filtering.

Given the configured tickers (service/config/settings.py's TickerConfig
list), figures out which specific option contracts should currently be
tracked, and keeps that set current as contracts expire and new ones
(including same-day 0DTE listings) appear.

**Scope note vs. the original PLAN.md draft:** the initial plan implied
this task would directly call subscribe_quotes()/subscribe_greeks() on the
source for the real, ongoing ingestion subscriptions. Implemented instead
so ContractManager only *resolves* the set (and persists contract
metadata) and returns a diff (added/removed) — actually wiring that diff
into subscribe_quotes()/subscribe_greeks() with a bar-aggregating callback
is Task 4's job. This keeps ContractManager testable on its own, without a
real ingestion pipeline needing to exist yet.

**Key design decision: delta filtering happens once, at "listing" time,
not continuously.** When a candidate contract is first seen, a live Greeks
snapshot decides whether it's in-range. Once tracked, a contract stays
tracked until it disappears from the chain entirely (typically at/after
expiration) — its delta is never re-checked. Two reasons: (1) it avoids a
real hazard where re-snapshotting an already-subscribed symbol's Greeks
and then cleaning up that snapshot could unsubscribe the *persistent*
ingestion subscription for the same symbol (snapshot_greeks() and
subscribe_greeks() share the underlying DXLink subscription state even
though they're separate methods); (2) for a backtesting dataset, "track
what started in your delta band" is a reasonable, simple, defensible
policy — a contract drifting out of your delta band over time is itself
useful signal to have captured, not a reason to stop recording it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from service.config.settings import TickerConfig
from service.db.models import Contract, OptionRight, SettlementType
from service.sources._async_utils import get_attr_any
from service.sources.base import MarketDataSource

log = logging.getLogger(__name__)


@dataclass
class ResolvedContract:
    contract_id: str
    underlying_ticker: str
    expiration_date: date
    strike: float
    right: OptionRight
    settlement_type: SettlementType | None


@dataclass
class ContractDiff:
    """Result of a refresh() call."""

    added: dict[str, ResolvedContract] = field(default_factory=dict)
    removed: set[str] = field(default_factory=set)
    current: dict[str, ResolvedContract] = field(default_factory=dict)


class ContractManager:
    def __init__(
        self,
        source: MarketDataSource,
        session_factory,
        tickers: list[TickerConfig],
        greeks_snapshot_timeout_s: float = 15.0,
    ):
        self._source = source
        self._session_factory = session_factory
        self._tickers = tickers
        self._greeks_snapshot_timeout_s = greeks_snapshot_timeout_s
        self._tracked: dict[str, ResolvedContract] = {}

    async def refresh(self) -> ContractDiff:
        """Re-resolves every configured ticker's contract set, persists
        metadata for anything new, and returns what changed since the last
        call (or, on the first call, everything as "added")."""
        resolved: dict[str, ResolvedContract] = {}
        for cfg in self._tickers:
            try:
                resolved.update(await self._resolve_ticker(cfg))
            except Exception:
                log.exception(
                    "Failed to resolve contracts for %s — skipping it this cycle, "
                    "keeping whatever was already tracked for it.",
                    cfg.ticker,
                )
                # Carry forward whatever we already had for this ticker
                # rather than dropping it because one refresh cycle failed
                # (e.g. a transient chain-lookup error shouldn't look like
                # every contract for that ticker just expired).
                resolved.update(
                    {cid: rc for cid, rc in self._tracked.items() if rc.underlying_ticker == cfg.ticker}
                )

        new_ids = set(resolved.keys())
        old_ids = set(self._tracked.keys())
        added = {cid: resolved[cid] for cid in (new_ids - old_ids)}
        removed = old_ids - new_ids

        await self._persist(resolved.values())

        if added or removed:
            log.info(
                "Contract refresh: %d added, %d removed, %d tracked total.",
                len(added), len(removed), len(new_ids),
            )
        self._tracked = resolved
        return ContractDiff(added=added, removed=removed, current=resolved)

    def get_resolved(self, contract_id: str) -> ResolvedContract | None:
        """Looks up a currently-tracked contract's resolved metadata (used
        by the ingestion pipeline to populate the denormalized ticker/
        expiration/strike/right fields when writing a bar). Returns None
        if `contract_id` isn't (or is no longer) tracked."""
        return self._tracked.get(contract_id)

    async def _resolve_ticker(self, cfg: TickerConfig) -> dict[str, ResolvedContract]:
        chain = await self._source.get_option_chain(cfg.ticker)

        candidates = []
        for _expiration, options in chain.items():
            for opt in options:
                dte = get_attr_any(opt, "days_to_expiration")
                if dte is not None and dte > cfg.max_days_to_expiration:
                    continue
                if cfg.exclude_am_settled and self._is_am_settled(opt):
                    continue
                candidates.append(opt)

        if not candidates:
            log.warning(
                "%s: no candidate contracts within %d days to expiration.",
                cfg.ticker, cfg.max_days_to_expiration,
            )
            return {}

        # Already-tracked contracts carry forward without re-checking delta
        # (see module docstring). Only genuinely new ones need a snapshot.
        carried_forward = {
            c.streamer_symbol: self._tracked[c.streamer_symbol]
            for c in candidates
            if c.streamer_symbol in self._tracked
        }
        new_candidates = [c for c in candidates if c.streamer_symbol not in self._tracked]

        newly_resolved: dict[str, ResolvedContract] = {}
        if new_candidates:
            symbols = [c.streamer_symbol for c in new_candidates]
            greeks_by_symbol = await self._source.snapshot_greeks(
                symbols, timeout_s=self._greeks_snapshot_timeout_s
            )

            missing = [s for s in symbols if s not in greeks_by_symbol]
            if missing:
                log.info(
                    "%s: no Greeks snapshot received for %d/%d new candidate(s) — will "
                    "retry next refresh.",
                    cfg.ticker, len(missing), len(symbols),
                )

            for opt in new_candidates:
                symbol = opt.streamer_symbol
                greeks = greeks_by_symbol.get(symbol)
                if greeks is None:
                    continue
                delta = get_attr_any(greeks, "delta")
                if delta is None:
                    continue

                right = self._option_right(opt)
                if right == OptionRight.CALL:
                    in_range = cfg.call_delta_min <= delta <= cfg.call_delta_max
                else:
                    in_range = cfg.put_delta_min <= delta <= cfg.put_delta_max
                if not in_range:
                    continue

                newly_resolved[symbol] = ResolvedContract(
                    contract_id=symbol,
                    underlying_ticker=cfg.ticker,
                    expiration_date=opt.expiration_date,
                    strike=float(opt.strike_price),
                    right=right,
                    settlement_type=self._settlement_type(opt),
                )

            log.info(
                "%s: %d new candidate(s) checked, %d newly in delta range "
                "(%d already tracked carried forward).",
                cfg.ticker, len(new_candidates), len(newly_resolved), len(carried_forward),
            )

        return {**carried_forward, **newly_resolved}

    @staticmethod
    def _option_right(opt: Any) -> OptionRight:
        value = get_attr_any(opt, "option_type")
        value = getattr(value, "value", value)  # unwrap enum if it is one
        return OptionRight.CALL if str(value).upper().startswith("C") else OptionRight.PUT

    @staticmethod
    def _is_am_settled(opt: Any) -> bool:
        value = get_attr_any(opt, "settlement_type", default="")
        value = getattr(value, "value", value)
        return str(value).upper() == "AM"

    @staticmethod
    def _settlement_type(opt: Any) -> SettlementType | None:
        value = get_attr_any(opt, "settlement_type")
        if value is None:
            return None
        value = str(getattr(value, "value", value)).upper()
        if value == "AM":
            return SettlementType.AM
        if value == "PM":
            return SettlementType.PM
        return None

    async def _persist(self, contracts: list[ResolvedContract]) -> None:
        contracts = list(contracts)
        if not contracts:
            return
        now = datetime.now(timezone.utc)
        async with self._session_factory() as session:
            for rc in contracts:
                existing = await session.get(Contract, rc.contract_id)
                if existing is None:
                    session.add(
                        Contract(
                            contract_id=rc.contract_id,
                            underlying_ticker=rc.underlying_ticker,
                            expiration_date=rc.expiration_date,
                            strike=rc.strike,
                            right=rc.right,
                            settlement_type=rc.settlement_type,
                            first_seen=now,
                            last_seen=now,
                        )
                    )
                else:
                    existing.last_seen = now
            await session.commit()
