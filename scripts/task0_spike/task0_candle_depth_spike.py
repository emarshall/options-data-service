#!/usr/bin/env python3
"""
Task 0 — Feasibility Spike: Historical Option Candle Depth + Sample Data Dump
===============================================================================

WHAT THIS DOES
--------------
1. Authenticates to TastyTrade.
2. Picks a handful of real option contracts on a configurable underlying
   (default SPY): the nearest expiration (which may literally be a 0DTE
   contract, possibly already expired by the time you run this), one about
   a week out, and one about a month out.
3. For each contract, requests historical 1-minute and 1-day Candle events
   going back as far as the feed will give us, and records what actually
   comes back (first bar timestamp, bar count, whether implied vol is
   populated).
4. Grabs live sample Quote/Greeks events for a call and a put so we have
   real payloads to look at (not just documentation examples).
5. (Best-effort/experimental) tries the same candle request against a
   contract from ~1 week ago that has almost certainly already expired,
   to specifically probe whether TastyTrade retains ANY candle history for
   expired option contracts — this is the single most decision-relevant
   question for 0DTE backtesting.
6. Writes everything to a timestamped output folder: a human-readable
   summary, a JSONL of every raw event received, and a curated sample_data
   file. Prints a summary to the console too.

WHY THIS EXISTS
----------------
See PLAN.md, Task 0. TastyTrade/dxfeed's documentation does not state how
far back Candle events retain history specifically for OPTION contracts
(as opposed to equities/indices), and dxfeed's own docs are explicit that
historical *Greeks* are not available via API at all (real-time only).
This script exists to empirically settle the candle-depth question before
Task 5 (backfill) gets scoped in detail.

REQUIREMENTS
------------
    pip install -r requirements.txt

This targets the `tastytrade` (tastyware) package, version >= 12.1.0
(the version that introduced DXLinkStreamer.subscribe_candle). This is an
actively developed, unofficial SDK, and its exact method signatures do
shift between versions. This script is written defensively (it inspects
signatures at runtime and tries a couple of call patterns) so that if a
signature has changed since this was written, you'll get a clear error
pointing at the mismatch rather than a silent failure — check
`pip show tastytrade` and https://github.com/tastyware/tastytrade for the
current API if that happens, the fix is almost always just adjusting one
function call, not this script's overall logic.

CREDENTIALS
-----------
Copy .env.example to .env and fill in EITHER:
  - TASTYTRADE_CLIENT_SECRET + TASTYTRADE_REFRESH_TOKEN (OAuth2, preferred
    for anything long-lived — see https://developer.tastytrade.com/oauth2/
    for how to set up a personal OAuth2 app), OR
  - TASTYTRADE_USERNAME + TASTYTRADE_PASSWORD (simpler to get started with)

This script only reads market data — it never places or touches orders.

USAGE
-----
    python task0_candle_depth_spike.py --ticker SPY
    python task0_candle_depth_spike.py --ticker SPX --use-sandbox
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import inspect
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncIterator

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("task0")


# ---------------------------------------------------------------------------
# Serialization helpers — the SDK's event objects may be pydantic models,
# dataclasses, or plain objects depending on version, so try a few strategies.
# ---------------------------------------------------------------------------

def to_serializable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_serializable(v) for v in obj]
    # pydantic v2
    if hasattr(obj, "model_dump"):
        try:
            return to_serializable(obj.model_dump())
        except Exception:
            pass
    # pydantic v1
    if hasattr(obj, "dict"):
        try:
            return to_serializable(obj.dict())
        except Exception:
            pass
    if dataclasses.is_dataclass(obj):
        return to_serializable(dataclasses.asdict(obj))
    if hasattr(obj, "__dict__"):
        return to_serializable(vars(obj))
    return str(obj)


def get_attr_any(obj: Any, *names: str, default=None):
    for name in names:
        if hasattr(obj, name):
            val = getattr(obj, name)
            if val is not None:
                return val
    return default


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------

class Recorder:
    """Writes everything incrementally, not just at the end. If the process
    gets hard-killed (e.g. because a stuck websocket read forced a manual
    Ctrl+C/kill), summary.md and sample_data.json still reflect whatever
    was collected up to that point instead of not existing at all."""

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.raw_events_path = self.out_dir / "raw_events.jsonl"
        self._raw_f = open(self.raw_events_path, "w")
        self.summary_path = self.out_dir / "summary.md"
        self.sample_path = self.out_dir / "sample_data.json"
        self.summary_lines: list[str] = []
        self.sample_data: dict[str, Any] = {}

    def raw_event(self, category: str, event: Any):
        record = {
            "category": category,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "event": to_serializable(event),
        }
        self._raw_f.write(json.dumps(record, default=str) + "\n")
        self._raw_f.flush()

    def summary(self, line: str = ""):
        print(line)
        self.summary_lines.append(line)
        self.summary_path.write_text("\n".join(self.summary_lines))

    def sample(self, key: str, event: Any):
        self.sample_data[key] = to_serializable(event)
        self.sample_path.write_text(json.dumps(self.sample_data, indent=2, default=str))

    def close(self):
        try:
            self._raw_f.close()
        except Exception:
            pass
        log.info("Wrote output to %s", self.out_dir)


# ---------------------------------------------------------------------------
# Event collection helper
# ---------------------------------------------------------------------------

async def collect_events(
    listen_iter: AsyncIterator[Any],
    timeout_s: float,
    max_count: int | None = None,
    cancel_grace_s: float = 2.0,
    event_filter=None,
) -> list[Any]:
    """Drain an async event generator for up to timeout_s seconds, or until
    max_count events collected, whichever comes first.

    Written to be hang-proof: some websocket transports don't respond
    cleanly to asyncio cancellation mid-read (this is what caused the
    original hang — Candle events for the in-progress/current bar keep
    streaming indefinitely, so the drain loop never exits on its own, and
    a plain `asyncio.wait_for(...)` can then block forever past its
    timeout waiting for a cancellation that never completes). Here we
    shield the draining task from wait_for's own cancellation, cancel it
    ourselves on timeout, give it a short grace period to actually stop,
    and if it still hasn't, we simply stop awaiting it rather than block
    forever. An abandoned task can't hold up the rest of the script; at
    worst you'll see a harmless "task was destroyed but it is pending"
    warning when the process exits.
    """
    events: list[Any] = []

    async def _drain():
        async for ev in listen_iter:
            if event_filter is not None and not event_filter(ev):
                continue
            events.append(ev)
            if max_count is not None and len(events) >= max_count:
                return

    task = asyncio.ensure_future(_drain())
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)
    except asyncio.TimeoutError:
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=cancel_grace_s)
        except asyncio.CancelledError:
            pass  # expected — cancellation completed cleanly, this is success
        except asyncio.TimeoutError:
            log.warning(
                "Event drain didn't stop within %.1fs of cancellation — "
                "abandoning it and moving on.",
                cancel_grace_s,
            )
        except Exception as e:
            log.warning("Unexpected error while cancelling event drain: %s", e)
    except asyncio.CancelledError:
        pass  # e.g. shield itself got cancelled from further up — not our concern here
    except Exception:
        pass
    return events


# ---------------------------------------------------------------------------
# Generic hang-proof await wrapper
# ---------------------------------------------------------------------------

async def with_timeout(coro, timeout_s: float, label: str, cancel_grace_s: float = 2.0):
    """Await `coro` with a hang-proof timeout — same rationale as
    collect_events above, but for single awaits like subscribe()/
    unsubscribe() calls rather than a draining loop. Some of those calls
    can themselves hang if the SDK internally waits on a server
    confirmation that never arrives for a given symbol/period combo.
    Raises asyncio.TimeoutError on timeout (after abandoning the stuck
    task), or re-raises whatever exception the coroutine itself raised.
    """
    task = asyncio.ensure_future(coro)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=timeout_s)
    except asyncio.TimeoutError:
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=cancel_grace_s)
        except asyncio.CancelledError:
            pass  # expected — cancellation completed cleanly
        except asyncio.TimeoutError:
            log.warning(
                "%s didn't stop within %.1fs of cancellation — abandoning it.",
                label, cancel_grace_s,
            )
        except Exception as e:
            log.warning("Unexpected error while cancelling %s: %s", label, e)
        raise asyncio.TimeoutError(f"{label} timed out after {timeout_s}s")


# ---------------------------------------------------------------------------
# Candle depth probe
# ---------------------------------------------------------------------------

async def probe_candle_depth(
    streamer,
    Candle,
    symbol: str,
    label: str,
    period: str,
    start_time: datetime,
    rec: Recorder,
    timeout_s: float = 25.0,
    sub_timeout_s: float = 10.0,
) -> dict:
    """Subscribe to Candle events for `symbol` at `period` going back to
    `start_time`, collect what comes back, then unsubscribe."""
    result = {
        "label": label,
        "symbol": symbol,
        "period": period,
        "requested_start": start_time.isoformat(),
        "bar_count": 0,
        "earliest_bar": None,
        "latest_bar": None,
        "has_iv": False,
        "error": None,
    }

    sub_symbol = f"{symbol}{{={period}}}"
    rec.summary(f"  [{label}] subscribing (period={period}, start={start_time.date()})...")

    subscribed_via = None
    try:
        sig = inspect.signature(streamer.subscribe_candle)
        params = sig.parameters
        # Try to match whatever this SDK version calls its args.
        symbol_param = next((p for p in ("symbols", "symbol") if p in params), None)
        candle_type_param = next(
            (p for p in ("interval", "candle_type", "period") if p in params), None
        )
        start_param = next(
            (p for p in ("start_time", "from_time") if p in params), None
        )
        if symbol_param and candle_type_param and start_param:
            call_args = {symbol_param: [symbol], candle_type_param: period, start_param: start_time}
            log.info("Using subscribe_candle(%s)", call_args)
            await with_timeout(
                streamer.subscribe_candle(**call_args), sub_timeout_s, f"{label} subscribe_candle()"
            )
            subscribed_via = "subscribe_candle"
        else:
            raise TypeError(
                f"subscribe_candle signature not recognized: {sig}. "
                "Falling back to raw symbol-suffix subscription."
            )
    except Exception as e:
        rec.summary(f"  [{label}] subscribe_candle() unavailable/failed ({e}) — trying fallback subscribe()...")
        try:
            await with_timeout(
                streamer.subscribe(Candle, [sub_symbol]), sub_timeout_s, f"{label} subscribe()"
            )
            subscribed_via = "subscribe_fallback"
        except Exception as e2:
            result["error"] = f"Both subscribe_candle and generic subscribe failed/timed out: {e2}"
            rec.summary(f"  [{label}] ERROR: {result['error']}")
            return result

    rec.summary(f"  [{label}] subscribed via {subscribed_via}, collecting events (up to {timeout_s}s)...")

    events = await collect_events(
        streamer.listen(Candle),
        timeout_s=timeout_s,
        max_count=3000,  # safety net: an in-progress bar can otherwise stream forever
        event_filter=lambda ev: symbol in str(get_attr_any(ev, "event_symbol", default="")),
    )
    for ev in events:
        rec.raw_event(f"candle:{label}:{symbol}", ev)

    rec.summary(f"  [{label}] collected {len(events)} event(s), unsubscribing...")

    if not events:
        result["error"] = "No events received (empty response or subscription mismatch)"
    else:
        times = []
        for ev in events:
            t = get_attr_any(ev, "time", "index")
            if t:
                times.append(t)
            iv = get_attr_any(ev, "imp_volatility", "implied_volatility", "iv")
            if iv:
                result["has_iv"] = True
        result["bar_count"] = len(events)
        if times:
            result["earliest_bar"] = datetime.fromtimestamp(
                min(times) / 1000, tz=timezone.utc
            ).isoformat()
            result["latest_bar"] = datetime.fromtimestamp(
                max(times) / 1000, tz=timezone.utc
            ).isoformat()
        rec.sample(f"candle_example:{label}", events[0])

    try:
        await with_timeout(
            streamer.unsubscribe(Candle, [sub_symbol]), sub_timeout_s, f"{label} unsubscribe()"
        )
    except Exception as e:
        rec.summary(f"  [{label}] unsubscribe didn't complete cleanly ({e}) — continuing anyway, this is best-effort cleanup")

    rec.summary(
        f"  [{label}] DONE period={period} requested_start={start_time.date()} -> "
        f"bars={result['bar_count']} "
        f"earliest={result['earliest_bar']} latest={result['latest_bar']} "
        f"has_iv={result['has_iv']} error={result['error']}"
    )
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", default="SPY", help="Underlying ticker to test (default SPY)")
    parser.add_argument("--use-sandbox", action="store_true", help="Use TastyTrade sandbox instead of prod")
    parser.add_argument("--skip-expired-test", action="store_true", help="Skip the experimental expired-contract probe")
    parser.add_argument("--out-dir", default=None, help="Output directory (default: ./task0_output/<timestamp>)")
    args = parser.parse_args()

    if load_dotenv:
        load_dotenv()

    out_dir = Path(args.out_dir) if args.out_dir else Path(
        f"task0_output/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{args.ticker}"
    )
    rec = Recorder(out_dir)

    # --- Imports deferred so --help works without the SDK installed ---
    from tastytrade import Session, DXLinkStreamer
    from tastytrade.dxfeed import Candle, Greeks, Quote
    from tastytrade.instruments import get_option_chain
    try:
        from tastytrade.utils import get_tasty_monthly, today_in_new_york
    except ImportError:
        get_tasty_monthly = None
        today_in_new_york = None

    # --- Auth ---
    client_secret = os.getenv("TASTYTRADE_CLIENT_SECRET")
    refresh_token = os.getenv("TASTYTRADE_REFRESH_TOKEN")
    username = os.getenv("TASTYTRADE_USERNAME")
    password = os.getenv("TASTYTRADE_PASSWORD")

    rec.summary(f"# Task 0 Findings — {args.ticker} — {datetime.now().isoformat()}\n")

    if client_secret and refresh_token:
        rec.summary("Auth method: OAuth2 (client_secret + refresh_token)")
        session = Session(client_secret, refresh_token, is_test=args.use_sandbox)
    elif username and password:
        rec.summary("Auth method: username/password")
        session = Session(username, password, is_test=args.use_sandbox)
    else:
        raise SystemExit(
            "No credentials found. Copy .env.example to .env and fill in either "
            "TASTYTRADE_CLIENT_SECRET+TASTYTRADE_REFRESH_TOKEN or "
            "TASTYTRADE_USERNAME+TASTYTRADE_PASSWORD."
        )
    rec.summary(f"Sandbox: {args.use_sandbox}\n")

    # --- Resolve option chain & pick candidate contracts ---
    chain_result = get_option_chain(session, args.ticker)
    chain = await chain_result if inspect.isawaitable(chain_result) else chain_result

    expirations = sorted(chain.keys())
    if not expirations:
        raise SystemExit(f"No option chain returned for {args.ticker}")

    today = today_in_new_york() if today_in_new_york else date.today()
    nearest_exp = expirations[0]
    week_out_exp = next((e for e in expirations if (e - today).days >= 5), expirations[min(2, len(expirations) - 1)])
    month_candidates = [e for e in expirations if (e - today).days >= 25]
    month_out_exp = month_candidates[0] if month_candidates else expirations[-1]

    rec.summary(f"Today (NY): {today}")
    rec.summary(f"Available expirations found: {len(expirations)} (first: {expirations[0]}, last: {expirations[-1]})")
    rec.summary(f"Candidates chosen: nearest={nearest_exp} (0DTE if == today: {nearest_exp == today}), "
                f"week_out={week_out_exp}, month_out={month_out_exp}\n")

    def pick_mid_call(exp):
        options = [o for o in chain[exp] if get_attr_any(o, "option_type", "type") in ("C", "Call", "CALL")]
        if not options:
            options = chain[exp]
        options = sorted(options, key=lambda o: float(get_attr_any(o, "strike_price", "strike", default=0)))
        return options[len(options) // 2]

    candidates = {
        "nearest_expiration": pick_mid_call(nearest_exp),
        "week_out": pick_mid_call(week_out_exp),
        "month_out": pick_mid_call(month_out_exp),
    }

    try:
        await run_probes(
            session, args, today, candidates,
            nearest_exp, week_out_exp, month_out_exp, rec,
        )
    finally:
        rec.close()
        print(f"\nOutput written to: {out_dir.resolve()}")
        print("(This happens incrementally too, so even if you had to kill the "
              "process partway through, summary.md and sample_data.json should "
              "still have whatever was collected up to that point.)")
        print("Please share summary.md (and sample_data.json if you're comfortable) back in chat.")


async def run_probes(session, args, today, candidates, nearest_exp, week_out_exp, month_out_exp, rec: Recorder):
    """Runs all live-sample and candle-depth probes over one DXLink session.
    Pulled out of main() so main() can guarantee cleanup (Recorder.close(),
    final messaging) via try/finally regardless of how this exits — including
    a Ctrl+C or an unhandled error partway through."""
    from tastytrade import DXLinkStreamer
    from tastytrade.dxfeed import Candle, Greeks, Quote

    async with DXLinkStreamer(session) as streamer:
        # --- Live sample data: underlying quote ---
        await streamer.subscribe(Quote, [args.ticker])
        underlying_quotes = await collect_events(streamer.listen(Quote), timeout_s=8.0, max_count=1)
        if underlying_quotes:
            rec.sample("underlying_quote", underlying_quotes[0])
            rec.raw_event("quote:underlying", underlying_quotes[0])
            rec.summary(f"Live underlying quote sample captured for {args.ticker}.")
        await streamer.unsubscribe(Quote, [args.ticker])

        # --- Live sample data: one call contract's Quote + Greeks ---
        sample_contract = candidates["nearest_expiration"]
        sample_symbol = sample_contract.streamer_symbol
        await streamer.subscribe(Quote, [sample_symbol])
        await streamer.subscribe(Greeks, [sample_symbol])
        opt_quotes = await collect_events(streamer.listen(Quote), timeout_s=8.0, max_count=1)
        opt_greeks = await collect_events(streamer.listen(Greeks), timeout_s=8.0, max_count=1)
        if opt_quotes:
            rec.sample("option_quote_example", opt_quotes[0])
            rec.raw_event("quote:option", opt_quotes[0])
        if opt_greeks:
            rec.sample("option_greeks_example", opt_greeks[0])
            rec.raw_event("greeks:option", opt_greeks[0])
        rec.summary(f"Live option quote/greeks sample captured for {sample_symbol}.\n")
        await streamer.unsubscribe(Quote, [sample_symbol])
        await streamer.unsubscribe(Greeks, [sample_symbol])

        # --- Candle depth probes ---
        rec.summary("## Candle Depth Results\n")
        now = datetime.now(timezone.utc)
        results = []
        for label, contract in candidates.items():
            symbol = contract.streamer_symbol
            strike = get_attr_any(contract, "strike_price", "strike")
            rec.summary(f"### {label}: {symbol} (expiration in chain: {[e for e in [nearest_exp, week_out_exp, month_out_exp]]}, strike~{strike})")

            r1 = await probe_candle_depth(
                streamer, Candle, symbol, f"{label}_1m", "1m", now - timedelta(days=730), rec
            )
            r2 = await probe_candle_depth(
                streamer, Candle, symbol, f"{label}_1d", "1d", now - timedelta(days=730), rec
            )
            results.append(r1)
            results.append(r2)
            rec.summary("")

        # --- Experimental: already-expired contract probe ---
        if not args.skip_expired_test:
            rec.summary("## Experimental: Already-Expired Contract Probe\n")
            rec.summary(
                "This reuses the *nearest expiration* contract above. If that "
                "expiration is today (0DTE) and you're running this after market "
                "close, its candle history (if any survives past expiration) was "
                "already captured in the 'nearest_expiration' results above — "
                "check whether bars exist there with a latest_bar timestamp on or "
                "before today's close. If the nearest expiration was NOT today, "
                "this section doesn't give a clean signal — rerun this script "
                "shortly after a 0DTE contract on this ticker has expired for a "
                "definitive answer.\n"
            )
            rec.summary(f"nearest_expiration was: {nearest_exp} (today: {today})")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(
            "\nInterrupted. Since results are written incrementally, check the "
            "task0_output/... directory printed above (or the most recent one) — "
            "summary.md and sample_data.json should have whatever was captured "
            "up to that point."
        )
