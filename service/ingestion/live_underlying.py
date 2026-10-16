"""
Near-real-time underlying poller for tickers that have NO live Quote feed
(VIX, NDX).

**Why this exists.** TastyTrade's DXFeed publishes live `Quote` events for
`SPX` but not for `NDX` or `VIX` (verified against the live feed 2026-09-30 —
see `scheduler.py`'s module docstring and PLAN.md Section 7, "NDX and VIX have
no live Quote feed"). For those two tickers the daily after-hours `backfill`
(the `scheduler` service, 16:30 ET) was the ONLY data source, so their 1m bars
only appeared once a day, well after the close. That is fine for building
Greeks history, but useless for anything that needs to see the session as it
happens — e.g. the entry-signal monitor in the sibling `zerodte-backtester`
repo, which watches VIX intraday.

**Why not just run `backfill` every minute.** The full backfill re-requests
candle history for every tracked option contract (SPX/NDX daily 0DTE listings
over the lookback window — a couple hundred contracts) plus each underlying;
one pass is minutes-to-hours and repeatedly hammering the feed for that is
both slow and wasteful. The slice we actually need is tiny: one or two
underlyings, the last few minutes. This service is exactly that slice. It
calls the same `MarketDataSource.request_candles` and writes the same
`UnderlyingBar1m` rows through the same skip-if-exists rule as
`BackfillJob._backfill_underlying`, so its output is indistinguishable from a
backfill's — just produced continuously through the session instead of once a
day.

**How a "live" pull works despite a historical-candle API.** `request_candles`
subscribes to a symbol's candle stream, replays history from `start_time`, then
keeps re-delivering the *current, in-progress* candle as new ticks arrive (see
its docstring). So the most recent candle it hands back tracks the live minute,
a few seconds behind. With a short lookback window the history replay is a
handful of candles and collection stops almost immediately — the repeated
live candle trips `stop_on_repeated_key` — so a poll costs a couple of seconds,
not minutes. Writes are idempotent (skip existing `(time, ticker)` rows), so
the same closed minutes returned on every poll are simply skipped; only
genuinely new minutes are inserted.

**Caveat on `start_time` (measured, not assumed).** The candle replay runs from
`start_time` *through to the latest available candle* — it is NOT bounded at
`start_time + lookback`. A probe on 2026-10-10 requesting `start_time` back on
2026-09-02 replayed the entire Sep 2 → Oct 9 span (~8,001 bars). That only
matters for testing with `--as-of` in the past (which makes the window span
from that past moment to the newest stored bar). In normal operation
`start_time = now - lookback` and "now" is inside the current session, so the
replay is just `lookback` minutes plus the live tail — cheap. Don't judge poll
cost from an `--as-of` run against an old date.

**Deliberately narrow.** Only the tickers passed via `--tickers` (default
`VIX,NDX`, overridable with the `LIVE_UNDERLYING_TICKERS` env var) are polled,
and only during the session window. Everything else about the data stays the
daily backfill's job. Run it as the `underlying-live` compose service.

**The trade path is the default source.** The poller aggregates aligned-UTC 1m
OHLCV bars from the continuous `Trade` subscription (`--source trade`), so the
newest bar tracks the live minute directly and neither it nor the daily backfill
ever writes a bar with no usable close. This default was adopted after the
DXLink `Candle` feed briefly delivered zeroed index candles on 2026-10-12
(OHLC all 0.0), and a zeroed row is permanent under skip-if-exists — a later
healthy source could never repair it. The `candle` request path stays available
via `--source candle` and is what the startup catch-up below uses on every boot;
it was re-verified healthy on 2026-10-13 (one request returned the full
13:30-onward NDX series), so it can still replay history the live-only trade
path cannot.

**Startup catch-up (why candles still matter despite the trade path).** The
trade path is live-only: it can write *only* the minutes that elapse while the
process is up. So a mid-session restart — a redeploy, a crash, a host reboot —
permanently loses every minute before the restart: a poller started at 11:00 MT
has no NDX bars from the morning session at all, even though the Candle feed
would happily replay them (measured 2026-10-13: a poll started at 17:01 UTC left
NDX with zero bars before 16:46 UTC, while a plain `request_candles` call for
the same day returned the full 13:30-onward series). To close that gap the
poller runs a **one-time candle catch-up per ticker on startup**
(`catch_up_ticker`), replaying from the newest stored bar (floored to a bounded
lookback so a long-dormant ticker can't trigger an unbounded replay) through to
now, while it settles into the routine trade-aggregation loop.

That catch-up runs as a **background task**, not a blocking step before the
loop, and its writes are serialized against the trade writer with a lock (both
insert into `underlying_bars_1m`). Two reasons it can't run inline: (1) the
instrument's candle feed lags wall-clock by ~13-16 min, so the catch-up's live
tail never matches `request_candles`' recency gate and the pull runs to its
full timeout instead of stopping early; and (2) the live-only trade path must
start capturing immediately, because it — not the candles — is the only source
for the most recent minutes. Blocking on the catch-up would lose exactly the
minutes the catch-up can't yet provide.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, time, timedelta, timezone

from service.config.settings import get_settings
from service.db.models import UnderlyingBar1m
from service.db.session import check_connection, get_session_factory
from service.ingestion.backfill import BackfillJob
from service.logging_config import configure_logging
from service.sources._async_utils import close_ok, get_attr_any, with_timeout
from service.sources.tastytrade import TastyTradeSource

# Explicit name, not `__name__`: this module is launched as
# `python -m service.ingestion.live_underlying`, which runs it under `__main__`
# — a different logger from the `"service.ingestion.live_underlying"` one that
# `configure_logging` raises to the configured level. Same reasoning as
# `backfill.py`'s logger.
log = logging.getLogger("service.ingestion.live_underlying")

# Per-poll candle-request timeouts. Much smaller than the backfill's
# underlying timeouts (`_UNDERLYING_TIMEOUT_S`/`_UNDERLYING_IDLE_TIMEOUT_S` =
# 1800s/60s), which are sized for a full multi-week replay. Here the window is
# a few minutes, so the replay finishes fast and `stop_on_repeated_key` ends
# the request as soon as the live candle repeats. These are just safety caps:
# a poll should normally return in a couple of seconds.
_POLL_TIMEOUT_S = 60.0
_POLL_IDLE_TIMEOUT_S = 8.0

# Startup catch-up timeouts. Generous idle timeout, like the daily backfill's
# underlying request, so a large replay isn't truncated by a delivery pause.
# But NOT the backfill's 1800s total: the catch-up runs as a *background* task
# (see run()) and has no reliable early stop during market hours (the delayed
# NDX/VIX candle's live tail never trips the recency gate — see
# catch_up_ticker), so the total timeout is a hard ceiling on how long each
# ticker's catch-up lingers, not just a safety net. 120s comfortably covers
# the worst case (catch_up_hours=8 -> <=480 bars; a measured full-session pull
# delivered ~450 bars within seconds) while keeping a hung pull short.
_CATCH_UP_TIMEOUT_S = 120.0
_CATCH_UP_IDLE_TIMEOUT_S = 60.0

# Trade-drain watchdog. The drain's own deadline (see poll_ticker_trades)
# only fires when an event *arrives* — a socket the server closed silently
# yields no events and no exception, so `async for` on `listen()` blocks
# forever with the poller wedged and zero bars written (observed 2026-10-16:
# the DXLink server dropped the connection, `subscribe` raised one
# LocalProtocolError, and every subsequent drain hung undetected for the
# rest of the session). The drain therefore gets a hard ceiling of
# `listen_s + _DRAIN_GRACE_S`: a quiet-but-healthy minute is expected
# (trades are sparse), but a full listen window past the deadline with no
# events at all means the streamer is presumed dead, and the next poll
# reconnects from scratch. The grace is deliberately much larger than the
# poll interval — reconnecting is not free (auth + resubscribe), so it
# should only happen on real stalls, not on sparse ticks.
_DRAIN_GRACE_S = 30.0

DEFAULT_TICKERS = ("VIX", "NDX")


@asynccontextmanager
async def _no_lock():
    """Stand-in for `write_lock=None`: an async no-op context manager.

    Used instead of a module-level `asyncio.Lock()` because asyncio locks bind
    to the first running loop that touches them, which breaks the multi-event-
    loop test suite. run() creates the real lock per process; direct callers
    (tests, one-off scripts) get no lock.
    """
    yield


class _TradeBarAggregator:
    """Builds aligned-UTC 1m OHLCV bars from live `Trade` ticks.

    This was adopted because the DXLink `Candle` feed briefly went zeroed for
    index underlyings on 2026-10-12, and the `Trade` stream for the same
    symbols kept delivering real prices (VIX/NDX/SPX all confirmed) — a
    hedge against the feed, not a claim it is still broken. Trade ticks are
    live-only and sparse, so a per-poll short listen would miss most minutes;
    instead the poller keeps one continuous Trade subscription and feeds this
    aggregator, which exposes only *fully closed* minutes (minute start +
    60s <= now) for writing — same aligned `:00` labels the quote-aggregated
    SPX bars use, same idempotent skip-if-exists write rule.
    """

    def __init__(self) -> None:
        # ticker -> minute_start_ms -> {open, high, low, close, volume}
        self._buckets: dict[str, dict[int, dict]] = {}

    def feed(self, ticker: str, ev) -> None:
        price = get_attr_any(ev, "last", "price")
        t = get_attr_any(ev, "time")
        if price is None or t is None:
            return
        try:
            fprice = float(price)
        except (TypeError, ValueError):
            return
        if fprice <= 0 or fprice != fprice:
            return
        if isinstance(t, (int, float)):
            t_ms = int(t)
        else:
            t_ms = int(t.timestamp() * 1000)
        minute = t_ms - (t_ms % 60_000)
        b = self._buckets.setdefault(ticker, {}).setdefault(minute, {})
        b["open"] = b.get("open", fprice)
        b["high"] = fprice if "high" not in b else max(b["high"], fprice)
        b["low"] = fprice if "low" not in b else min(b["low"], fprice)
        b["close"] = fprice
        size = get_attr_any(ev, "size")
        if size is not None:
            try:
                b["volume"] = b.get("volume", 0) + int(size)
            except (TypeError, ValueError):
                pass

    def ready_bars(
        self, now_ms: int
    ) -> list[tuple[str, datetime, dict]]:
        """Completed-minute bars (minute_start + 60s <= now_ms), kept."""
        out: list[tuple[str, datetime, dict]] = []
        for ticker, minutes in self._buckets.items():
            for minute, b in minutes.items():
                if minute + 60_000 <= now_ms:
                    out.append(
                        (ticker, datetime.fromtimestamp(minute / 1000, tz=timezone.utc), dict(b))
                    )
        return out

    def drop_ready(
        self, now_ms: int
    ) -> list[tuple[str, datetime, dict]]:
        """ready_bars() that also removes them from the accumulator."""
        bars = self.ready_bars(now_ms)
        for ticker, bar_time, _ in bars:
            # _buckets is keyed by raw epoch-ms int, not by datetime —
            # recompute the int so the pop actually matches.
            minute_ms = int(bar_time.timestamp() * 1000)
            m = self._buckets.get(ticker, {})
            m.pop(minute_ms, None)
            if not m:
                self._buckets.pop(ticker, None)
        return bars


def parse_hhmm(value: str) -> time:
    """Parse an `HH:MM` 24h string into a naive `time` (UTC wall clock)."""
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def in_window(now: datetime, start: time, end: time, weekdays_only: bool) -> bool:
    """Whether `now` (a UTC-aware datetime) is inside the polling window.

    Compares UTC wall-clock time directly — no timezone conversion, matching
    the compose service's own "13:00/21:30 UTC" style for the monitor.
    """
    if weekdays_only and now.weekday() >= 5:
        return False
    return start <= now.time() <= end


async def poll_ticker(
    source: TastyTradeSource,
    session_factory,
    ticker: str,
    start_time: datetime,
    timeout_s: float = _POLL_TIMEOUT_S,
    idle_timeout_s: float = _POLL_IDLE_TIMEOUT_S,
    write_lock: asyncio.Lock | None = None,
) -> tuple[int, int, int]:
    """One candle pull + idempotent write for a single underlying.

    Returns `(written, skipped_existing, received)`. Mirrors
    `BackfillJob._backfill_underlying`'s write loop exactly (same model, same
    `session.get` skip rule) so live-polled rows are byte-identical in shape to
    backfilled ones.

    `timeout_s`/`idle_timeout_s` default to the tiny live-poll values; the
    startup catch-up (`catch_up_ticker`) passes the far larger backfill-sized
    ones so a whole-session replay isn't truncated.
    """
    candles = await source.request_candles(
        ticker, "1m", start_time,
        timeout_s=timeout_s, idle_timeout_s=idle_timeout_s,
    )
    if not candles:
        return 0, 0, 0

    written = 0
    skipped = 0
    skipped_invalid = 0
    # `write_lock` serializes this write against the trade-path writer when the
    # catch-up runs concurrently with the live loop — both insert into
    # `underlying_bars_1m`, and the check-then-insert below would otherwise
    # race into a duplicate-key error at the boundary minute they both cover.
    async with (write_lock if write_lock is not None else _no_lock()):
        async with session_factory() as session:
            for candle in candles:
                bar_time = BackfillJob._candle_time(candle)
                if bar_time is None:
                    continue
                existing = await session.get(UnderlyingBar1m, {"time": bar_time, "ticker": ticker})
                if existing is not None:
                    skipped += 1
                    continue
                close = get_attr_any(candle, "close")
                if not close_ok(close):
                    # A zeroed/absent candle (see close_ok's docstring — the
                    # live feed has been shipping them) would poison the row
                    # forever; refuse to write it so a later healthy source can.
                    skipped_invalid += 1
                    continue
                session.add(
                    UnderlyingBar1m(
                        time=bar_time,
                        ticker=ticker,
                        open=get_attr_any(candle, "open"),
                        high=get_attr_any(candle, "high"),
                        low=get_attr_any(candle, "low"),
                        close=close,
                        volume=get_attr_any(candle, "volume"),
                    )
                )
                written += 1
            await session.commit()
    if skipped_invalid:
        log.warning(
            "%s: skipped %d candle(s) with no usable close (feed zeroed?) — "
            "not writing them.",
            ticker, skipped_invalid,
        )
    return written, skipped, len(candles)


def _as_utc(dt: datetime) -> datetime:
    """Normalize a possibly-naive stored timestamp back to UTC-aware.

    SQLite (tests, and possible local dev) doesn't preserve tzinfo, so a value
    read back can be naive even though everything this codebase writes is UTC.
    """
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


async def _latest_bar_time(session_factory, ticker: str) -> datetime | None:
    """Newest stored 1m bar time for `ticker`, or None if it has none."""
    from sqlalchemy import func, select

    async with session_factory() as session:
        result = await session.execute(
            select(func.max(UnderlyingBar1m.time)).where(UnderlyingBar1m.ticker == ticker)
        )
        latest = result.scalar()
    return latest


async def catch_up_ticker(
    source: TastyTradeSource,
    session_factory,
    ticker: str,
    now: datetime,
    max_lookback_hours: float,
    write_lock: asyncio.Lock | None = None,
) -> int:
    """One-time candle replay to close the gap between `ticker`'s newest
    stored bar and `now`, returning bars written.

    The trade path can only write minutes that elapse while the process is up,
    so a mid-session restart loses everything before it (see the module
    docstring). This replays that stretch from the Candle feed instead — the
    same idempotent, refuse-zeroed-close write path as the live poll — which
    the feed retains for weeks.

    Start is the later of the newest stored bar and `now - max_lookback_hours`,
    so a long-dormant ticker can't trigger an unbounded replay while a restart
    that lost the morning still replays from the session open. A ticker already
    current to within a minute is a no-op.

    **No reliable early stop during market hours.** `request_candles`'
    `stop_on_repeated_key` only ends a pull when a *recent* key repeats
    (within 5 min of the call). NDX/VIX candle data lags wall-clock by
    ~13-16 min (the same delay the trade path sees), so their live candle
    looks "old" and the recency gate never fires — the request runs to
    `timeout_s` even though all the history arrived in the first seconds
    (measured 2026-10-13: a 20:05→now NDX pull returned its full series by
    20:39 and then spun for the whole 95s timeout). That is why the catch-up
    runs in the background rather than before the live loop: it must never
    block live capture, which would itself lose minutes the instrument's
    delayed feed can't replay yet.
    """
    latest = await _latest_bar_time(session_factory, ticker)
    floor = now - timedelta(hours=max_lookback_hours)
    start = floor if latest is None else max(_as_utc(latest), floor)
    if now - start < timedelta(minutes=1):
        log.info(
            "%s: catch-up not needed — newest bar %s is already current.",
            ticker, start.isoformat(),
        )
        return 0
    written, skipped, received = await poll_ticker(
        source, session_factory, ticker, start,
        timeout_s=_CATCH_UP_TIMEOUT_S, idle_timeout_s=_CATCH_UP_IDLE_TIMEOUT_S,
        write_lock=write_lock,
    )
    log.info(
        "%s: catch-up from %s wrote %d new bar(s) (%d received, %d already present)",
        ticker, start.isoformat(), written, received, skipped,
    )
    return written


async def _run_catch_ups(
    source: TastyTradeSource,
    session_factory,
    tickers: list[str],
    max_lookback_hours: float,
    write_lock: asyncio.Lock,
) -> None:
    """Catch up every ticker once, sequentially, swallowing per-ticker errors.

    Sequential because `request_candles` consumes the shared
    `streamer.listen(Candle)` stream — two concurrent candle pulls starve each
    other (one completes, the other runs to its full timeout with no events).
    A failure on one ticker must not stop the others, nor the live loop this
    runs alongside.
    """
    now = datetime.now(timezone.utc)
    for ticker in tickers:
        try:
            await catch_up_ticker(
                source, session_factory, ticker, now, max_lookback_hours, write_lock
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s: startup catch-up failed — continuing.", ticker)


async def poll_ticker_trades(
    source: TastyTradeSource,
    session_factory,
    aggregator: _TradeBarAggregator,
    tickers: list[str],
    listen_s: float,
    write_lock: asyncio.Lock | None = None,
) -> None:
    """One continuous Trade-stream drain + flush of completed minutes.

    Keeps the subscription alive across calls (the streamer stays
    connected), so ticks arriving between drain periods still accumulate in
    `aggregator`; this call only *listens* for `listen_s`, then flushes the
    minutes that have fully closed (minute start + 60s <= now).

    Failure contract: every streamer interaction is bounded by a timeout,
    and on any failure the dead streamer is dropped via
    `source._mark_disconnected()` so the *next* call rebuilds a fresh
    connection (same reconnect pattern as TastytradeSource._listen_loop).
    Nothing the aggregator already holds is lost on failure — completed
    minutes stay queued until the next successful call's flush.
    """
    from tastytrade.dxfeed import Trade

    ticker_set = set(tickers)
    streamer = await source._ensure_streamer()
    try:
        await with_timeout(
            streamer.subscribe(Trade, tickers), timeout_s=10.0, label="trade subscribe",
        )
    except Exception as e:
        # A failed send means the connection is already gone (or going) —
        # wsproto raises LocalProtocolError when the server has closed the
        # socket; observed 2026-10-16 at the first poll after an overnight
        # disconnect. Drop the dead streamer now so the next poll builds a
        # fresh one instead of reusing the corpse.
        log.warning("trade subscribe failed (%s) — reconnecting on the next poll.", e)
        await source._mark_disconnected()
        raise

    deadline = asyncio.get_event_loop().time() + listen_s

    async def _drain() -> None:
        async for ev in streamer.listen(Trade):
            sym = get_attr_any(ev, "event_symbol", default="")
            if sym in ticker_set:
                aggregator.feed(sym, ev)
            if asyncio.get_event_loop().time() >= deadline:
                break

    try:
        # Hard ceiling — see _DRAIN_GRACE_S. The deadline break inside the
        # loop only fires when an event *arrives*; a silently-closed socket
        # delivers neither events nor exceptions, so without this the drain
        # (and the whole poller) wedges forever. Anything the aggregator
        # already holds is not lost on failure: completed minutes stay
        # queued until the next successful poll's flush.
        await with_timeout(
            _drain(), timeout_s=listen_s + _DRAIN_GRACE_S, label="trade drain",
        )
    except Exception as e:
        # Timeout or listen failure: the streamer can't be trusted. The next
        # poll reconnects from scratch (fresh auth + resubscribe).
        log.warning(
            "trade drain failed (%s) — reconnecting on the next poll.", e,
        )
        await source._mark_disconnected()
        raise
    else:
        try:
            await with_timeout(
                streamer.unsubscribe(Trade, tickers), timeout_s=10.0, label="trade unsubscribe",
            )
        except Exception as e:
            log.warning("Trade unsubscribe for %s didn't complete cleanly: %s", tickers, e)

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    bar_rows = aggregator.drop_ready(now_ms)
    if not bar_rows:
        return
    written = 0
    skipped = 0
    skipped_invalid = 0
    # Serialize against the startup catch-up's candle writer (see poll_ticker):
    # both insert into `underlying_bars_1m`, and the delayed NDX/VIX feed means
    # the trade path's boundary minutes overlap what the catch-up replays.
    async with (write_lock if write_lock is not None else _no_lock()):
        async with session_factory() as session:
            for ticker, bar_time, b in bar_rows:
                existing = await session.get(
                    UnderlyingBar1m, {"time": bar_time, "ticker": ticker}
                )
                if existing is not None:
                    skipped += 1
                    continue
                if not close_ok(b.get("close")):
                    skipped_invalid += 1
                    continue
                session.add(
                    UnderlyingBar1m(
                        time=bar_time,
                        ticker=ticker,
                        open=b.get("open"),
                        high=b.get("high"),
                        low=b.get("low"),
                        close=b.get("close"),
                        volume=b.get("volume"),
                    )
                )
                written += 1
            await session.commit()
    if written:
        log.info(
            "trade bars: wrote %d new bar(s) (%d already present, %d invalid skipped) "
            "for %s",
            written, skipped, skipped_invalid, ", ".join(sorted(ticker_set)),
        )


async def run(args: argparse.Namespace) -> None:
    settings = get_settings()

    if not await check_connection():
        log.error("Could not connect to the database — check DATABASE_URL / that the DB is up.")
        raise SystemExit(1)
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        log.error(
            "No TastyTrade credentials configured (TASTYTRADE_CLIENT_SECRET / "
            "TASTYTRADE_REFRESH_TOKEN) — cannot poll without them."
        )
        raise SystemExit(1)

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    known = {tc.ticker for tc in settings.tickers}
    unknown = [t for t in tickers if t not in known]
    if unknown:
        log.warning(
            "Polling ticker(s) not present in config.yaml: %s. Polling anyway — "
            "the candle feed is keyed on the raw symbol, not the config.",
            unknown,
        )

    wstart = parse_hhmm(args.window_start)
    wend = parse_hhmm(args.window_end)

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )
    await source.authenticate()
    session_factory = get_session_factory()

    log.info(
        "live underlying poller starting. tickers=%s window=%s..%s UTC weekdays_only=%s "
        "interval=%.0fs lookback=%.0fmin catch_up_hours=%.1f",
        tickers, wstart, wend, args.weekdays_only, args.interval,
        args.lookback_min, args.catch_up_hours,
    )

    # One-time gap recovery for the minutes the live-only trade path can't
    # replay (see the module docstring). It runs as a *background* task rather
    # than before the loop: a catch-up pull has no reliable early stop during
    # market hours (the delayed NDX/VIX candle's live tail never matches the
    # recency gate — see catch_up_ticker), so awaiting it would stall live
    # capture for up to _CATCH_UP_TIMEOUT_S per ticker, losing exactly the
    # recent minutes this is meant to protect. The lock keeps its writes from
    # racing the trade writer's. Skipped for `--once` and `--no-catch-up`.
    write_lock = asyncio.Lock()
    catch_up_task: asyncio.Task | None = None
    if not args.once and not args.no_catch_up:
        catch_up_task = asyncio.create_task(
            _run_catch_ups(source, session_factory, tickers, args.catch_up_hours, write_lock)
        )
        if args.source == "candle":
            # `--source candle` makes the live loop consume the same
            # `streamer.listen(Candle)` stream the catch-up does; two concurrent
            # candle pulls race (one starves), so serialize in that mode. The
            # default trade path listens on a different event type and is safe
            # to run alongside — there the task stays in the background.
            await catch_up_task
            catch_up_task = None

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    try:
        trade_agg = _TradeBarAggregator()
        while not stop_event.is_set():
            now = datetime.fromisoformat(args.as_of) if args.as_of else datetime.now(timezone.utc)
            if args.once or in_window(now, wstart, wend, args.weekdays_only):
                if args.source == "trade":
                    if args.as_of:
                        log.warning(
                            "--as-of has no meaning on the trade path (trades are live-only, "
                            "not replayable) — polling the live stream regardless."
                        )
                    try:
                        # The drain itself paces the loop; only step out long
                        # enough to notice a stop signal afterwards.
                        await poll_ticker_trades(
                            source, session_factory, trade_agg, tickers,
                            listen_s=max(1.0, args.interval - 0.5),
                            write_lock=write_lock,
                        )
                    except Exception:
                        # One bad drain must not stop the feed watcher.
                        log.exception("trade poll failed — continuing.")
                else:
                    start = now - timedelta(minutes=args.lookback_min)
                    for ticker in tickers:
                        try:
                            written, skipped, received = await poll_ticker(
                                source, session_factory, ticker, start,
                                write_lock=write_lock,
                            )
                            if written:
                                log.info(
                                    "%s: wrote %d new bar(s) (%d received, %d already present)",
                                    ticker, written, received, skipped,
                                )
                            else:
                                log.debug(
                                    "%s: no new bars (%d received, %d already present)",
                                    ticker, received, skipped,
                                )
                        except Exception:
                            # One ticker failing must not stop the other, nor the loop.
                            log.exception("%s: poll failed — continuing.", ticker)
            else:
                log.debug("outside window (%s UTC) — skipping this tick", now.strftime("%H:%M"))

            if args.once:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=0.5 if args.source == "trade" else args.interval)
            except asyncio.TimeoutError:
                pass
    finally:
        if catch_up_task is not None and not catch_up_task.done():
            catch_up_task.cancel()
            try:
                await catch_up_task
            except asyncio.CancelledError:
                pass
        await source.close()
        log.info("live underlying poller stopped.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Poll recent 1m candles for underlyings with no live quote feed "
                    "(VIX/NDX) and write them to underlying_bars_1m continuously.",
    )
    p.add_argument(
        "--tickers",
        default=os.environ.get("LIVE_UNDERLYING_TICKERS", ",".join(DEFAULT_TICKERS)),
        help="Comma-separated tickers to poll (default: %(default)s).",
    )
    p.add_argument("--interval", type=float, default=60.0, help="Seconds between polls (default 60).")
    p.add_argument(
        "--lookback-min", type=float, default=4.0,
        help="Minutes of recent history to request each poll (default 4).",
    )
    p.add_argument(
        "--catch-up-hours", type=float,
        default=float(os.environ.get("LIVE_UNDERLYING_CATCH_UP_HOURS", "8")),
        help="On startup, replay candles from at most this many hours back "
             "through to now to close any gap left by downtime (default 8; "
             "a full session plus margin). Only the stretch after the newest "
             "stored bar is actually fetched. Set via "
             "LIVE_UNDERLYING_CATCH_UP_HOURS. See the module docstring.",
    )
    p.add_argument(
        "--no-catch-up", action="store_true",
        help="Skip the one-time startup catch-up (advance/backfill-only mode).",
    )
    p.add_argument("--window-start", default="13:25", help="UTC HH:MM poll window start (default 13:25).")
    p.add_argument("--window-end", default="20:20", help="UTC HH:MM poll window end (default 20:20).")
    p.add_argument("--all-days", dest="weekdays_only", action="store_false", help="Poll on weekends too.")
    p.add_argument(
        "--source", choices=("trade", "candle"), default="trade",
        help="Which live feed to build 1m bars from. 'trade' subscribes the "
             "DXLink Trade stream continuously and aggregates aligned minute "
             "bars directly from the live tape (default; never writes a zeroed "
             "bar). 'candle' uses the historical-candle request path — also "
             "what the daily backfill and the startup catch-up use.",
    )
    p.add_argument("--once", action="store_true", help="Poll once (ignoring the window) then exit — for testing.")
    p.add_argument(
        "--as-of", default=None,
        help="ISO-8601 UTC timestamp to treat as 'now' (testing) — pulls that moment's "
             "recent candles instead of the live wall clock. Use with --once.",
    )
    p.set_defaults(weekdays_only=True)
    return p


def main() -> None:
    args = build_parser().parse_args()
    configure_logging("service.ingestion.live_underlying", get_settings().log_level)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
