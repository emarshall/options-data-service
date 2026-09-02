"""
Underlying candle retention probe — determines empirically how far back
TastyTrade's DXLink feed actually retains 1-minute Candle history for an
UNDERLYING ticker (an index or ETF), as opposed to an option contract.

WHY THIS EXISTS
----------------
See PLAN.md Section 7. Task 0's original candle-depth spike
(scripts/task0_spike/) measured ~6 weeks (~43 days) of 1-minute candle
retention — but that spike only ever tested OPTION CONTRACTS. Two
follow-up bugs in this codebase (request_candles' max_count/timeout_s,
then its idle_timeout_s) were both found and fixed on the assumption that
underlying retention matches that same ~43-day figure — but after both
fixes, backfill/gap-reconcile are still not recovering some underlyings'
older data. The next most likely explanation: underlying retention is
genuinely shorter (or just different) than option retention, which this
codebase has never actually measured. This script measures it directly,
the same way Task 0 measured it for options.

WHAT THIS DOES
--------------
Uses the real, production `TastyTradeSource.request_candles()` — the
exact same code path `BackfillJob` uses, not a reimplementation — with
deliberately generous `timeout_s`/`idle_timeout_s`/`max_count` overrides.
That matters: it means any truncation observed here can only be a genuine
server-side/feed limit, not one of this codebase's own tunable safety
caps (which is exactly what the previous two bug investigations couldn't
rule out without a probe like this).

For a range of `start_time`s going progressively further back (default:
15, 30, 43, 60, 90, 180, 365 days), it requests 1-minute candles for
`--ticker` and reports the earliest bar actually returned for each.

- If every request (past some point) returns the SAME earliest bar
  regardless of how much further back it asked, that's strong evidence
  of a hard retention wall at that date — i.e. a real, fixed limit, not a
  code bug.
- If asking further back keeps returning more history (an earlier
  earliest bar each time, all the way out to the oldest request tested),
  retention is longer than this script found evidence of, and something
  else in this codebase still explains the original report.

Doesn't touch config.yaml or the app's database at all — read-only
against TastyTrade, nothing written anywhere except this script's own
output file.

NOTE: earlier runs of this script took roughly 15 minutes *per lookback
window* (nearly the full 900s probe timeout, every single time) — that
was itself a real bug in `request_candles()` (a continuously-quoted
underlying's full historical replay is apparently delivered slowly/
trickled, never triggering the idle-timeout early-exit), now fixed with a
"stop once caught up to live" condition. This script calls the same
production `request_candles()`, so it inherits that fix automatically —
each window should now take roughly as long as the data itself takes to
transfer, not 15 minutes regardless.

USAGE
-----
    python -m scripts.underlying_retention_probe --ticker NDX
    python -m scripts.underlying_retention_probe --ticker VIX --lookback-days 10,20,30,45,60

Requires .env (TastyTrade creds) — see scripts/task0_spike/README.md for
how to obtain them if you don't already have one from running Task 0's
spike.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

log = logging.getLogger("underlying_retention_probe")

# Generous on purpose — this script exists specifically to rule out this
# codebase's own request_candles() safety limits as the explanation, so
# they need to be set high enough that hitting them here would itself be
# newsworthy (and is checked for explicitly below), not just "the usual
# tuning for a routine backfill run."
PROBE_TIMEOUT_S = 900.0
PROBE_IDLE_TIMEOUT_S = 30.0
PROBE_MAX_COUNT = 200_000

DEFAULT_LOOKBACK_DAYS = [15, 30, 43, 60, 90, 180, 365]


def _parse_lookback_days(raw: str) -> list[int]:
    try:
        days = sorted({int(x.strip()) for x in raw.split(",") if x.strip()})
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"expected comma-separated integers, got {raw!r}") from e
    if not days:
        raise argparse.ArgumentTypeError("at least one lookback value is required")
    return days


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ticker", required=True, help="Underlying ticker to probe, e.g. NDX, VIX, SPX")
    parser.add_argument(
        "--lookback-days", type=_parse_lookback_days, default=DEFAULT_LOOKBACK_DAYS,
        help="Comma-separated list of day-counts to test, e.g. '15,30,43,60,90'. "
             f"Default: {','.join(map(str, DEFAULT_LOOKBACK_DAYS))}",
    )
    parser.add_argument("--out-dir", default=None, help="Output directory (default: ./retention_probe_output/<timestamp>_<ticker>)")
    args = parser.parse_args()

    from service.config.settings import get_settings
    from service.logging_config import configure_logging
    from service.sources.tastytrade import TastyTradeSource

    configure_logging("underlying_retention_probe", "INFO")

    settings = get_settings()
    if not (settings.tastytrade_client_secret and settings.tastytrade_refresh_token):
        raise SystemExit(
            "TASTYTRADE_CLIENT_SECRET / TASTYTRADE_REFRESH_TOKEN not set — see "
            "scripts/task0_spike/README.md for how to obtain them."
        )

    out_dir = Path(args.out_dir) if args.out_dir else Path(
        f"retention_probe_output/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{args.ticker}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    source = TastyTradeSource(
        client_secret=settings.tastytrade_client_secret,
        refresh_token=settings.tastytrade_refresh_token,
        use_sandbox=settings.tastytrade_use_sandbox,
    )

    results = []
    summary_lines = [
        f"# Underlying Candle Retention Probe — {args.ticker} — {datetime.now(timezone.utc).isoformat()}",
        "",
        f"Sandbox: {settings.tastytrade_use_sandbox}",
        f"Lookback windows tested (days): {args.lookback_days}",
        f"Probe overrides: timeout_s={PROBE_TIMEOUT_S}, idle_timeout_s={PROBE_IDLE_TIMEOUT_S}, "
        f"max_count={PROBE_MAX_COUNT}",
        "",
        "## Results",
        "",
        "| requested lookback (days) | requested start | earliest bar returned | latest bar returned "
        "| bar count | hit max_count? |",
        "|---:|---|---|---|---:|---|",
    ]

    try:
        log.info("Authenticating...")
        await source.authenticate()

        now = datetime.now(timezone.utc)
        for lookback_days in args.lookback_days:
            start_time = now - timedelta(days=lookback_days)
            log.info(
                "Requesting %s 1m candles from %d day(s) back (%s)...",
                args.ticker, lookback_days, start_time.date(),
            )
            candles = await source.request_candles(
                args.ticker, "1m", start_time,
                timeout_s=PROBE_TIMEOUT_S, idle_timeout_s=PROBE_IDLE_TIMEOUT_S, max_count=PROBE_MAX_COUNT,
            )

            times = []
            for c in candles:
                ms = getattr(c, "time", None)
                if ms is not None:
                    times.append(datetime.fromtimestamp(ms / 1000, tz=timezone.utc))

            earliest = min(times) if times else None
            latest = max(times) if times else None
            hit_cap = len(candles) >= PROBE_MAX_COUNT

            result = {
                "lookback_days": lookback_days,
                "requested_start": start_time.isoformat(),
                "earliest_bar": earliest.isoformat() if earliest else None,
                "latest_bar": latest.isoformat() if latest else None,
                "bar_count": len(candles),
                "hit_max_count": hit_cap,
            }
            results.append(result)

            summary_lines.append(
                f"| {lookback_days} | {start_time.date()} | {earliest.date() if earliest else 'NONE'} "
                f"| {latest.date() if latest else 'NONE'} | {len(candles)} | {'**YES**' if hit_cap else 'no'} |"
            )
            log.info(
                "  -> earliest=%s latest=%s bar_count=%d%s",
                earliest, latest, len(candles), " [HIT MAX_COUNT]" if hit_cap else "",
            )

        # --- Interpretation ---
        summary_lines += ["", "## Interpretation", ""]
        any_hit_cap = any(r["hit_max_count"] for r in results)

        # A request only tells us anything about a *wall* when the server
        # refused to go back as far as it was asked — i.e. earliest_bar is
        # measurably later than requested_start. When they're equal (or
        # very close), that request simply got everything it asked for and
        # hasn't found a limit yet; comparing *those* against each other
        # (comparing each request's own requested_start-driven earliest,
        # which trivially differs across different requested_starts) is
        # exactly the mistake this script's own first version made,
        # producing a false "no wall found" conclusion from data that
        # actually showed a clear, consistent wall among the *other*
        # requests. See PLAN.md Section 7 for the incident.
        WALL_TOLERANCE = timedelta(days=1)
        wall_hits = []
        for r in results:
            if not r["earliest_bar"]:
                continue
            earliest = datetime.fromisoformat(r["earliest_bar"])
            requested = datetime.fromisoformat(r["requested_start"])
            if earliest - requested > WALL_TOLERANCE:
                wall_hits.append((r["lookback_days"], earliest))
        wall_dates = {e.date() for _, e in wall_hits}

        if any_hit_cap:
            summary_lines.append(
                "**At least one request hit `max_count` despite the generous probe override "
                f"({PROBE_MAX_COUNT}) — this result set may itself be truncated. Re-run with an "
                "even larger --lookback-days floor removed or investigate further before trusting "
                "the table above.**"
            )
        elif not wall_hits:
            summary_lines.append(
                "Every request returned data going back exactly as far as it asked (earliest bar "
                "== requested start in each case) — no retention wall found within the tested "
                "range. Try larger --lookback-days values to find where one actually appears."
            )
        elif len(wall_dates) == 1:
            wall_date = next(iter(wall_dates))
            hit_at = min(d for d, _ in wall_hits)
            summary_lines.append(
                f"Requests asking further back than **{wall_date}** consistently got refused "
                f"exactly at that date (first seen at --lookback-days={hit_at}), while shorter "
                f"requests correctly got everything they asked for. This is strong evidence of a "
                f"real, fixed retention wall for {args.ticker} at **{wall_date}** — not a bug in "
                "this codebase's own request limits (which were deliberately set very high for "
                "this probe)."
            )
            summary_lines.append("")
            summary_lines.append(
                "**Next step:** if this date doesn't match `RETENTION_DAYS` in "
                "`service/ingestion/gap_detection.py` (currently used as a shared ~6-week "
                "assumption for both option contracts and underlyings), update it to reflect "
                "this measurement — see that constant's own comment for where it's used."
            )
        else:
            summary_lines.append(
                f"Requests that did hit a wall disagreed on the date ({sorted(wall_dates)}) — "
                "that's not a simple fixed retention wall. Worth re-running (a moving/rolling "
                "window that shifted slightly between requests could explain small differences) "
                "or investigating further before drawing a conclusion from this."
            )

    finally:
        await source.close()

    summary_path = out_dir / "summary.md"
    summary_path.write_text("\n".join(summary_lines))
    (out_dir / "results.json").write_text(json.dumps(results, indent=2, default=str))

    print("\n" + "\n".join(summary_lines))
    print(f"\nOutput written to: {out_dir.resolve()}")
    print("Please share summary.md (and/or results.json) back in chat.")


if __name__ == "__main__":
    asyncio.run(main())
