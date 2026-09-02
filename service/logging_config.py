"""
Shared logging setup for every entry point (ingestion, backfill,
greeks-backfill, api). Exists specifically to fix a real, confirmed bug:
excessive log volume that was severe enough to crash Docker on at least
one deployment (HDD-backed host, overwhelmed by the write I/O of
capturing container logs).

**Root cause, confirmed by reading the installed `tastytrade` package's
own source** (not a guess): `tastytrade/__init__.py` does

    logger = logging.getLogger(__name__)   # the "tastytrade" logger
    logger.setLevel(logging.DEBUG)

unconditionally, at import time. Every submodule (`tastytrade.streamer`,
`tastytrade.session`, `tastytrade.dxfeed.*`, ...) gets its logger via
`logging.getLogger(__name__)` too, and none of them set their own level —
so they all inherit DEBUG from that top-level "tastytrade" logger, not
from our own app's root-logger configuration. That matters because
`tastytrade/streamer.py` does this on **every single message received
over the websocket**:

    logger.debug("received message: %s", data)

`data` is the full raw decoded JSON payload for that message — one Quote,
Greeks, or Candle event, or a subscription/heartbeat frame. With
potentially hundreds of contracts subscribed, that's an enormous number
of DEBUG log records, each containing a full JSON dump, being *created*
regardless of what level our own app requested — because Python's logging
module resolves a logger's effective level by walking up to the nearest
*explicitly set* ancestor, and `tastytrade`'s own `setLevel(DEBUG)` sits
between `tastytrade.streamer` and root, shadowing whatever level our own
`logging.basicConfig()` call put on root. Our previous setup (a bare
`logging.basicConfig(level=logging.INFO)` in each entry point) never
actually reached that logger at all — this is why the volume looked like
"the app is logging every raw event," even though no `service/` module
ever does that.

**The fix:** explicitly set the `tastytrade` logger's level ourselves,
after import, which — because it's the same "explicitly set ancestor"
mechanism causing the problem — correctly overrides the level the package
sets on itself and applies to every one of its submodules at once. A few
other third-party loggers known to be chatty in similar ways (raw
HTTP/websocket frame logging) are clamped defensively alongside it, even
without the same level of confirmed evidence.
"""

from __future__ import annotations

import logging

# Loggers whose own package code sets a level on itself (bypassing our
# root-level config the same way `tastytrade` does above), or that are
# simply known to be very chatty at INFO. Clamped to WARNING regardless of
# our own app's configured level — there's no legitimate reason a
# deployment would want raw frame-by-frame dumps from a dependency, and if
# that's ever needed for debugging a library-level issue specifically,
# `logging.getLogger("tastytrade").setLevel(logging.DEBUG)` can be called
# ad hoc (e.g. in a one-off script) rather than needing it on by default.
_NOISY_THIRD_PARTY_LOGGERS = ("tastytrade", "httpx", "httpcore", "websockets", "hpack")


def configure_logging(app_logger_name: str, level: str = "INFO") -> None:
    """Call once, at process startup, before doing anything else that
    might import/use a third-party library. Sets up a single root
    handler, puts our own app logger (and everything under `service.*`,
    since every module in this codebase logs via
    `logging.getLogger(__name__)`, i.e. `service.ingestion.pipeline` etc.)
    at `level`, and silences the known-noisy dependencies above
    regardless of `level` — so requesting DEBUG for our own code doesn't
    accidentally re-enable the flood this function exists to prevent.
    """
    resolved_level = getattr(logging, level.upper(), logging.INFO)

    # Root default is WARNING, not `level` — third-party libraries we
    # haven't specifically vetted stay quiet unless they log a real
    # warning/error, rather than inheriting whatever verbosity our own
    # app wants for itself.
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    logging.getLogger(app_logger_name).setLevel(resolved_level)
    logging.getLogger("service").setLevel(resolved_level)

    for name in _NOISY_THIRD_PARTY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
