"""
Covers the fix for a real bug (see PLAN.md Section 7 / service/logging_config.py's
module docstring): the `tastytrade` package sets its own top-level logger to
DEBUG at import time, independent of whatever level our own app configures
on the root logger, causing every raw websocket message it receives to be
logged. These tests import the real `tastytrade` package (already a
dependency) to assert against its actual, current logger configuration
rather than a synthetic stand-in — the whole point is confirming
`configure_logging` beats *that specific* real behavior, not a general
logging story.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from service.logging_config import clamp_noisy_third_party_loggers, configure_logging


def test_tastytrade_package_really_does_set_debug_on_itself():
    """Documents the root cause this module exists to fix. If this ever
    stops being true (e.g. a `tastytrade` upgrade removes the
    self-`setLevel(DEBUG)` call), the rest of this file's tests still pass
    trivially — but it'd be worth revisiting whether configure_logging's
    explicit override is still needed."""
    import tastytrade  # noqa: F401 — importing is what triggers the setLevel(DEBUG) call

    assert logging.getLogger("tastytrade").level == logging.DEBUG


def test_configure_logging_overrides_tastytrade_debug_flood():
    import tastytrade  # noqa: F401 — ensure it's imported (and has set itself to DEBUG) first

    configure_logging("ingestion", "INFO")

    tt_logger = logging.getLogger("tastytrade")
    assert tt_logger.level == logging.WARNING
    # The actual bug: a submodule logger with no level of its own inherits
    # from the nearest ancestor that has one set — confirm that ancestor is
    # now WARNING, not DEBUG, for a representative submodule logger.
    streamer_logger = logging.getLogger("tastytrade.streamer")
    assert streamer_logger.getEffectiveLevel() == logging.WARNING
    assert not streamer_logger.isEnabledFor(logging.DEBUG)


def test_configure_logging_sets_app_logger_to_requested_level():
    configure_logging("ingestion", "DEBUG")
    assert logging.getLogger("ingestion").level == logging.DEBUG
    assert logging.getLogger("service").level == logging.DEBUG
    # A real service module logger (child of "service") should inherit it.
    assert logging.getLogger("service.ingestion.pipeline").getEffectiveLevel() == logging.DEBUG


def test_configure_logging_defaults_app_logger_to_info():
    configure_logging("ingestion", "INFO")
    assert logging.getLogger("ingestion").level == logging.INFO


def test_configure_logging_falls_back_to_info_for_unknown_level_name():
    configure_logging("ingestion", "not-a-real-level")
    assert logging.getLogger("ingestion").level == logging.INFO


def test_configure_logging_clamps_other_noisy_third_party_loggers():
    configure_logging("ingestion", "DEBUG")
    for name in ("httpx", "httpcore", "websockets", "hpack"):
        assert logging.getLogger(name).level == logging.WARNING


def test_configure_logging_does_not_survive_a_later_lazy_import_of_tastytrade():
    """Regression test for the bug this `clamp_noisy_third_party_loggers` helper
    exists to fix.

    `service/ingestion/backfill.py` imports `TastyTradeSource` *inside* `_main()`,
    i.e. after `configure_logging()` has already run. That deferred import
    re-executes `tastytrade/__init__.py`, which unconditionally does
    `setLevel(DEBUG)` on its own logger — silently undoing our clamp and
    re-enabling the raw-websocket-message flood, on a job that requests far
    more messages than normal ingestion (one full historical candle backfill
    per gap, per ticker).

    `importlib.reload` re-runs that module body, which is exactly the effect
    the deferred import has, so it reproduces the clobber deterministically
    rather than depending on `tastytrade` being absent from `sys.modules`.
    """
    import importlib

    import tastytrade

    configure_logging("ingestion", "INFO")
    assert logging.getLogger("tastytrade").level == logging.WARNING

    try:
        importlib.reload(tastytrade)  # simulates the deferred import

        # The bug: our clamp is gone, the flood is back.
        assert logging.getLogger("tastytrade").level == logging.DEBUG
        assert logging.getLogger("tastytrade.streamer").isEnabledFor(logging.DEBUG)

        # The fix, as actually called in backfill.py: re-clamp after the import.
        clamp_noisy_third_party_loggers()

        assert logging.getLogger("tastytrade").level == logging.WARNING
        streamer_logger = logging.getLogger("tastytrade.streamer")
        assert streamer_logger.getEffectiveLevel() == logging.WARNING
        assert not streamer_logger.isEnabledFor(logging.DEBUG)
    finally:
        # Leave the real (already-imported) module's logger in the clamped
        # state so the other tests in this file aren't affected by the reload.
        clamp_noisy_third_party_loggers()


def test_clamp_helper_is_idempotent_and_leaves_app_levels_alone():
    """The helper is called on hot paths and after re-imports, so calling it
    repeatedly must be a no-op, and it must not disturb our own app loggers'
    configured verbosity (it only touches third-party loggers)."""
    configure_logging("ingestion", "DEBUG")
    clamp_noisy_third_party_loggers()
    clamp_noisy_third_party_loggers()

    assert logging.getLogger("tastytrade").level == logging.WARNING
    # App loggers keep the level configure_logging gave them.
    assert logging.getLogger("ingestion").level == logging.DEBUG
    assert logging.getLogger("service").level == logging.DEBUG


@pytest.mark.asyncio
async def test_ensure_streamer_reclamps_after_lazy_sdk_import(monkeypatch):
    """Regression test for the real chokepoint, not just the helper.

    `_ensure_streamer` is where `service/sources/tastytrade.py` first imports
    the SDK for streaming, and therefore the one place the flood could be
    re-enabled regardless of entry point. Stub out the streamer so no network
    call happens; all we assert on is the logger state afterwards.
    """
    import importlib

    import tastytrade

    from service.sources.tastytrade import TastyTradeSource

    configure_logging("ingestion", "INFO")
    importlib.reload(tastytrade)  # SDK re-applies its own setLevel(DEBUG)
    assert logging.getLogger("tastytrade").level == logging.DEBUG

    class _FakeStreamer:
        def __init__(self, session):
            self.session = session

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    monkeypatch.setattr(tastytrade, "DXLinkStreamer", _FakeStreamer)

    # Build the instance without running __init__/authenticate — only the
    # three attributes `_ensure_streamer` actually touches.
    source = TastyTradeSource.__new__(TastyTradeSource)
    source._streamer = None
    source._streamer_lock = asyncio.Lock()
    source._session = object()

    streamer = await source._ensure_streamer()
    assert isinstance(streamer, _FakeStreamer)

    # The fix: the SDK's import-time setLevel(DEBUG) gets re-clamped here.
    assert logging.getLogger("tastytrade").level == logging.WARNING
    assert not logging.getLogger("tastytrade.streamer").isEnabledFor(logging.DEBUG)
