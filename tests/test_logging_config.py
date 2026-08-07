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

import logging

from service.logging_config import configure_logging


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
