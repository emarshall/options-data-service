"""
Unit tests for the Black-Scholes calculator.

Validated three ways:
1. Against known textbook reference values (S=100, K=100, T=1, r=0.05,
   sigma=0.20 is a standard example with well-published correct answers).
2. Structural sanity checks (put-call parity, delta bounds, gamma/vega
   symmetry between calls and puts) that must hold for *any* correct
   Black-Scholes implementation, not just one specific input.
3. Round-trip: computing a price, then back-solving IV from that price,
   should recover the original IV.
"""

import math

import pytest

from service.greeks.black_scholes import (
    Right,
    bs_price,
    compute_greeks,
    implied_volatility,
)

# Standard textbook example: S=100, K=100, T=1yr, r=5%, sigma=20%, q=0.
# Published reference values (e.g. Hull's "Options, Futures, and Other
# Derivatives"): call ≈ 10.4506, put ≈ 5.5735, call delta ≈ 0.6368.
_S, _K, _T, _R, _SIGMA = 100.0, 100.0, 1.0, 0.05, 0.20


def test_call_price_matches_known_reference_value():
    price = bs_price(_S, _K, _T, _R, _SIGMA, Right.CALL)
    assert price == pytest.approx(10.4506, abs=0.001)


def test_put_price_matches_known_reference_value():
    price = bs_price(_S, _K, _T, _R, _SIGMA, Right.PUT)
    assert price == pytest.approx(5.5735, abs=0.001)


def test_put_call_parity_holds():
    """C - P = S*exp(-qT) - K*exp(-rT) — must hold for any correct BS
    implementation, not just the specific reference values above."""
    call = bs_price(_S, _K, _T, _R, _SIGMA, Right.CALL)
    put = bs_price(_S, _K, _T, _R, _SIGMA, Right.PUT)
    expected = _S - _K * math.exp(-_R * _T)
    assert (call - put) == pytest.approx(expected, abs=1e-6)


def test_call_delta_matches_known_reference_value():
    result = compute_greeks(_S, _K, _T, _R, Right.CALL, iv=_SIGMA)
    assert result.delta == pytest.approx(0.6368, abs=0.001)


def test_put_delta_matches_known_reference_value():
    result = compute_greeks(_S, _K, _T, _R, Right.PUT, iv=_SIGMA)
    assert result.delta == pytest.approx(0.6368 - 1.0, abs=0.001)


def test_call_and_put_deltas_differ_by_one():
    """Delta_call - Delta_put = exp(-qT), which is 1.0 when q=0 —
    a structural identity independent of the specific reference values."""
    call = compute_greeks(_S, _K, _T, _R, Right.CALL, iv=_SIGMA)
    put = compute_greeks(_S, _K, _T, _R, Right.PUT, iv=_SIGMA)
    assert (call.delta - put.delta) == pytest.approx(1.0, abs=1e-6)


def test_gamma_is_identical_for_call_and_put():
    """Gamma doesn't depend on option right — same formula for both."""
    call = compute_greeks(_S, _K, _T, _R, Right.CALL, iv=_SIGMA)
    put = compute_greeks(_S, _K, _T, _R, Right.PUT, iv=_SIGMA)
    assert call.gamma == pytest.approx(put.gamma, abs=1e-9)


def test_vega_is_identical_for_call_and_put():
    call = compute_greeks(_S, _K, _T, _R, Right.CALL, iv=_SIGMA)
    put = compute_greeks(_S, _K, _T, _R, Right.PUT, iv=_SIGMA)
    assert call.vega == pytest.approx(put.vega, abs=1e-9)


def test_deep_itm_call_delta_approaches_one():
    result = compute_greeks(S=200.0, K=100.0, T=0.5, r=0.05, right=Right.CALL, iv=0.20)
    assert result.delta > 0.95


def test_deep_otm_call_delta_approaches_zero():
    result = compute_greeks(S=50.0, K=100.0, T=0.5, r=0.05, right=Right.CALL, iv=0.20)
    assert result.delta < 0.05


def test_gamma_is_always_non_negative():
    for right in (Right.CALL, Right.PUT):
        result = compute_greeks(_S, _K, _T, _R, right, iv=_SIGMA)
        assert result.gamma >= 0


def test_vega_is_always_non_negative():
    for right in (Right.CALL, Right.PUT):
        result = compute_greeks(_S, _K, _T, _R, right, iv=_SIGMA)
        assert result.vega >= 0


def test_implied_volatility_round_trips_from_price():
    price = bs_price(_S, _K, _T, _R, _SIGMA, Right.CALL)
    recovered_iv = implied_volatility(price, _S, _K, _T, _R, Right.CALL)
    assert recovered_iv == pytest.approx(_SIGMA, abs=1e-4)


def test_compute_greeks_back_solves_iv_when_not_given():
    price = bs_price(_S, _K, _T, _R, _SIGMA, Right.CALL)
    result = compute_greeks(_S, _K, _T, _R, Right.CALL, option_price=price)
    assert result is not None
    assert result.iv == pytest.approx(_SIGMA, abs=1e-3)
    # And the resulting delta should match the "given IV directly" path.
    direct = compute_greeks(_S, _K, _T, _R, Right.CALL, iv=_SIGMA)
    assert result.delta == pytest.approx(direct.delta, abs=1e-3)


def test_compute_greeks_returns_none_for_already_expired():
    assert compute_greeks(_S, _K, T=0.0, r=_R, right=Right.CALL, iv=_SIGMA) is None
    assert compute_greeks(_S, _K, T=-0.01, r=_R, right=Right.CALL, iv=_SIGMA) is None


def test_compute_greeks_returns_none_without_iv_or_price():
    assert compute_greeks(_S, _K, _T, _R, Right.CALL) is None


def test_compute_greeks_returns_none_for_invalid_spot_or_strike():
    assert compute_greeks(S=0.0, K=_K, T=_T, r=_R, right=Right.CALL, iv=_SIGMA) is None
    assert compute_greeks(S=_S, K=0.0, T=_T, r=_R, right=Right.CALL, iv=_SIGMA) is None


def test_near_expiration_does_not_crash_or_produce_nan():
    """T very close to zero is where BS gets numerically dicey (division
    by sigma*sqrt(T)) — this shouldn't crash or return NaN/inf, even though
    the values themselves are not claimed to be meaningful that close to
    expiration (see module docstring)."""
    one_minute_in_years = 60 / (365.25 * 24 * 3600)
    result = compute_greeks(_S, _K, T=one_minute_in_years, r=_R, right=Right.CALL, iv=_SIGMA)
    assert result is not None
    for value in (result.delta, result.gamma, result.theta, result.vega, result.rho):
        assert math.isfinite(value)


def test_implied_volatility_returns_none_for_expired():
    assert implied_volatility(1.0, _S, _K, T=0.0, r=_R, right=Right.CALL) is None


def test_vega_matches_tastytrade_convention_order_of_magnitude():
    """Sanity check against Task 0's real captured sample: a cheap
    (~$0.22) short-dated option showed vega ≈ 0.0046 in TastyTrade's own
    Greeks event — i.e. small fractions, not raw per-100%-vol values
    (which would be ~100x larger). Not an exact match (different
    underlying/strike/IV), just confirming the unit convention is right."""
    result = compute_greeks(S=750.0, K=750.0, T=5 / 365.25, r=_R, right=Right.CALL, iv=0.475)
    assert 0.0001 < result.vega < 1.0
