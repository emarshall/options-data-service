"""
Black-Scholes(-Merton) Greeks calculator.

**Why this is simpler than the original plan draft anticipated:** Task 0
confirmed `Candle` events include `ImpVolatility` directly (see PLAN.md
Section 2/5). That means the primary use case — filling in Greeks for
Task 5's backfilled rows — already has IV in hand; this module just needs
to turn (IV, strike, expiration, spot, rate) into delta/gamma/theta/vega/
rho via closed-form formulas. Back-solving IV from price (Newton-Raphson
in the original plan) is only needed as a fallback, for the rarer case of
a Task 4 gap — a bar with a price but no Greeks *and* no IV (e.g. a Quote
arrived but the matching Greeks event never did that minute).

**Known simplifications, deliberate, documented per PLAN.md's own open
question about this:**
- European-style exercise assumed (standard Black-Scholes). Reasonable
  approximation for liquid, short-dated equity/index options — the early-
  exercise premium is small for those — but a real simplification for
  anything where it isn't (deep ITM American puts on dividend-paying
  stocof, for instance). Not a concern for the project's 0DTE/short-dated
  focus.
- Dividend yield defaults to 0.0 (the `q` parameter throughout). Fine for
  backtesting purposes per the plan's own reasoning; revisit only if
  accuracy issues actually show up for dividend-paying underlyings.
- Near/at expiration (T -> 0), Black-Scholes itself becomes numerically
  degenerate (gamma/vega blow up, delta becomes a step function) — this
  is a real modeling limitation, not just a code issue, since actual 0DTE
  market behavior diverges from BS assumptions in the final minutes
  anyway. `T` is clamped to a small minimum (see `_MIN_T`) to avoid
  outright division-by-zero rather than to claim numerical accuracy in
  that regime — treat computed Greeks in the last few minutes before
  expiration with appropriate skepticism.
- Already-expired bars (T <= 0) are not computed at all — `compute_greeks`
  returns None. Intrinsic value applies post-expiration, not BS Greeks.

Greeks are returned in the same per-unit convention TastyTrade's own live
Greeks event uses (confirmed against real captured samples in Task 0):
vega per 1% (0.01) change in IV, theta per calendar day, rho per 1% (0.01)
change in the risk-free rate — not the raw per-100%/per-year values the
textbook formulas produce directly.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

# Clamp for time-to-expiry, in years — about 30 seconds. Prevents division
# by zero in sigma*sqrt(T) terms right at/near expiration; see module
# docstring re: why this is a floor, not a claim of accuracy there.
_MIN_T_YEARS = 30 / (365.25 * 24 * 3600)

_SQRT_2PI = math.sqrt(2 * math.pi)


class Right(str, Enum):
    CALL = "call"
    PUT = "put"


@dataclass
class GreeksResult:
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    iv: float  # echoed back — useful when this was back-solved, not given


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def _d1_d2(S: float, K: float, T: float, r: float, sigma: float, q: float) -> tuple[float, float]:
    T = max(T, _MIN_T_YEARS)
    sigma = max(sigma, 1e-6)  # avoid division by zero for a degenerate zero-vol input
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def bs_price(S: float, K: float, T: float, r: float, sigma: float, right: Right, q: float = 0.0) -> float:
    """Theoretical Black-Scholes price. Used both standalone and as the
    target function for implied_volatility()'s bisection search."""
    T = max(T, _MIN_T_YEARS)
    d1, d2 = _d1_d2(S, K, T, r, sigma, q)
    if right == Right.CALL:
        return S * math.exp(-q * T) * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * math.exp(-q * T) * _norm_cdf(-d1)


def implied_volatility(
    target_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    right: Right,
    q: float = 0.0,
    lo: float = 1e-4,
    hi: float = 5.0,
    tol: float = 1e-6,
    max_iter: int = 100,
) -> float | None:
    """Back-solves IV from an observed option price via bisection.

    Bisection rather than Newton-Raphson, deliberately: BS price is
    monotonically non-decreasing in sigma (vega >= 0 always), so bisection
    is guaranteed to converge given a valid bracket — no risk of the
    numerical instability Newton-Raphson can hit when vega is tiny (which
    happens often here, e.g. deep OTM or near expiration, exactly the
    conditions common in 0DTE data). This isn't latency-sensitive code
    (runs as a batch reconciliation job), so trading a few extra
    iterations for that robustness is a clear win.
    """
    if T <= 0 or target_price <= 0 or S <= 0 or K <= 0:
        return None

    price_lo = bs_price(S, K, T, r, lo, right, q)
    price_hi = bs_price(S, K, T, r, hi, right, q)
    if target_price <= price_lo:
        return lo
    if target_price >= price_hi:
        return hi

    for _ in range(max_iter):
        mid = (lo + hi) / 2
        price_mid = bs_price(S, K, T, r, mid, right, q)
        if abs(price_mid - target_price) < tol:
            return mid
        if price_mid < target_price:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def compute_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    right: Right,
    iv: float | None = None,
    option_price: float | None = None,
    q: float = 0.0,
) -> GreeksResult | None:
    """Computes delta/gamma/theta/vega/rho.

    Pass `iv` directly when known (the common case — Task 5's backfilled
    rows already have it from the Candle event's ImpVolatility). Pass
    `option_price` instead (leaving `iv=None`) to back-solve IV first via
    implied_volatility() — the fallback path for Task 4 gap rows.

    Returns None if T <= 0 (already expired — see module docstring),
    S/K aren't positive, or IV couldn't be determined (neither `iv` nor a
    usable `option_price` was given, or the solve failed).
    """
    if T <= 0 or S <= 0 or K <= 0:
        return None

    if iv is None:
        if option_price is None:
            return None
        iv = implied_volatility(option_price, S, K, T, r, right, q)
        if iv is None:
            return None
    if iv <= 0:
        return None

    T_eff = max(T, _MIN_T_YEARS)
    d1, d2 = _d1_d2(S, K, T_eff, r, iv, q)
    pdf_d1 = _norm_pdf(d1)
    sqrt_T = math.sqrt(T_eff)

    if right == Right.CALL:
        delta = math.exp(-q * T_eff) * _norm_cdf(d1)
        theta_per_year = (
            -(S * math.exp(-q * T_eff) * pdf_d1 * iv) / (2 * sqrt_T)
            - r * K * math.exp(-r * T_eff) * _norm_cdf(d2)
            + q * S * math.exp(-q * T_eff) * _norm_cdf(d1)
        )
        rho_raw = K * T_eff * math.exp(-r * T_eff) * _norm_cdf(d2)
    else:
        delta = math.exp(-q * T_eff) * (_norm_cdf(d1) - 1.0)
        theta_per_year = (
            -(S * math.exp(-q * T_eff) * pdf_d1 * iv) / (2 * sqrt_T)
            + r * K * math.exp(-r * T_eff) * _norm_cdf(-d2)
            - q * S * math.exp(-q * T_eff) * _norm_cdf(-d1)
        )
        rho_raw = -K * T_eff * math.exp(-r * T_eff) * _norm_cdf(-d2)

    gamma = math.exp(-q * T_eff) * pdf_d1 / (S * iv * sqrt_T)
    vega_raw = S * math.exp(-q * T_eff) * pdf_d1 * sqrt_T

    return GreeksResult(
        delta=delta,
        gamma=gamma,
        theta=theta_per_year / 365.25,  # per calendar day — matches TastyTrade's convention
        vega=vega_raw * 0.01,  # per 1% (0.01) change in IV
        rho=rho_raw * 0.01,  # per 1% (0.01) change in the risk-free rate
        iv=iv,
    )
