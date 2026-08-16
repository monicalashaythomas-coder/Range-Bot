"""
DERIV EXPIRYRANGE BOT  v3
====================================
Symbols  : 1HZ10V  (Volatility 10 Index)
Contracts: EXPIRYRANGE — terminal price must land WITHIN fixed barriers at expiry

Fixed barriers (2-minute expiry, from live testing):
  1HZ10V  EXPIRYRANGE : ±1.60 price units

Signal philosophy
─────────────────
  We do NOT scan for the best EV across a grid of barriers.
  The barriers are fixed. The only question is: does the market,
  RIGHT NOW, give us enough confidence that price will behave as
  required over the next 2 minutes?

  A RANGE_QUIET regime gate runs FIRST. It must confirm the market is
  in a quiet, range-bound state before any of the five intelligence
  layers below are even evaluated — a trending or just-jumped market
  is not a candidate for EXPIRYRANGE regardless of what the layers say.

  RANGE_QUIET regime gate (7 independent models, weighted vote):
    · OU        — Ornstein-Uhlenbeck mean-reversion speed (θ) fit via
                  AR(1) on price levels; fast reversion → quiet
    · RSI       — Wilder RSI(14); wants price near the 50 midline,
                  not overbought/oversold (i.e. not trending)
    · StochRSI  — RSI's own recent range position; wants the mid-band,
                  confirms RSI isn't just resting mid-swing
    · Bollinger — band width (k·σ) relative to price; narrow bands
                  ("squeeze") → quiet/compressed
    · Z-score   — (price − rolling mean) / rolling σ; wants price
                  close to its short-term mean, not stretched
    · S/R       — support/resistance channel over the lookback window;
                  wants price comfortably inside the channel, not
                  pressed against either edge
    · Post-jump — hard block (not weighted): if any recent tick moved
                  more than N·σ, that's a jump — trading is paused for
                  a cooldown window regardless of how quiet everything
                  else looks, since jumps often precede vol clusters
  A weighted score from OU/RSI/StochRSI/Bollinger/Z-score/S-R must clear
  RQ_THRESHOLD, AND the post-jump check must be clear, before layers 1-5
  are evaluated at all.

  Five intelligence layers must then reach agreement before a trade fires:

  LAYER 1 — Momentum direction (last 5 and 20 ticks)
    · Short momentum (5-tick): direction and magnitude of recent move
    · Medium momentum (20-tick): whether momentum is sustained
    · Wants LOW momentum in both windows (price is drifting, not
      running — a price that is running is likely to exit barriers)

  LAYER 2 — Volatility level (σ from last 60 ticks)
    · Low σ strongly favours containment (±1.6 is wide relative to
      the expected move if vol is low)

  LAYER 3 — Hurst exponent (mean-reversion vs trend, last 80 ticks)
    · H < 0.50 (mean-reverting) strongly favoured — price is likely
      to oscillate back to centre, not drift to edges

  LAYER 4 — Recent price level vs range centre
    · Gate checks that price is near the centre of the range, not
      already pushed to one edge

  LAYER 5 — Monte Carlo terminal simulation
    · Primary: GARCH(1,1)-GBM — σ comes from a one-step-ahead GARCH fit
      (120-tick window) instead of a flat rolling average, so it reacts
      to recent shocks rather than smoothing over them. Draws N terminal
      prices from GBM(µ, σ_garch, T=120s).
    · Cross-check: empirical bootstrap — resamples actual historical
      tick returns (same window) instead of assuming Gaussian ones,
      naturally capturing real fat tails/skew. Logged alongside the
      GARCH-GBM estimate for visibility, does NOT gate the trade.
    · P(|terminal - entry| < barrier) is the GARCH-GBM estimate; it
      must clear MIN_MC_CONFIDENCE threshold
    · MC is the final arbiter — all other layers passing but MC
      saying P(win) < threshold → no trade
    · Calibration logging: at entry, the MC's assumed σ/µ and both
      p_win estimates are snapshotted; at settlement, realized price
      move and win/loss are logged alongside them (CALIB log line +
      range_mc_calibration table) — builds a dataset to check whether
      MC's stated confidence is actually well-calibrated over time.

  AGREEMENT LOGIC:
    · RANGE_QUIET gate must pass first (see above)
    · Each layer votes pass/fail with a confidence weight
    · Total weighted score must exceed SIGNAL_THRESHOLD
    · GARCH-GBM MC confidence must independently exceed MIN_MC_CONFIDENCE
    · All conditions must hold simultaneously
    · No EV/payout gate — the bootstrap cross-check and calibration log
      are for visibility and future tuning, not a second trade gate

Risk management
───────────────
  · Stake per trade with martingale progression (configurable),
    not Kelly (payout ratios are too thin for Kelly to size meaningfully)
  · Per-symbol consecutive loss circuit breaker
  · Session stop-loss

Connection: Deriv new Options API (REST OTP + WebSocket)
"""

import asyncio
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.request
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Tuple, Dict

try:
    import websockets
    from websockets.exceptions import (
        ConnectionClosed, ConnectionClosedError, ConnectionClosedOK,
    )
except ImportError:
    sys.exit("websockets not installed — run: pip install websockets")

try:
    import requests
except ImportError:
    requests = None


# ============================================================================
# CONFIGURATION
# ============================================================================

def _env(key: str, default):
    val = os.environ.get(key)
    if val is None:
        return default
    if isinstance(default, bool):
        return val.lower() in ("1", "true", "yes")
    if isinstance(default, float):
        return float(val)
    if isinstance(default, int):
        return int(val)
    return val


CONFIG = {
    # ── Deriv credentials ─────────────────────────────────────────
    "api_token":        _env("DERIV_API_TOKEN", ""),
    "app_id":           _env("DERIV_APP_ID", ""),
    "account_id":       _env("DERIV_ACCOUNT_ID", ""),
    "use_real_account": _env("DERIV_USE_REAL", False),

    # ── Symbols ───────────────────────────────────────────────────
    "symbols":          ["1HZ10V"],
    "currency":         "USD",

    # ── Fixed barriers (confirmed from live API testing) ──────────
    # These are the known-valid barrier sizes for 2-minute expiry.
    # We do not scan for barriers — they are fixed. The signal layers
    # decide whether market conditions justify trading them.
    "barriers": {
        "1HZ10V": 1.60,
    },

    # ── Contract duration ─────────────────────────────────────────
    "duration_s":       120,   # 2 minutes in seconds
    "duration_unit":    "s",

    # ── Tick history ──────────────────────────────────────────────
    "tick_window":      300,   # keep last 300 ticks (5 minutes)
    "min_ticks":        90,    # minimum before evaluating (1.5 min warmup)

    # ── Signal layer parameters ───────────────────────────────────
    # Layer 1: Momentum
    "momentum_short_n":   5,    # short momentum window (ticks)
    "momentum_medium_n":  20,   # medium momentum window (ticks)
    # Layer 2: Volatility
    "vol_window":         60,   # ticks for σ estimate (Layer 2 gate only)
    # How many ticks does price typically move in 120 seconds at this vol?
    # Expected move = σ × √120. Barrier must be > expected move for the
    # trade to be reasonable — this ratio gates signal confidence.
    "vol_skip_thresh":    _env("VOL_SKIP", 0.000300),   # skip if chaos
    # Layer 3: Hurst
    "hurst_window":       80,
    # Layer 4: Barrier proximity (price vs centre of the range)
    "er_centre_gate":     0.65,  # skip EXPIRYRANGE if price moved >65% of barrier from centre

    # Layer 5: Monte Carlo
    # Shared lookback for the GARCH fit AND the bootstrap resample — one
    # window feeds both, instead of MC silently using its own hardcoded
    # window like it used to. 120 (vs the 60 Layer 2 uses) gives the
    # GARCH(1,1) fit a more stable parameter estimate; GARCH's own
    # recency-weighting (not the window length) is what keeps it
    # reactive to a recent shock.
    "mc_window":           _env("MC_WINDOW", 120),
    "mc_n_sims":           2000,
    "mc_min_confidence":   _env("MC_MIN_CONF", 0.72),  # MC p(win) must exceed this
    # Empirical bootstrap cross-check — resamples actual historical tick
    # returns instead of assuming Gaussian ones. Run alongside the
    # GARCH-GBM MC so the two can be compared; does not gate the trade
    # (no EV/agreement gate — logged for visibility and calibration only).
    "mc_bootstrap_n_sims": _env("MC_BOOTSTRAP_N_SIMS", 1000),

    # ── MC calibration logging ──────────────────────────────────────
    # At entry, snapshot what the MC predicted; at settlement, snapshot
    # what actually happened, so predicted vs realized can be compared
    # later to check whether MC's p_win is well-calibrated.
    "mc_calibration_log":  _env("MC_CALIBRATION_LOG", True),

    # ── Signal agreement threshold ────────────────────────────────
    # Weighted vote from layers 1-4 must exceed this before MC is even run.
    # Scale: 0.0 (no agreement) to 1.0 (all layers fully agree)
    "signal_threshold":   _env("SIGNAL_THRESH", 0.60),

    # ── RANGE_QUIET regime gate ─────────────────────────────────────
    # Runs BEFORE layers 1-5. Combines 6 weighted quiet/range-bound
    # detectors (OU, RSI, StochRSI, Bollinger, Z-score, S/R) into one
    # score that must clear rq_threshold, AND a 7th hard gate
    # (post-jump) that must be clear — a recent jump blocks regardless
    # of what the weighted score says.
    "rq_window":            _env("RQ_WINDOW", 60),     # shared lookback for OU/S-R
    # OU (Ornstein-Uhlenbeck) mean-reversion speed, fit via AR(1) on price
    "rq_ou_theta_min":      _env("RQ_OU_THETA_MIN", 0.05),
    # RSI(14) — wants |RSI-50| within band → not trending
    "rq_rsi_window":        _env("RQ_RSI_WINDOW", 14),
    "rq_rsi_band":          _env("RQ_RSI_BAND", 15.0),
    # StochRSI — wants |StochRSI-0.5| within band
    "rq_srsi_window":       _env("RQ_SRSI_WINDOW", 14),
    "rq_srsi_band":         _env("RQ_SRSI_BAND", 0.30),
    # Bollinger — band width (k·σ) / price must stay under this to count as "squeezed"
    "rq_boll_window":       _env("RQ_BOLL_WINDOW", 20),
    "rq_boll_k":            _env("RQ_BOLL_K", 2.0),
    "rq_boll_width_max":    _env("RQ_BOLL_WIDTH_MAX", 0.0035),
    # Z-score — |price z-score| vs rolling mean/σ must stay under this
    "rq_zscore_window":     _env("RQ_ZSCORE_WINDOW", 30),
    "rq_zscore_max":        _env("RQ_ZSCORE_MAX", 1.25),
    # S/R — price must stay this fraction (or more) away from either
    # channel edge, as a fraction of channel width
    "rq_sr_edge_gate":      _env("RQ_SR_EDGE_GATE", 0.15),
    # Post-jump — hard block: any tick move > sigma_mult × local σ within
    # the last `jump_cooldown_ticks` ticks blocks trading outright
    "rq_jump_window":            _env("RQ_JUMP_WINDOW", 30),
    "rq_jump_sigma_mult":        _env("RQ_JUMP_SIGMA_MULT", 3.5),
    "rq_jump_cooldown_ticks":    _env("RQ_JUMP_COOLDOWN_TICKS", 15),
    # Composite weighted score (OU/RSI/StochRSI/Bollinger/Z-score/S-R)
    # must clear this before the gate passes
    "rq_threshold":         _env("RQ_THRESHOLD", 0.60),

    # ── Stake sizing ─────────────────────────────────────────────
    # Base stake — the martingale progression (below) multiplies off this.
    "stake":            _env("STAKE", 1.00),
    "max_stake":        _env("MAX_STAKE", 50.00),

    # ── Martingale ─────────────────────────────────────────────────
    # On a loss, next stake = base_stake * (factor ** step), step capped
    # at martingale_max_steps. A win — or hitting max_steps without a
    # win — resets the step back to 0 (base stake).
    # This is purely step-driven: it does NOT look at account balance
    # or equity to decide whether to progress, so it activates the same
    # way regardless of account size. (max_stake still applies as a
    # hard ceiling so a runaway progression can't place an oversized
    # trade — raise max_stake if you want the full 3-step progression
    # to have headroom: base * 2.5^3 = base * 15.625.)
    "martingale_enabled":   _env("MARTINGALE_ENABLED",   True),
    "martingale_factor":    _env("MARTINGALE_FACTOR",    2.5),
    "martingale_max_steps": _env("MARTINGALE_MAX_STEPS", 3),

    # ── Session risk ──────────────────────────────────────────────
    "stop_loss":        _env("STOP_LOSS", 20.00),
    "stop_loss_pct":    _env("STOP_PCT",   0.0),

    # ── Circuit breaker ───────────────────────────────────────────
    "consec_loss_limit": _env("CONSEC_LOSS_LIMIT",    3),
    "consec_pause_secs": _env("CONSEC_PAUSE_SECS",  300),

    # ── Eval cooldown ─────────────────────────────────────────────
    # Minimum seconds between evaluations per symbol.
    # Given 2-minute contracts, no point evaluating faster than every 30s.
    "eval_cooldown":    _env("EVAL_COOLDOWN", 30),

    # ── Resilience ────────────────────────────────────────────────
    "lock_timeout":         _env("LOCK_TIMEOUT",     240),  # 2min contract + 2min buffer
    "buy_recv_retries":     _env("BUY_RETRIES",        8),
    "reconnect_delay_min":  _env("RECONNECT_MIN",       2),
    "reconnect_delay_max":  _env("RECONNECT_MAX",      60),
    "ws_ping_interval":     _env("WS_PING",            30),
    "orphan_poll_attempts": _env("ORPHAN_ATTEMPTS",     4),
    "orphan_poll_interval": _env("ORPHAN_INTERVAL",     3),

    # ── Persistence ───────────────────────────────────────────────
    "supabase_url":      _env("SUPABASE_URL", ""),
    "supabase_key":      _env("SUPABASE_KEY", ""),
    "persist_every_secs": _env("PERSIST_EVERY_SECS", 120),
}


# ============================================================================
# HELPERS
# ============================================================================

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _log(tag: str, msg: str):
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)


# ============================================================================
# TICK STORE — per-symbol tick history
# ============================================================================

class TickStore:
    """Maintains a rolling window of recent prices and computes local
    statistical estimates (vol, drift, Hurst) used by both MC engines."""

    def __init__(self, symbol: str, maxlen: int = 200):
        self.symbol  = symbol
        self.prices: deque = deque(maxlen=maxlen)
        self.count   = 0

    def add(self, price: float):
        self.prices.append(price)
        self.count += 1

    def is_ready(self, min_ticks: int) -> bool:
        return len(self.prices) >= min_ticks

    def returns(self, n: int = 50) -> List[float]:
        """Most recent N log-returns (relative moves)."""
        p = list(self.prices)[-n-1:]
        return [(p[i] - p[i-1]) / p[i-1]
                for i in range(1, len(p))
                if p[i-1] != 0]

    def local_vol(self, n: int = 50) -> float:
        """Return-scale σ from the last N ticks."""
        rets = self.returns(n)
        if len(rets) < 5:
            return 0.0
        mu  = sum(rets) / len(rets)
        var = sum((r - mu)**2 for r in rets) / len(rets)
        return math.sqrt(var)

    def local_drift(self, n: int = 50) -> float:
        """Mean return over last N ticks (drift µ per tick)."""
        rets = self.returns(n)
        if not rets:
            return 0.0
        return sum(rets) / len(rets)

    def _garch_loglik(self, rets: List[float], omega: float,
                      alpha: float, beta: float, seed_var: float) -> float:
        var = seed_var
        ll  = 0.0
        for i in range(1, len(rets)):
            var = omega + alpha * rets[i-1]**2 + beta * var
            if var <= 0:
                return -1e18
            ll += -0.5 * (math.log(2 * math.pi * var) + rets[i]**2 / var)
        return ll

    def garch_sigma(self, window: int = 120) -> float:
        """
        One-step-ahead conditional volatility from a GARCH(1,1) fit on
        the last `window` tick returns — reacts to recent shocks instead
        of averaging them away like a flat rolling σ does.

        Fit via a coarse grid search over (persistence, alpha-share),
        maximizing Gaussian log-likelihood — no external dependencies,
        cheap enough to re-fit every evaluation on ~120 points.
        Falls back to local_vol() if there isn't enough data or the
        fit degenerates.
        """
        rets = self.returns(window)
        if len(rets) < 20:
            return self.local_vol(window)

        n = len(rets)
        mu = sum(rets) / n
        long_run_var = sum((r - mu)**2 for r in rets) / n
        if long_run_var <= 0:
            return self.local_vol(window)

        best_ll, best_params = -1e18, None
        for persistence in (0.80, 0.85, 0.90, 0.93, 0.95, 0.97):
            for alpha_frac in (0.05, 0.10, 0.20, 0.30, 0.40):
                alpha = persistence * alpha_frac
                beta  = persistence - alpha
                if beta < 0 or alpha <= 0:
                    continue
                omega = long_run_var * (1 - persistence)
                if omega <= 0:
                    continue
                ll = self._garch_loglik(rets, omega, alpha, beta, long_run_var)
                if ll > best_ll:
                    best_ll, best_params = ll, (omega, alpha, beta)

        if best_params is None:
            return self.local_vol(window)

        omega, alpha, beta = best_params
        # Run the recursion forward through the window to get the current
        # conditional variance, then forecast one step ahead.
        var = long_run_var
        for i in range(1, n):
            var = omega + alpha * rets[i-1]**2 + beta * var
        next_var = omega + alpha * rets[-1]**2 + beta * var
        if next_var <= 0:
            return self.local_vol(window)
        return math.sqrt(next_var)

    def bootstrap_terminal_paths(self, window: int, duration_s: int,
                                 n_sims: int) -> List[float]:
        """
        Empirical bootstrap: resample actual historical tick returns
        (with replacement) from the last `window` ticks to build
        `n_sims` synthetic `duration_s`-tick terminal price paths.
        Captures the real empirical return distribution (fat tails,
        skew, jumps) instead of assuming Gaussian returns like the
        parametric GBM MC does. Returns a list of terminal prices.
        """
        rets  = self.returns(window)
        price = self.current_price()
        if not rets or not price:
            return []
        terminals = []
        for _ in range(n_sims):
            p = price
            for _ in range(duration_s):
                r = rets[random.randrange(len(rets))]
                p = p * (1 + r)
            terminals.append(p)
        return terminals

    def hurst(self, n: int = 80) -> float:
        """
        R/S Hurst exponent estimate. Uses log-spaced sub-window sizes
        over the last N returns. H < 0.45 → mean-reverting,
        H > 0.55 → trending, H ≈ 0.5 → random walk.

        Computed on RETURNS (not raw prices) to avoid the spurious
        H≈1.0 result that R/S on a price series always produces due to
        the cumulative sum of a random walk having long memory by
        construction — a well-known trap in the literature.
        """
        rets = self.returns(n)
        if len(rets) < 20:
            return 0.5  # not enough data — assume random walk

        rs_vals, ns = [], []
        for size in [10, 15, 20, 30, 40]:
            if size > len(rets) // 2:
                continue
            chunks = [rets[i:i+size] for i in range(0, len(rets)-size, size)]
            if not chunks:
                continue
            rs_list = []
            for chunk in chunks:
                mu_c   = sum(chunk) / len(chunk)
                devs   = [chunk[j] - mu_c for j in range(len(chunk))]
                cumdev = [sum(devs[:k+1]) for k in range(len(devs))]
                R      = max(cumdev) - min(cumdev)
                S      = math.sqrt(sum((r - mu_c)**2 for r in chunk) / len(chunk))
                if S > 0:
                    rs_list.append(R / S)
            if rs_list:
                rs_vals.append(math.log(sum(rs_list) / len(rs_list)))
                ns.append(math.log(size))

        if len(ns) < 2:
            return 0.5
        # OLS slope of log(R/S) vs log(n) — that slope is H
        n_pts = len(ns)
        mx = sum(ns) / n_pts
        my = sum(rs_vals) / n_pts
        num = sum((ns[i]-mx)*(rs_vals[i]-my) for i in range(n_pts))
        den = sum((ns[i]-mx)**2 for i in range(n_pts))
        return float(num / den) if den > 0 else 0.5

    def current_price(self) -> Optional[float]:
        return self.prices[-1] if self.prices else None

    # ── RANGE_QUIET regime indicators ───────────────────────────────

    def _rsi_from_window(self, window: List[float], period: int) -> Optional[float]:
        if len(window) < period + 1:
            return None
        gains = losses = 0.0
        for i in range(1, len(window)):
            delta = window[i] - window[i-1]
            if delta > 0:
                gains += delta
            else:
                losses += -delta
        avg_gain = gains / period
        avg_loss = losses / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def rsi(self, period: int = 14) -> float:
        """Wilder-style RSI over the last `period` ticks. 0-100, 50=neutral."""
        prices = list(self.prices)[-(period + 1):]
        val = self._rsi_from_window(prices, period)
        return val if val is not None else 50.0

    def rsi_series(self, period: int = 14, count: int = 14) -> List[float]:
        """RSI recomputed at each of the last `count` points (sliding window)."""
        prices = list(self.prices)
        vals = []
        start = max(period, len(prices) - count)
        for end in range(start, len(prices)):
            val = self._rsi_from_window(prices[end - period:end + 1], period)
            if val is not None:
                vals.append(val)
        return vals

    def stoch_rsi(self, rsi_period: int = 14, stoch_period: int = 14) -> float:
        """
        Stochastic RSI — where the latest RSI value sits within its own
        recent [min, max] range. 0-1, 0.5 = mid-band (neither RSI extreme
        of its own recent history).
        """
        vals = self.rsi_series(rsi_period, count=stoch_period)
        if len(vals) < 2:
            return 0.5
        lo, hi = min(vals), max(vals)
        if hi - lo < 1e-9:
            return 0.5
        return (vals[-1] - lo) / (hi - lo)

    def bollinger(self, n: int = 20, k: float = 2.0) -> Optional[Tuple[float, float, float, float]]:
        """Returns (mid, upper, lower, width_pct); width_pct = (upper-lower)/mid."""
        prices = list(self.prices)[-n:]
        if len(prices) < max(5, n // 2):
            return None
        mid   = sum(prices) / len(prices)
        var   = sum((p - mid) ** 2 for p in prices) / len(prices)
        sigma = math.sqrt(var)
        upper = mid + k * sigma
        lower = mid - k * sigma
        width_pct = (upper - lower) / mid if mid != 0 else 0.0
        return mid, upper, lower, width_pct

    def zscore(self, n: int = 30) -> float:
        """(current price - rolling mean) / rolling σ over the last n ticks."""
        prices = list(self.prices)[-n:]
        if len(prices) < max(5, n // 2):
            return 0.0
        mean  = sum(prices) / len(prices)
        var   = sum((p - mean) ** 2 for p in prices) / len(prices)
        sigma = math.sqrt(var)
        if sigma <= 0:
            return 0.0
        return (prices[-1] - mean) / sigma

    def support_resistance(self, n: int = 60) -> Optional[Tuple[float, float, float]]:
        """
        Returns (support, resistance, edge_ratio). edge_ratio is how far
        current price sits from the NEARER channel edge, as a fraction of
        channel width — 0.0 = sitting on an edge, 0.5 = dead centre.
        """
        prices = list(self.prices)[-n:]
        if len(prices) < max(10, n // 3):
            return None
        support    = min(prices)
        resistance = max(prices)
        width      = resistance - support
        if width <= 0:
            return support, resistance, 0.5
        price         = prices[-1]
        dist_to_edge  = min(price - support, resistance - price)
        edge_ratio    = dist_to_edge / width
        return support, resistance, edge_ratio

    def ou_theta(self, n: int = 60) -> float:
        """
        Ornstein-Uhlenbeck mean-reversion speed θ, estimated by fitting
        AR(1) on price levels: X_t = a + b·X_{t-1} + ε, then θ = -ln(b).
        θ > 0 and larger → faster reversion to the long-run mean → more
        range-bound. b >= 1 (no reversion / trending) → θ = 0.
        """
        prices = list(self.prices)[-n:]
        if len(prices) < max(10, n // 2):
            return 0.0
        x, y = prices[:-1], prices[1:]
        mx, my = sum(x) / len(x), sum(y) / len(y)
        num = sum((x[i] - mx) * (y[i] - my) for i in range(len(x)))
        den = sum((x[i] - mx) ** 2 for i in range(len(x)))
        if den <= 0:
            return 0.0
        b = num / den
        if b <= 0 or b >= 1:
            return 0.0
        return -math.log(b)

    def recent_jump(self, window: int, sigma_mult: float,
                    cooldown_ticks: int) -> Optional[int]:
        """
        Returns ticks-since a jump (a single-tick return exceeding
        sigma_mult × local σ) if one occurred within the last
        `cooldown_ticks` ticks, else None.
        """
        sigma = self.local_vol(window)
        if sigma <= 0:
            return None
        prices = list(self.prices)[-(cooldown_ticks + 1):]
        n = len(prices)
        if n < 2:
            return None
        for i in range(n - 1, 0, -1):
            prev = prices[i - 1]
            if prev == 0:
                continue
            ret = (prices[i] - prev) / prev
            if abs(ret) > sigma_mult * sigma:
                return n - 1 - i
        return None


# ============================================================================
# SIGNAL ENGINE — RANGE_QUIET regime gate + 5-layer intelligence
# ============================================================================

@dataclass
class ContractSignal:
    """Result of the full RANGE_QUIET + 5-layer intelligence evaluation."""
    contract_type:  str       # "EXPIRYRANGE" | "SKIP"
    symbol:         str
    barrier:        float     # absolute barrier offset (positive, ±barrier)
    p_win_mc:       float     # GARCH-GBM MC probability estimate
    layer_score:    float     # weighted score from layers 1-4 (0-1)
    rq_score:       float     # RANGE_QUIET composite score (0-1)
    reasons:        List[str]
    mc_sigma:              float = 0.0   # GARCH one-step-ahead σ used by the MC
    mc_mu:                 float = 0.0   # drift used by the MC
    mc_bootstrap_p_win:    float = 0.0   # empirical bootstrap cross-check estimate


@dataclass
class TradeSignal:
    """Resolved trade instruction passed to place_trade()."""
    contract_type:  str
    symbol:         str
    barrier:        float     # absolute barrier offset (+)
    p_win_mc:       float
    layer_score:    float
    stake:          float
    reasons:        List[str] = field(default_factory=list)
    mc_sigma:            float = 0.0
    mc_mu:                float = 0.0
    mc_bootstrap_p_win:   float = 0.0
    entry_price:          float = 0.0   # snapshot at signal time, for calibration
    entry_time:           float = 0.0   # time.monotonic() at signal time


# ── RANGE_QUIET regime gate ──────────────────────────────────────────────
# Seven independent models. Six are weighted and combined into a single
# quiet/range-bound score; the seventh (post-jump) is a hard block.

def _rq_ou_score(ts: TickStore, cfg: dict) -> Tuple[float, str]:
    theta = ts.ou_theta(cfg["rq_window"])
    theta_min = cfg["rq_ou_theta_min"]
    # Score ramps 0→1 as theta goes from 0 to 3x the minimum threshold
    score = max(0.0, min(1.0, theta / (theta_min * 3))) if theta_min > 0 else 0.0
    reason = f"rq_ou θ={theta:.4f} (min={theta_min:.4f}) score={score:.2f}"
    return score, reason


def _rq_rsi_score(ts: TickStore, cfg: dict) -> Tuple[float, str]:
    rsi = ts.rsi(cfg["rq_rsi_window"])
    band = cfg["rq_rsi_band"]
    dev = abs(rsi - 50.0)
    score = max(0.0, 1.0 - dev / band)
    reason = f"rq_rsi={rsi:.1f} |dev|={dev:.1f} band={band:.1f} score={score:.2f}"
    return score, reason


def _rq_srsi_score(ts: TickStore, cfg: dict) -> Tuple[float, str]:
    srsi = ts.stoch_rsi(cfg["rq_rsi_window"], cfg["rq_srsi_window"])
    band = cfg["rq_srsi_band"]
    dev = abs(srsi - 0.5)
    score = max(0.0, 1.0 - dev / band) if band > 0 else 0.0
    reason = f"rq_srsi={srsi:.2f} |dev|={dev:.2f} band={band:.2f} score={score:.2f}"
    return score, reason


def _rq_boll_score(ts: TickStore, cfg: dict) -> Tuple[float, str]:
    boll = ts.bollinger(cfg["rq_boll_window"], cfg["rq_boll_k"])
    if boll is None:
        return 0.5, "rq_boll=insufficient_data"
    _, _, _, width_pct = boll
    width_max = cfg["rq_boll_width_max"]
    # Narrower than width_max → high score; 2x width_max or more → 0
    score = max(0.0, min(1.0, 1.0 - (width_pct - width_max) / width_max)) \
            if width_max > 0 else 0.0
    reason = f"rq_boll width={width_pct*100:.3f}% max={width_max*100:.3f}% score={score:.2f}"
    return score, reason


def _rq_zscore_score(ts: TickStore, cfg: dict) -> Tuple[float, str]:
    z = ts.zscore(cfg["rq_zscore_window"])
    zmax = cfg["rq_zscore_max"]
    score = max(0.0, min(1.0, 1.0 - abs(z) / zmax)) if zmax > 0 else 0.0
    reason = f"rq_zscore={z:+.2f} max={zmax:.2f} score={score:.2f}"
    return score, reason


def _rq_sr_score(ts: TickStore, cfg: dict) -> Tuple[float, str]:
    sr = ts.support_resistance(cfg["rq_window"])
    if sr is None:
        return 0.5, "rq_sr=insufficient_data"
    support, resistance, edge_ratio = sr
    edge_gate = cfg["rq_sr_edge_gate"]
    if edge_ratio < edge_gate:
        return 0.0, (f"rq_sr BLOCKED price near channel edge "
                     f"edge_ratio={edge_ratio:.2f} gate={edge_gate:.2f}")
    # 0.5 (dead centre) → 1.0 score; at the gate itself → 0.0 score
    score = max(0.0, min(1.0, (edge_ratio - edge_gate) / (0.5 - edge_gate))) \
            if edge_gate < 0.5 else 1.0
    reason = (f"rq_sr support={support:.2f} resistance={resistance:.2f} "
              f"edge_ratio={edge_ratio:.2f} score={score:.2f}")
    return score, reason


def _rq_jump_check(ts: TickStore, cfg: dict) -> Tuple[bool, str]:
    """Hard gate — not weighted. Returns (blocked, reason)."""
    ticks_ago = ts.recent_jump(cfg["rq_jump_window"], cfg["rq_jump_sigma_mult"],
                               cfg["rq_jump_cooldown_ticks"])
    if ticks_ago is not None:
        return True, (f"rq_post_jump BLOCKED jump {ticks_ago} ticks ago "
                      f"(cooldown={cfg['rq_jump_cooldown_ticks']})")
    return False, "rq_post_jump clear"


def range_quiet_gate(ts: TickStore, cfg: dict) -> Tuple[bool, float, List[str]]:
    """
    Runs the 7-model RANGE_QUIET regime gate. Returns
    (passed, composite_score, reasons). The post-jump check is checked
    first and hard-blocks; the remaining 6 models are weighted and must
    clear cfg['rq_threshold'].
    """
    reasons: List[str] = []

    blocked, jr = _rq_jump_check(ts, cfg)
    reasons.append(jr)
    if blocked:
        return False, 0.0, reasons

    WEIGHTS = {
        "ou":    0.20,
        "rsi":   0.15,
        "srsi":  0.15,
        "boll":  0.20,
        "zscore":0.15,
        "sr":    0.15,
    }

    s_ou, r_ou       = _rq_ou_score(ts, cfg);      reasons.append(r_ou)
    s_rsi, r_rsi     = _rq_rsi_score(ts, cfg);     reasons.append(r_rsi)
    s_srsi, r_srsi   = _rq_srsi_score(ts, cfg);    reasons.append(r_srsi)
    s_boll, r_boll   = _rq_boll_score(ts, cfg);    reasons.append(r_boll)
    s_zscore, r_zsc  = _rq_zscore_score(ts, cfg);  reasons.append(r_zsc)
    s_sr, r_sr       = _rq_sr_score(ts, cfg);      reasons.append(r_sr)

    if s_sr == 0.0 and "BLOCKED" in r_sr:
        return False, 0.0, reasons

    composite = (WEIGHTS["ou"]     * s_ou +
                 WEIGHTS["rsi"]    * s_rsi +
                 WEIGHTS["srsi"]   * s_srsi +
                 WEIGHTS["boll"]   * s_boll +
                 WEIGHTS["zscore"] * s_zscore +
                 WEIGHTS["sr"]     * s_sr)
    threshold = cfg["rq_threshold"]
    reasons.append(f"rq_composite={composite:.3f} (threshold={threshold:.2f})")

    return composite >= threshold, composite, reasons


def _momentum_score(ts: TickStore, short_n: int, medium_n: int) -> Tuple[float, str]:
    """
    Layer 1 — Momentum.

    We want LOW momentum in both windows. Price that is moving strongly
    in any direction is more likely to exit the range.
    Score = 1.0 when momentum is near-zero, 0.0 when it's large.
    """
    prices = list(ts.prices)
    if len(prices) < medium_n + 1:
        return 0.5, "momentum=insufficient_data"

    # Short momentum: net tick direction over last short_n ticks
    short_prices = prices[-short_n-1:]
    short_move   = (short_prices[-1] - short_prices[0]) / abs(short_prices[0]) \
                   if short_prices[0] != 0 else 0.0

    # Medium momentum: net tick direction over last medium_n ticks
    med_prices = prices[-medium_n-1:]
    med_move   = (med_prices[-1] - med_prices[0]) / abs(med_prices[0]) \
                 if med_prices[0] != 0 else 0.0

    # Low |move| in both windows → high score
    # Normalise against typical move magnitude for 1Hz instruments
    norm = 0.0005
    s_score = max(0.0, 1.0 - abs(short_move) / norm)
    m_score = max(0.0, 1.0 - abs(med_move) / (norm * 2))
    score   = 0.5 * s_score + 0.5 * m_score
    reason  = (f"momentum_er short={short_move*100:+.3f}% "
               f"med={med_move*100:+.3f}% score={score:.2f}")

    return score, reason


def _vol_score(ts: TickStore, vol_window: int, vol_skip: float,
               barrier: float, duration_s: int) -> Tuple[float, str]:
    """
    Layer 2 — Volatility.

    Key insight: the expected absolute price move over `duration_s` ticks
    under GBM is approximately σ × √T × current_price (in price units).
    We compare this expected move against the barrier to assess feasibility.

    expected_move << barrier → high score (price likely stays inside)
    expected_move ≈ barrier  → moderate score
    expected_move >> barrier → skip (price likely exits)
    """
    sigma = ts.local_vol(vol_window)
    price = ts.current_price() or 1.0

    if sigma <= 0 or sigma >= vol_skip:
        return 0.0, f"vol=skip (σ={sigma:.6f})"

    # Expected price move magnitude in absolute units over duration
    expected_move = sigma * math.sqrt(duration_s) * price

    ratio = expected_move / barrier if barrier > 0 else 1.0
    # ratio < 0.5 → well inside → 1.0 score
    # ratio = 1.0 → expected move = barrier → 0.5 score
    # ratio > 1.5 → likely exit → 0.0 score
    score  = max(0.0, min(1.0, 1.0 - (ratio - 0.5)))
    reason = (f"vol_er σ={sigma:.5f} exp_move={expected_move:.2f} "
              f"barrier={barrier:.2f} ratio={ratio:.2f} score={score:.2f}")

    return score, reason


def _hurst_score(ts: TickStore, hurst_window: int) -> Tuple[float, str]:
    """
    Layer 3 — Hurst exponent.

    H < 0.5 (mean-reverting) is ideal. Mean-reverting price is likely to
    oscillate back toward centre rather than drift to edges.
    Score ramps from 0 at H=0.7 to 1.0 at H=0.3.
    """
    H      = ts.hurst(hurst_window)
    score  = max(0.0, min(1.0, (0.70 - H) / 0.40))
    reason = f"hurst_er H={H:.3f} score={score:.2f}"
    return score, reason


def _proximity_score(ts: TickStore, barrier: float,
                     centre_gate: float) -> Tuple[float, str]:
    """
    Layer 4 — Barrier proximity.

    If price has already moved more than centre_gate of the barrier from
    centre (i.e. is already near one edge), it's more likely to exit.
    Score based on how centred price is.
    """
    prices = list(ts.prices)
    if len(prices) < 10:
        return 0.5, "proximity=insufficient_data"

    price = prices[-1]
    # Centre reference: average of last 30 ticks (slow drift baseline)
    centre_window = prices[-30:] if len(prices) >= 30 else prices
    centre = sum(centre_window) / len(centre_window)

    # How far from centre relative to barrier?
    price_offset  = abs(price - centre)
    centre_ratio  = price_offset / barrier if barrier > 0 else 0.0
    if centre_ratio >= centre_gate:
        return 0.0, (f"proximity_er BLOCKED price near edge "
                     f"ratio={centre_ratio:.2f}")
    score  = max(0.0, 1.0 - centre_ratio / centre_gate)
    reason = (f"proximity_er offset={price_offset:.3f} "
              f"ratio={centre_ratio:.2f} score={score:.2f}")

    return score, reason


def mc_p_win_expiryrange(ts: TickStore, barrier: float, duration_s: int,
                          n_sims: int, window: int) -> Tuple[float, float, float]:
    """
    Layer 5 — Monte Carlo terminal distribution (GARCH-GBM).

    σ comes from a GARCH(1,1) one-step-ahead conditional-volatility fit
    (see TickStore.garch_sigma) instead of a flat rolling σ — it reacts
    to recent shocks rather than averaging them away. µ is still the
    simple rolling drift over the same shared window.

    Draws N terminal prices from GBM(µ, σ_garch, T=duration_s ticks).
    Returns (p_win, sigma_used, mu_used) so callers can log/compare
    what the MC actually assumed.
    """
    sigma = ts.garch_sigma(window)
    mu    = ts.local_drift(window)
    price = ts.current_price()
    if not price or sigma <= 0:
        return 0.0, sigma, mu

    mu_T    = (mu - 0.5 * sigma**2) * duration_s
    sigma_T = sigma * math.sqrt(duration_s)
    wins    = 0
    for _ in range(n_sims):
        u1 = random.random() or 1e-15
        u2 = random.random() or 1e-15
        z  = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
        terminal = price * math.exp(mu_T + sigma_T * z)
        if abs(terminal - price) < barrier:
            wins += 1
    return wins / n_sims, sigma, mu


def mc_p_win_bootstrap(ts: TickStore, barrier: float, duration_s: int,
                       n_sims: int, window: int) -> float:
    """
    Layer 5 cross-check — empirical bootstrap.

    Resamples actual historical tick returns (with replacement) instead
    of assuming Gaussian ones, so it naturally reflects whatever fat
    tails, skew, or recent jumps are actually present in the window —
    the GARCH-GBM MC above cannot see those by construction.

    Does NOT gate the trade — logged alongside the GARCH-GBM estimate
    purely for visibility/calibration. If the two disagree a lot, that's
    a signal the parametric assumption may be off, worth watching in
    the calibration log rather than acting on immediately.
    """
    price = ts.current_price()
    if not price:
        return 0.0
    terminals = ts.bootstrap_terminal_paths(window, duration_s, n_sims)
    if not terminals:
        return 0.0
    wins = sum(1 for t in terminals if abs(t - price) < barrier)
    return wins / len(terminals)


def evaluate_signal(ts: TickStore, cfg: dict) -> ContractSignal:
    """
    Runs the RANGE_QUIET regime gate first; only if it passes do layers
    1-4 run, producing a weighted score. If that score >= signal_threshold,
    Layer 5 runs: the GARCH-GBM MC (which gates on mc_min_confidence) plus
    the bootstrap cross-check (logged only, not gating).
    """
    sym     = ts.symbol
    barrier = cfg["barriers"].get(sym, 1.0)
    dur     = cfg["duration_s"]
    reasons = []

    # ── RANGE_QUIET gate (runs BEFORE layers 1-5) ──────────────────────
    rq_passed, rq_score, rq_reasons = range_quiet_gate(ts, cfg)
    reasons.extend(rq_reasons)
    if not rq_passed:
        return ContractSignal("SKIP", sym, barrier, 0.0, 0.0, rq_score,
                              reasons + ["RANGE_QUIET gate failed"])

    # Layer weights — MC is separate (mandatory threshold, not weighted)
    WEIGHTS = {
        "momentum":  0.30,
        "vol":       0.35,
        "hurst":     0.20,
        "proximity": 0.15,
    }

    # Layer 1 — Momentum
    s1, r1 = _momentum_score(ts, cfg["momentum_short_n"], cfg["momentum_medium_n"])
    reasons.append(r1)

    # Layer 2 — Volatility
    s2, r2 = _vol_score(ts, cfg["vol_window"], cfg["vol_skip_thresh"], barrier, dur)
    reasons.append(r2)
    if s2 == 0.0 and "skip" in r2:
        return ContractSignal("SKIP", sym, barrier, 0.0, 0.0, rq_score,
                              reasons + ["vol_skip triggered"])

    # Layer 3 — Hurst
    s3, r3 = _hurst_score(ts, cfg["hurst_window"])
    reasons.append(r3)

    # Layer 4 — Proximity
    s4, r4 = _proximity_score(ts, barrier, cfg["er_centre_gate"])
    reasons.append(r4)
    if s4 == 0.0 and "BLOCKED" in r4:
        return ContractSignal("SKIP", sym, barrier, 0.0, 0.0, rq_score,
                              reasons + ["proximity gate blocked"])

    # Weighted pre-MC score
    layer_score = (WEIGHTS["momentum"]  * s1 +
                   WEIGHTS["vol"]       * s2 +
                   WEIGHTS["hurst"]     * s3 +
                   WEIGHTS["proximity"] * s4)
    reasons.append(f"layer_score={layer_score:.3f} "
                   f"(threshold={cfg['signal_threshold']:.2f})")

    if layer_score < cfg["signal_threshold"]:
        return ContractSignal("SKIP", sym, barrier, 0.0, layer_score, rq_score,
                              reasons + ["below signal threshold"])

    # Layer 5 — Monte Carlo (only runs if RANGE_QUIET + layers 1-4 pass)
    mc_window = cfg["mc_window"]
    p_win, mc_sigma, mc_mu = mc_p_win_expiryrange(ts, barrier, dur,
                                                   cfg["mc_n_sims"], mc_window)
    boot_p_win = mc_p_win_bootstrap(ts, barrier, dur,
                                    cfg["mc_bootstrap_n_sims"], mc_window)
    divergence = abs(p_win - boot_p_win)

    reasons.append(f"MC(garch) p_win={p_win:.3f} σ={mc_sigma:.6f} µ={mc_mu:+.6f} "
                   f"(min={cfg['mc_min_confidence']:.2f})")
    reasons.append(f"MC(bootstrap) p_win={boot_p_win:.3f} "
                   f"divergence={divergence:.3f}")

    if p_win < cfg["mc_min_confidence"]:
        return ContractSignal("SKIP", sym, barrier, p_win, layer_score, rq_score,
                              reasons + ["MC below confidence threshold"],
                              mc_sigma, mc_mu, boot_p_win)

    return ContractSignal("EXPIRYRANGE", sym, barrier, p_win,
                          layer_score, rq_score, reasons,
                          mc_sigma, mc_mu, boot_p_win)


# ============================================================================
# PERSISTENCE (optional)
# ============================================================================

class PersistenceStore:
    def __init__(self, cfg: dict):
        self.url = cfg.get("supabase_url", "")
        self.key = cfg.get("supabase_key", "")
        self.ok  = bool(self.url and self.key and requests is not None)
        self._headers = {
            "apikey":        self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type":  "application/json",
            "Prefer":        "resolution=merge-duplicates",
        }

    def _upsert(self, table: str, row: dict):
        if not self.ok:
            return
        try:
            requests.post(f"{self.url}/rest/v1/{table}",
                          headers=self._headers, json=row, timeout=10)
        except Exception as exc:
            _log("STORE", f"Upsert failed: {exc}")

    def _select(self, table: str, key: str) -> Optional[dict]:
        if not self.ok:
            return None
        try:
            resp = requests.get(
                f"{self.url}/rest/v1/{table}?key=eq.{key}&select=*",
                headers=self._headers, timeout=10)
            rows = resp.json()
            return rows[0] if rows else None
        except Exception as exc:
            _log("STORE", f"Select failed: {exc}")
            return None

    def save_symbol_stats(self, sym: str, eng: "SignalEngine"):
        self._upsert("range_symbol_stats", {
            "key":          sym,
            "wins":         eng.wins,
            "losses":       eng.losses,
            "total_profit": eng.total_profit,
            "updated_at":   datetime.utcnow().isoformat(),
        })

    def load_symbol_stats(self, sym: str, eng: "SignalEngine"):
        row = self._select("range_symbol_stats", sym)
        if not row:
            return
        try:
            eng.wins         = int(row.get("wins", 0))
            eng.losses       = int(row.get("losses", 0))
            eng.total_profit = float(row.get("total_profit", 0.0))
            _log("STORE", f"{sym}: warm-started — "
                          f"{eng.wins}W/{eng.losses}L "
                          f"P&L=${eng.total_profit:+.2f}")
        except Exception as exc:
            _log("STORE", f"Failed to parse stats for {sym}: {exc}")

    def save_mc_calibration(self, row: dict):
        """
        Appends one row of predicted-vs-realized MC data (see
        _handle_settlement). Not a merge-duplicates upsert target like
        the stats table — each trade gets its own row, keyed by
        contract_id, so a history accumulates for later calibration
        analysis (e.g. binning by predicted p_win and checking realized
        win rate per bin).
        """
        self._upsert("range_mc_calibration", row)


# ============================================================================
# DERIV CLIENT — new Options API (OTP bootstrap, identical to PR05)
# ============================================================================

REST_BASE = "https://api.derivws.com"


class DerivClient:
    def __init__(self, cfg: dict):
        self.api_token  = cfg["api_token"]
        self.app_id     = cfg["app_id"]
        self.account_id = cfg.get("account_id") or None
        self.use_real   = bool(cfg.get("use_real_account", False))
        self.cfg        = cfg
        self.ws_url                = None
        self.ws                    = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._inbox:      Optional[asyncio.Queue] = None
        self._send_task:  Optional[asyncio.Task]  = None
        self._recv_task:  Optional[asyncio.Task]  = None
        self._req_id_counter: int  = 1
        self._pending_requests: dict = {}
        self.initial_balance: float = 0.0

    def _rest_request(self, path: str, method: str = "GET") -> dict:
        req = urllib.request.Request(
            f"{REST_BASE}{path}", method=method,
            headers={
                "Deriv-App-ID":  self.app_id,
                "Authorization": f"Bearer {self.api_token}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} from {path}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error calling {path}: {exc.reason}") from exc

    def _resolve_account_id(self) -> str:
        payload  = self._rest_request("/trading/v1/options/accounts")
        accounts = payload.get("data") or payload.get("accounts") or []
        if not accounts:
            raise RuntimeError("No accounts returned")
        wanted = "real" if self.use_real else "demo"
        for acc in accounts:
            t = str(acc.get("type") or acc.get("account_type") or "").lower()
            if t == wanted:
                return acc.get("account_id") or acc.get("id")
        first = accounts[0]
        return first.get("account_id") or first.get("id")

    def _fetch_ws_url(self) -> str:
        if not self.account_id:
            self.account_id = self._resolve_account_id()
            self.cfg["account_id"] = self.account_id
        payload = self._rest_request(
            f"/trading/v1/options/accounts/{self.account_id}/otp", method="POST")
        url = (payload.get("data") or {}).get("url")
        if not url:
            raise RuntimeError(f"OTP response missing url: {payload}")
        return url

    async def connect(self) -> bool:
        loop = asyncio.get_event_loop()
        try:
            self.ws_url = await loop.run_in_executor(None, self._fetch_ws_url)
        except Exception as exc:
            _log("AUTH", f"Failed to obtain OTP URL: {exc}")
            return False

        safe = self.ws_url.split("?")[0]
        _log("WS", f"Connecting → {safe} (account {self.account_id})")
        self.ws = await websockets.connect(
            self.ws_url,
            ping_interval=self.cfg["ws_ping_interval"],
            ping_timeout=20,
            close_timeout=10,
        )
        self._send_queue = asyncio.Queue()
        self._inbox      = asyncio.Queue()
        self._start_io()

        await self.send({"balance": 1})
        resp = await self.receive_type("balance", timeout=15)
        if resp is None or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("AUTH", f"Failed: {err}")
            return False
        bal = resp.get("balance", {})
        self.initial_balance = float(bal.get("balance", 0) or 0)
        _log("AUTH",
             f"OK | account {self.account_id} | "
             f"Balance: ${self.initial_balance:.2f} {bal.get('currency', '')}")
        return True

    def _start_io(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        self._send_task = asyncio.create_task(self._send_pump(), name="send_pump")
        self._recv_task = asyncio.create_task(self._recv_pump(), name="recv_pump")
        self._req_id_counter = 1
        self._pending_requests: dict = {}  # req_id -> asyncio.Future

    def _next_req_id(self) -> int:
        rid = self._req_id_counter
        self._req_id_counter += 1
        return rid

    async def _send_pump(self):
        while True:
            data, fut = await self._send_queue.get()
            try:
                await self.ws.send(json.dumps(data))
                if fut and not fut.done():
                    fut.set_result(True)
            except Exception as exc:
                if fut and not fut.done():
                    fut.set_exception(exc)
            finally:
                self._send_queue.task_done()

    async def _recv_pump(self):
        try:
            async for raw in self.ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                # Route by req_id if present — resolves a pending future
                # directly rather than going into the shared inbox, which
                # would get interleaved with ticks and consumed by the wrong
                # concurrent gather task.
                rid = msg.get("req_id")
                if rid and rid in self._pending_requests:
                    fut = self._pending_requests.pop(rid)
                    if not fut.done():
                        fut.set_result(msg)
                else:
                    await self._inbox.put(msg)
        except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK):
            # Cancel all pending request futures
            for fut in self._pending_requests.values():
                if not fut.done():
                    fut.cancel()
            self._pending_requests.clear()
            await self._inbox.put({"__disconnect__": True})
        except Exception as exc:
            _log("RECV", f"Error: {exc}")
            for fut in self._pending_requests.values():
                if not fut.done():
                    fut.cancel()
            self._pending_requests.clear()
            await self._inbox.put({"__disconnect__": True})

    async def send_with_id(self, data: dict, timeout: float = 12) -> Optional[dict]:
        """
        Sends a request with a unique req_id and awaits the response
        directly via a Future, bypassing the shared inbox entirely.
        This is how concurrent proposal fetches work correctly alongside
        a continuous tick stream — each proposal gets its own Future,
        matched by req_id when the response arrives.
        """
        loop = asyncio.get_event_loop()
        rid  = self._next_req_id()
        fut  = loop.create_future()
        self._pending_requests[rid] = fut
        data = dict(data)
        data["req_id"] = rid
        await self.send(data)
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(rid, None)
            if not fut.done():
                fut.cancel()
            return None
        except asyncio.CancelledError:
            self._pending_requests.pop(rid, None)
            return None

    async def close(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

    async def send(self, data: dict):
        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        await self._send_queue.put((data, fut))
        await fut

    async def receive(self, timeout: float = 10) -> dict:
        try:
            return await asyncio.wait_for(self._inbox.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return {}

    async def receive_type(self, msg_type: str, timeout: float = 10) -> Optional[dict]:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            if "__disconnect__" in msg:
                await self._inbox.put(msg)
                return None
            if msg_type in msg or "error" in msg:
                return msg
            await self._inbox.put(msg)

    async def subscribe_ticks(self, symbol: str) -> bool:
        await self.send({"ticks": symbol, "subscribe": 1})
        resp = await self.receive_type("tick", timeout=10)
        if resp is None or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("TICK", f"Subscribe failed for {symbol}: {err}")
            return False
        _log("TICK", f"Subscribed to {symbol}")
        return True

    async def fetch_balance(self) -> Optional[float]:
        try:
            await self.send({"balance": 1})
            resp = await self.receive_type("balance", timeout=10)
            if resp and "balance" in resp:
                return float(resp["balance"]["balance"])
        except Exception as exc:
            _log("BALANCE", f"Fetch error: {exc}")
        return None

    async def _fetch_proposal_ratio(self, req: dict, label: str,
                                    p_win_mc: float) -> Optional[float]:
        """
        Sends a proposal request via send_with_id (req_id-matched routing)
        so the response is delivered directly to a Future rather than
        going into the shared inbox where it would be interleaved with
        tick messages and consumed by the wrong concurrent gather task.

        Also renames 'symbol' -> 'underlying_symbol' — the new Options
        API renamed this field; sending 'symbol' returns
        InputValidationFailed: Properties not allowed: symbol.
        """
        if "symbol" in req:
            req["underlying_symbol"] = req.pop("symbol")

        resp = await self.send_with_id(req, timeout=12)
        if resp is None:
            _log("QUOTE", f"{label}: TIMEOUT (12s)")
            return None
        if "error" in resp:
            err = resp["error"]
            _log("QUOTE", f"{label}: {err.get('code','?')} — {err.get('message','?')}")
            return None
        data = resp.get("proposal", {})
        if not data:
            _log("QUOTE", f"{label}: no 'proposal' key — keys={list(resp.keys())}")
            return None
        ask  = float(data.get("ask_price", 0))
        pout = float(data.get("payout", 0))
        if ask <= 0:
            _log("QUOTE", f"{label}: ask_price=0")
            return None
        ratio = (pout - ask) / ask
        _log("QUOTE", f"{label}: p_mc={p_win_mc:.3f} ask={ask:.2f} "
                      f"payout={pout:.2f} ratio={ratio:.3f} "
                      f"ev={p_win_mc*ratio-(1-p_win_mc):+.4f}")
        return ratio

    async def place_trade(self, sig: TradeSignal) -> Optional[str]:
        """
        Proposal + buy at fixed barrier. Uses sig.barrier directly.
        EXPIRYRANGE: barrier + barrier2 (symmetric ±)
        """
        contract_type = sig.contract_type
        barrier       = sig.barrier
        req = {
            "proposal":           1,
            "amount":             sig.stake,
            "basis":              "stake",
            "contract_type":      contract_type,
            "currency":           self.cfg["currency"],
            "duration":           self.cfg["duration_s"],
            "duration_unit":      self.cfg["duration_unit"],
            "underlying_symbol":  sig.symbol,
            "barrier":            f"+{barrier:.2f}",
            "barrier2":           f"-{barrier:.2f}",
        }

        proposal = await self.send_with_id(req, timeout=12)
        if proposal is None or "error" in proposal:
            err = (proposal or {}).get("error", {}).get("message", "timeout")
            _log("PROPOSAL", f"Error: {err}")
            return None

        prop_data   = proposal.get("proposal", {})
        proposal_id = prop_data.get("id")
        ask_price   = float(prop_data.get("ask_price", sig.stake))
        payout      = float(prop_data.get("payout", 0))
        if not proposal_id:
            _log("PROPOSAL", "No proposal ID")
            return None

        ratio = (payout - ask_price) / ask_price if ask_price > 0 else 0
        _log("PROPOSAL",
             f"{contract_type} {sig.symbol} +{barrier:.2f} "
             f"ask=${ask_price:.2f} payout=${payout:.2f} "
             f"return={ratio*100:.1f}%")

        buy_time    = time.time()
        contract_id = None
        await self.send({"buy": proposal_id, "price": ask_price})

        for attempt in range(self.cfg["buy_recv_retries"]):
            resp = await self.receive_type("buy", timeout=8)
            if resp is None:
                _log("BUY", f"No response (attempt {attempt+1})")
                continue
            if "error" in resp:
                _log("BUY", f"Error: {resp['error'].get('message','')}")
                return None
            contract_id = resp.get("buy", {}).get("contract_id")
            if contract_id:
                break

        if not contract_id:
            _log("BUY", "No contract_id — running orphan recovery")
            contract_id = await self._recover_orphan(sig.stake, buy_time)
            if contract_id:
                _log("BUY", f"Orphan recovered → {contract_id}")
            else:
                _log("BUY", "Orphan recovery failed — unlocking")
                return None

        _log("TRADE",
             f"{contract_type} {sig.symbol} ${sig.stake:.2f} "
             f"{self.cfg['duration_s']}s contract={contract_id}")
        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id, "subscribe": 1})
        except Exception as exc:
            _log("TRADE", f"Subscribe to updates failed: {exc}")

        return str(contract_id)

    async def _recover_orphan(self, stake: float, buy_time: float) -> Optional[str]:
        for attempt in range(self.cfg["orphan_poll_attempts"]):
            await asyncio.sleep(self.cfg["orphan_poll_interval"])
            try:
                await self.send({"profit_table": 1, "description": 1,
                                 "sort": "DESC", "limit": 5})
                resp = await self.receive_type("profit_table", timeout=10)
                if not resp or "error" in resp:
                    continue
                for tx in resp.get("profit_table", {}).get("transactions", []):
                    if (abs(float(tx.get("buy_price", 0)) - stake) < 0.01 and
                            float(tx.get("purchase_time", 0)) >= buy_time - 5):
                        return str(tx.get("contract_id"))
            except Exception as exc:
                _log("ORPHAN", f"Poll {attempt+1} error: {exc}")
        return None


# ============================================================================
# SIGNAL ENGINE (lightweight wrapper — logic now lives in evaluate_signal())
# ============================================================================

class SignalEngine:
    def __init__(self, symbol: str, cfg: dict):
        self.symbol       = symbol
        self.cfg          = cfg
        self.ts           = TickStore(symbol, maxlen=cfg["tick_window"])
        self.wins         = 0
        self.losses       = 0
        self.total_profit = 0.0

    def add_tick(self, price: float):
        self.ts.add(price)

    def is_ready(self) -> bool:
        return self.ts.is_ready(self.cfg["min_ticks"])


# ============================================================================
# PER-SYMBOL STATE
# ============================================================================

@dataclass
class SymbolState:
    symbol:           str
    engine:           SignalEngine
    waiting:          bool           = False
    contract_id:      Optional[str]  = None
    current_sig:      Optional[TradeSignal] = None
    lock_since:       Optional[float] = None
    last_eval_time:   float           = 0.0
    balance_before:   Optional[float] = None
    consec_losses:    int             = 0
    cb_paused_until:  float           = 0.0
    martingale_step:  int             = 0   # 0 = base stake; resets on win or after max_steps


# ============================================================================
# MAIN BOT
# ============================================================================

class RangeBot:
    def __init__(self, cfg: dict = CONFIG):
        self.cfg    = cfg
        self.client = DerivClient(cfg)
        self.store  = PersistenceStore(cfg)

        self.symbols: List[str] = cfg["symbols"]
        self.states:  Dict[str, SymbolState] = {
            sym: SymbolState(sym, SignalEngine(sym, cfg))
            for sym in self.symbols
        }

        self.balance:            float         = 0.0
        self.session_start_bal:  float         = 0.0
        self.total_profit:       float         = 0.0
        self._stop:              bool          = False
        self._last_persist:      float         = 0.0

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _is_settled(self, data: dict) -> bool:
        if data.get("is_settled"):
            return True
        for key in ("status", "contract_status"):
            if data.get(key, "").lower() in ("sold", "won", "lost"):
                return True
        return False

    def _can_trade_session(self) -> bool:
        stop = self.cfg["stop_loss"]
        if self.session_start_bal > 0 and self.cfg.get("stop_loss_pct", 0) > 0:
            stop = self.session_start_bal * self.cfg["stop_loss_pct"]
        if self.total_profit <= -stop:
            _log("RISK", f"Session stop-loss hit (${self.total_profit:.2f})")
            return False
        return True

    def _current_stake(self, st: SymbolState) -> float:
        """
        Stake for the NEXT trade on this symbol, driven purely by the
        martingale step counter — never by balance or account size.
        step 0 → base stake; each loss bumps step (up to max_steps);
        a win (or exhausting max_steps) resets step to 0 in _handle_settlement.
        """
        base = self.cfg["stake"]
        if not self.cfg.get("martingale_enabled", False):
            stake = base
        else:
            factor = self.cfg["martingale_factor"]
            stake  = base * (factor ** st.martingale_step)
        stake = min(stake, self.cfg["max_stake"])
        return round(stake, 2)

    def _check_lock_timeout(self, st: SymbolState):
        if not st.waiting or st.lock_since is None:
            return
        elapsed = time.monotonic() - st.lock_since
        timeout = self.cfg["lock_timeout"]
        if elapsed >= timeout:
            _log("TIMEOUT",
                 f"{st.symbol} locked {elapsed:.0f}s (limit {timeout}s) — unlocking")
            st.waiting      = False
            st.contract_id  = None
            st.current_sig  = None
            st.lock_since   = None

    # ── Quote fetching ────────────────────────────────────────────────────────

    # ── Per-symbol evaluation ─────────────────────────────────────────────────

    async def _evaluate_symbol(self, st: SymbolState):
        """
        Full evaluation cycle for one symbol.
        New approach: run 5-layer intelligence at fixed barriers, no grid scan.
        If both contract types pass, pick the one with the higher MC confidence.
        """
        if st.waiting:
            return
        now = time.monotonic()
        if now - st.last_eval_time < self.cfg["eval_cooldown"]:
            return
        if now < st.cb_paused_until:
            _log("BREAKER", f"{st.symbol} paused {st.cb_paused_until - now:.0f}s")
            return
        if not self._can_trade_session():
            return
        if not st.engine.is_ready():
            return

        st.last_eval_time = now
        sym  = st.symbol
        ts   = st.engine.ts

        # RANGE_QUIET gate + 5-layer EXPIRYRANGE intelligence
        sig = evaluate_signal(ts, self.cfg)
        chosen: Optional[ContractSignal] = sig if sig.contract_type != "SKIP" else None

        # Always print the signal block
        print(f"\n{'='*60}")
        print(f"SIGNAL  {sym}  {_ts()}")
        print(f"  [ER] rq_score={sig.rq_score:.3f}")
        for r in sig.reasons:
            print(f"    · {r}")
        if not chosen:
            print(f"  → No trade")
        else:
            stake = self._current_stake(st)
            mg_tag = (f" [martingale step {st.martingale_step}/"
                      f"{self.cfg['martingale_max_steps']}]"
                      if self.cfg.get("martingale_enabled") and st.martingale_step > 0
                      else "")
            print(f"  → {chosen.contract_type} barrier=±{chosen.barrier:.2f} "
                  f"p_win(garch)={chosen.p_win_mc:.3f} "
                  f"p_win(boot)={chosen.mc_bootstrap_p_win:.3f} "
                  f"layer_score={chosen.layer_score:.3f} "
                  f"stake=${stake:.2f}{mg_tag}")
        print(f"{'='*60}")

        if not chosen:
            return

        # Convert to TradeSignal for place_trade
        stake = self._current_stake(st)
        trade_sig = TradeSignal(
            contract_type = chosen.contract_type,
            symbol        = sym,
            barrier       = chosen.barrier,
            p_win_mc      = chosen.p_win_mc,
            layer_score   = chosen.layer_score,
            stake         = stake,
            reasons       = chosen.reasons,
            mc_sigma            = chosen.mc_sigma,
            mc_mu               = chosen.mc_mu,
            mc_bootstrap_p_win  = chosen.mc_bootstrap_p_win,
            entry_price         = ts.current_price() or 0.0,
            entry_time          = time.monotonic(),
        )

        # Snap balance before trade
        bal = await self.client.fetch_balance()
        if bal is not None:
            self.balance      = bal
            st.balance_before = bal

        # Place trade
        contract_id = await self.client.place_trade(trade_sig)
        if contract_id:
            st.waiting     = True
            st.contract_id = contract_id
            st.current_sig = trade_sig
            st.lock_since  = time.monotonic()
            _log("LOCK", f"{sym} waiting on {contract_id}")
        else:
            st.balance_before = None

    # ── Settlement ────────────────────────────────────────────────────────────

    async def _handle_settlement(self, sym: str, data: dict):
        st = self.states.get(sym)
        if st is None or not st.waiting:
            return
        cid = str(data.get("contract_id", ""))
        if cid != st.contract_id:
            return
        if not self._is_settled(data):
            return

        bal_after  = await self.client.fetch_balance()
        api_profit = float(data.get("profit", 0))

        if bal_after is not None and st.balance_before is not None:
            actual = round(bal_after - st.balance_before, 2)
            _log("BALANCE",
                 f"{sym} pre=${st.balance_before:.2f} → post=${bal_after:.2f} "
                 f"| actual={actual:+.2f} | api={api_profit:+.2f}")
        else:
            actual = api_profit

        print(f"\n{'='*60}")
        print(f"RESULT  {sym}  contract={cid}")
        print(f"        profit={actual:+.2f}")
        print(f"{'='*60}")

        # ── MC calibration log ──────────────────────────────────────────
        # Compare what the MC predicted at entry against what actually
        # happened, before we clear st.current_sig below.
        if self.cfg.get("mc_calibration_log", False) and st.current_sig:
            sig = st.current_sig
            exit_price = (data.get("exit_spot") or data.get("sell_spot")
                         or data.get("current_spot"))
            try:
                exit_price = float(exit_price) if exit_price is not None else None
            except (TypeError, ValueError):
                exit_price = None
            if exit_price is None:
                exit_price = st.engine.ts.current_price()

            hold_secs = (time.monotonic() - sig.entry_time) if sig.entry_time else None
            realized_move = (abs(exit_price - sig.entry_price)
                             if exit_price and sig.entry_price else None)
            realized_win  = actual > 0

            calib_row = {
                "contract_id":         cid,
                "symbol":              sym,
                "settled_at":          datetime.utcnow().isoformat(),
                "entry_price":         sig.entry_price,
                "exit_price":          exit_price,
                "barrier":             sig.barrier,
                "predicted_p_win_garch":     sig.p_win_mc,
                "predicted_p_win_bootstrap": sig.mc_bootstrap_p_win,
                "mc_sigma":            sig.mc_sigma,
                "mc_mu":               sig.mc_mu,
                "hold_secs":           hold_secs,
                "realized_move":       realized_move,
                "realized_win":        realized_win,
                "profit":              actual,
            }
            self.store.save_mc_calibration(calib_row)

            move_str = f"{realized_move:.3f}" if realized_move is not None else "?"
            _log("CALIB",
                 f"{sym} predicted(garch)={sig.p_win_mc:.3f} "
                 f"predicted(boot)={sig.mc_bootstrap_p_win:.3f} "
                 f"σ_used={sig.mc_sigma:.6f} realized_move={move_str} "
                 f"barrier={sig.barrier:.2f} win={realized_win}")

        if actual > 0:
            st.engine.wins += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses = 0
            if st.martingale_step > 0:
                _log("MARTINGALE", f"{sym} win at step {st.martingale_step} → reset to base stake")
            st.martingale_step = 0
            _log("WIN", f"{sym} +${actual:.2f} | "
                        f"session P&L ${self.total_profit:+.2f}")
        else:
            st.engine.losses += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses += 1
            _log("LOSS", f"{sym} ${actual:.2f} | "
                         f"session P&L ${self.total_profit:+.2f} | "
                         f"streak={st.consec_losses}")

            if self.cfg.get("martingale_enabled", False):
                max_steps = self.cfg["martingale_max_steps"]
                if st.martingale_step >= max_steps:
                    _log("MARTINGALE",
                         f"{sym} max steps ({max_steps}) reached without a win → "
                         f"reset to base stake")
                    st.martingale_step = 0
                else:
                    st.martingale_step += 1
                    next_stake = self._current_stake(st)
                    _log("MARTINGALE",
                         f"{sym} step → {st.martingale_step}/{max_steps} "
                         f"(next stake ${next_stake:.2f})")

            limit = self.cfg["consec_loss_limit"]
            if st.consec_losses >= limit:
                pause = self.cfg["consec_pause_secs"]
                st.cb_paused_until = time.monotonic() + pause
                st.consec_losses   = 0
                _log("BREAKER",
                     f"{sym} {limit} consecutive losses → pausing {pause}s")

        if bal_after is not None:
            self.balance = bal_after

        st.waiting      = False
        st.contract_id  = None
        st.current_sig  = None
        st.lock_since   = None
        st.balance_before = None

    # ── Reconnect ─────────────────────────────────────────────────────────────

    async def _reconnect(self) -> bool:
        delay = self.cfg["reconnect_delay_min"]
        max_d = self.cfg["reconnect_delay_max"]
        attempt = 0
        while not self._stop:
            attempt += 1
            _log("RECONNECT", f"Attempt {attempt} in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_d)
            await self.client.close()
            self.client = DerivClient(self.cfg)
            try:
                if not await self.client.connect():
                    continue
                for sym in self.symbols:
                    if not await self.client.subscribe_ticks(sym):
                        continue
                # Re-attach to any open contracts
                for st in self.states.values():
                    if st.waiting and st.contract_id:
                        cid = st.contract_id
                        _log("RECONNECT", f"Re-attaching to {cid}")
                        try:
                            await self.client.send({
                                "proposal_open_contract": 1,
                                "contract_id": cid, "subscribe": 1})
                        except Exception:
                            pass
                _log("RECONNECT", "OK")
                self.balance = self.client.initial_balance
                return True
            except Exception as exc:
                _log("RECONNECT", f"Error: {exc}")
        return False

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        print(f"\n{'='*60}")
        print("  DERIV EXPIRYRANGE BOT")
        print(f"{'='*60}")
        print(f"  Symbols  : {', '.join(self.symbols)}")
        print(f"  Contracts: EXPIRYRANGE (gated on RANGE_QUIET regime, "
              f"threshold={self.cfg['rq_threshold']:.2f})")
        if self.cfg.get("martingale_enabled", False):
            print(f"  Stake    : ${self.cfg['stake']:.2f} base, martingale "
                  f"x{self.cfg['martingale_factor']} up to "
                  f"{self.cfg['martingale_max_steps']} steps "
                  f"(max ${self.cfg['max_stake']:.2f})")
        else:
            print(f"  Stake    : ${self.cfg['stake']:.2f} flat  "
                  f"(max ${self.cfg['max_stake']:.2f})")
        print(f"  Stop-loss: ${self.cfg['stop_loss']:.2f}")
        print(f"  Cooldown : {self.cfg['eval_cooldown']}s between evals per symbol")
        print(f"{'='*60}\n")

        if not self.cfg["api_token"]:
            _log("ERROR", "Set DERIV_API_TOKEN env var before running")
            return
        if not self.cfg["app_id"]:
            _log("ERROR", "Set DERIV_APP_ID env var (register at developers.deriv.com)")
            return

        if not await self.client.connect():
            return

        self.balance           = self.client.initial_balance
        self.session_start_bal = self.client.initial_balance

        # Warm-start persisted stats
        for sym, st in self.states.items():
            self.store.load_symbol_stats(sym, st.engine)

        # Subscribe to all symbols
        for sym in self.symbols:
            if not await self.client.subscribe_ticks(sym):
                _log("ERROR", f"Could not subscribe to {sym} — aborting")
                return

        _log("BOT", f"Live — warming up ({self.cfg['min_ticks']} ticks needed per symbol)...")

        console_task = asyncio.create_task(self._console(), name="console")

        try:
            while not self._stop:
                response = await self.client.receive(timeout=60)

                if "__disconnect__" in response:
                    _log("WS", "Disconnected — reconnecting")
                    if not await self._reconnect():
                        break
                    continue

                if not response:
                    try:
                        await self.client.ws.ping()
                    except Exception:
                        _log("WS", "Ping failed — reconnecting")
                        if not await self._reconnect():
                            break
                    continue

                # Route ticks to the correct symbol's TickStore
                if "tick" in response:
                    tick = response["tick"]
                    sym  = tick.get("symbol") or tick.get("instrument_id", "")
                    if sym in self.states:
                        st = self.states[sym]
                        self._check_lock_timeout(st)
                        st.engine.add_tick(float(tick.get("quote", 0)))

                        # Periodic persist
                        now = time.monotonic()
                        if (self.store.ok and
                                now - self._last_persist >= self.cfg["persist_every_secs"]):
                            self._last_persist = now
                            for s, sx in self.states.items():
                                self.store.save_symbol_stats(s, sx.engine)

                        # Evaluate
                        if st.engine.is_ready() and not st.waiting:
                            await self._evaluate_symbol(st)

                # Settlement
                if "proposal_open_contract" in response:
                    poc = response["proposal_open_contract"]
                    cid = str(poc.get("contract_id", ""))
                    for st in self.states.values():
                        if st.contract_id == cid:
                            await self._handle_settlement(st.symbol, poc)
                            break

                if "transaction" in response:
                    tx  = response["transaction"]
                    cid = str(tx.get("contract_id", ""))
                    if cid:
                        for st in self.states.values():
                            if st.contract_id == cid:
                                await self._handle_settlement(st.symbol, {
                                    "contract_id": cid,
                                    "profit":      tx.get("profit", 0),
                                    "status":      tx.get("action", ""),
                                    "is_settled":  True,
                                })
                                break

        except KeyboardInterrupt:
            print("\n\nInterrupted")
        except Exception as exc:
            print(f"\nUnhandled error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            console_task.cancel()
            for sym, st in self.states.items():
                self.store.save_symbol_stats(sym, st.engine)
            if self.store.ok:
                _log("STORE", "Final stats saved.")
            await self.client.close()
            print(f"\n{'='*60}")
            print("  FINAL SESSION RESULTS")
            print(f"{'='*60}")
            for sym, st in self.states.items():
                total = st.engine.wins + st.engine.losses
                wr    = st.engine.wins / total * 100 if total else 0
                print(f"  {sym}: {total} trades | "
                      f"W:{st.engine.wins} L:{st.engine.losses} | "
                      f"WR:{wr:.1f}% | P&L:${st.engine.total_profit:+.2f}")
            print(f"  Session P&L: ${self.total_profit:+.2f}")
            print(f"{'='*60}")
            print("Goodbye")

    async def _console(self):
        loop = asyncio.get_event_loop()
        _log("CMD", "Commands: [s]tats  [u]nlock <symbol>  [q]uit")
        while not self._stop:
            try:
                cmd = (await loop.run_in_executor(None, input)).strip().lower()
                if cmd == "s":
                    for sym, st in self.states.items():
                        eng   = st.engine
                        total = eng.wins + eng.losses
                        wr    = eng.wins / total * 100 if total else 0
                        sigma   = eng.ts.local_vol(self.cfg["vol_window"])
                        hurst   = eng.ts.hurst(self.cfg["hurst_window"])
                        barrier = self.cfg["barriers"].get(sym, "?")
                        rq_pass, rq_score, _ = range_quiet_gate(eng.ts, self.cfg)
                        print(f"\n  {sym}: W:{eng.wins} L:{eng.losses} "
                              f"WR:{wr:.1f}% P&L:${eng.total_profit:+.2f}")
                        print(f"    σ={sigma:.6f}  H={hurst:.3f}  ticks={eng.ts.count}")
                        print(f"    barrier ER=±{barrier}  "
                              f"RANGE_QUIET={'PASS' if rq_pass else 'fail'} "
                              f"score={rq_score:.3f}")
                        print(f"    waiting={st.waiting}  "
                              f"cb_paused={st.cb_paused_until > time.monotonic()}  "
                              f"martingale_step={st.martingale_step}/{self.cfg['martingale_max_steps']}  "
                              f"next_stake=${self._current_stake(st):.2f}")
                    print(f"\n  Session P&L: ${self.total_profit:+.2f}  "
                          f"Balance: ${self.balance:.2f}")
                elif cmd.startswith("u "):
                    sym = cmd[2:].strip().upper()
                    if sym in self.states:
                        st = self.states[sym]
                        st.waiting = False
                        st.contract_id = None
                        st.current_sig = None
                        st.lock_since  = None
                        _log("CMD", f"Unlocked {sym}")
                    else:
                        _log("CMD", f"Unknown symbol: {sym}")
                elif cmd in ("q", "quit", "exit"):
                    _log("CMD", "Quit")
                    self._stop = True
                    break
            except (EOFError, KeyboardInterrupt):
                break


# ============================================================================
# SUPABASE SCHEMA (run once in Supabase SQL editor before first deploy)
# ============================================================================
# CREATE TABLE IF NOT EXISTS range_symbol_stats (
#     key            TEXT PRIMARY KEY,
#     wins           INTEGER NOT NULL DEFAULT 0,
#     losses         INTEGER NOT NULL DEFAULT 0,
#     total_profit   DOUBLE PRECISION NOT NULL DEFAULT 0,
#     updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
# );
# NOTIFY pgrst, 'reload schema';


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main():
    bot = RangeBot(CONFIG)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
