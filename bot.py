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
    · Draws N terminal prices from GBM(µ, σ, T=120s)
    · P(|terminal - entry| < barrier) is the direct estimate
    · Must clear MIN_MC_CONFIDENCE threshold
    · MC is the final arbiter — all other layers passing but MC
      saying P(win) < threshold → no trade

  AGREEMENT LOGIC:
    · RANGE_QUIET gate must pass first (see above)
    · Each layer votes pass/fail with a confidence weight
    · Total weighted score must exceed SIGNAL_THRESHOLD
    · MC confidence must independently exceed MIN_MC_CONFIDENCE
    · All conditions must hold simultaneously

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
import uuid
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

    # ── Symbols & strategies ─────────────────────────────────────────
    # Each symbol maps to a LIST of strategies that run independently and
    # concurrently for it. Each strategy evaluates its own signal on its
    # own schedule and can fire its own trade with its own barrier — they
    # are not paired against each other. "expiryrange" = the original
    # RANGE_QUIET strategy. "touch"/"notouch" = single-barrier ONETOUCH/
    # NOTOUCH contracts, added for symbols (like RDBEAR) whose real
    # behavior is "wide-swinging" rather than "quiet range" — see the
    # RDBEAR structural analysis: EXPIRYRANGE essentially never fires for
    # it, but its volatility supports Touch/No-Touch at sane barrier
    # widths (validated against RDBEAR's real logged sigma before this
    # was added).
    "symbols": {
        "1HZ10V": ["expiryrange"],
        "RDBEAR":  ["touch", "notouch"],
    },
    "currency":         "USD",

    # ── Fixed barriers (confirmed from live API testing) ──────────
    # These are the known-valid barrier sizes for 2-minute expiry.
    # We do not scan for barriers — they are fixed. The signal layers
    # decide whether market conditions justify trading them.
    # EVERY symbol in "symbols" MUST have an entry here — there is no
    # silent fallback any more (see _validate_symbol_config). A missing
    # entry means "I haven't validated a barrier for this symbol against
    # the live API yet," not "assume 1.0."
    #
    # TODO(RDBEAR): 1.0 below is a PLACEHOLDER, not a validated barrier.
    # RDBEAR trades around 918-926 (vs 1HZ10V's much smaller price scale)
    # so 1HZ10V's ±1.60 is not a safe stand-in. Confirm the real tradeable
    # barrier for RDBEAR at 120s expiry via the Deriv API before going live.
    # NOTE: this "barriers" dict is used ONLY by the expiryrange strategy
    # (a symmetric ± range). Touch/No-Touch use their own barrier dicts
    # below, since they take a single signed barrier, not a symmetric one.
    "barriers": {
        "1HZ10V": 1.60,
        "RDBEAR": 1.00,   # PLACEHOLDER — validate before trading real money
    },

    # ── Touch / No-Touch barriers (single, signed distance from entry) ──
    # TODO(RDBEAR): both PLACEHOLDERS derived from the feasibility check
    # against RDBEAR's real logged sigma (median ~0.000384 at 120s):
    #   touch_barriers   ~±2.2  -> ~60% modeled touch probability
    #   notouch_barriers ~±4.5  -> a comfortably wide miss-distance
    # These are starting points for shadow evaluation, NOT validated
    # against Deriv's actual quoted payout for these barriers yet — that
    # quote is what determines real edge, not the GBM model alone.
    "touch_barriers": {
        "RDBEAR": 2.20,
    },
    "notouch_barriers": {
        "RDBEAR": 4.50,
    },
    # Minimum modeled probability required to fire, same spirit as
    # mc_min_confidence for expiryrange.
    "touch_min_confidence":   0.55,
    "notouch_min_confidence": 0.55,

    # ── Per-symbol volatility skip threshold (Layer 2) ─────────────
    # Different symbols have different baseline tick-to-tick volatility,
    # so one global vol_skip_thresh does not transfer across symbols.
    # A single global value (0.000300) tuned for 1HZ10V silently killed
    # every RDBEAR signal, since RDBEAR's typical σ (~0.00033-0.00043,
    # observed live) sits above that threshold — Layer 2 hard-skips
    # whenever σ >= vol_skip, so the signal never even reached Layer 3+
    # or the Monte Carlo layer.
    #
    # "default" is used for any symbol without its own explicit entry.
    # RDBEAR's value below is set just above its observed live range —
    # treat it as a starting point to re-tune with more data, not gospel.
    "vol_skip_thresh": {
        "default":  _env("VOL_SKIP", 0.000300),
        "1HZ10V":   _env("VOL_SKIP_1HZ10V", 0.000300),
        "RDBEAR":   _env("VOL_SKIP_RDBEAR", 0.000500),
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
    "vol_window":         60,   # ticks for σ estimate
    # How many ticks does price typically move in 120 seconds at this vol?
    # Expected move = σ × √120. Barrier must be > expected move for the
    # trade to be reasonable — this ratio gates signal confidence.
    # (per-symbol "vol_skip_thresh" dict is defined above, under Symbols)
    # Layer 3: Hurst
    "hurst_window":       80,
    # Layer 4: Barrier proximity (price vs centre of the range)
    "er_centre_gate":     0.65,  # skip EXPIRYRANGE if price moved >65% of barrier from centre
    # Layer 5: Monte Carlo
    "mc_n_sims":          2000,
    "mc_min_confidence":  _env("MC_MIN_CONF", 0.72),  # MC p(win) must exceed this

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

    # ── Remote-tunable config (replaces "edit Railway var, redeploy") ──
    # The bot polls range_bot_config in Supabase every remote_config_poll_s
    # seconds. The ML model is always synced from there (shadow-mode only,
    # zero trading risk). Thresholds that actually gate live trades
    # (rq_threshold, signal_threshold, mc_min_confidence, vol_skip_thresh,
    # barriers) only sync if remote_config_enabled is explicitly True —
    # default is False, so nothing about live trading changes until you
    # deliberately turn this on. See RangeBot._apply_remote_config.
    "remote_config_enabled": _env("REMOTE_CONFIG_ENABLED", False),
    "remote_config_poll_s":  _env("REMOTE_CONFIG_POLL_S", 900),
    "ml_model": None,   # populated at runtime from range_bot_config, shadow-only
    "persist_every_secs": _env("PERSIST_EVERY_SECS", 120),
}


# ============================================================================
# HELPERS
# ============================================================================

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _log(tag: str, msg: str):
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)


def _sym_vol_skip(cfg: dict, sym: str) -> float:
    """Per-symbol vol_skip_thresh lookup, falling back to 'default'."""
    table = cfg["vol_skip_thresh"]
    return table.get(sym, table["default"])


def _validate_symbol_config(cfg: dict) -> List[str]:
    """
    Fail loud instead of silently defaulting. Returns a list of problem
    descriptions (empty list = all good). Called once at startup so a
    symbol/strategy switch can never quietly run with an unvalidated
    barrier or a vol threshold borrowed from another symbol.

    cfg["symbols"] is {symbol: [strategy, ...]} — each strategy for each
    symbol needs its own validated barrier config, since expiryrange,
    touch, and notouch each use a different barrier dict.
    """
    problems = []
    for sym, strategies in cfg["symbols"].items():
        if sym not in cfg["vol_skip_thresh"]:
            _log("CONFIG",
                 f"{sym}: no per-symbol vol_skip_thresh — using "
                 f"'default'={cfg['vol_skip_thresh']['default']:.6f}. "
                 f"This may be miscalibrated for {sym}; consider adding "
                 f"an explicit entry once you have live σ data.")
        for strat in strategies:
            if strat == "expiryrange":
                if sym not in cfg["barriers"]:
                    problems.append(
                        f"{sym} (expiryrange): no entry in cfg['barriers'] — "
                        f"refusing to guess a barrier. Add a validated "
                        f"barrier for {sym} first.")
            elif strat == "touch":
                if sym not in cfg["touch_barriers"]:
                    problems.append(
                        f"{sym} (touch): no entry in cfg['touch_barriers'] — "
                        f"add a barrier before enabling this strategy.")
            elif strat == "notouch":
                if sym not in cfg["notouch_barriers"]:
                    problems.append(
                        f"{sym} (notouch): no entry in cfg['notouch_barriers'] "
                        f"— add a barrier before enabling this strategy.")
            else:
                problems.append(f"{sym}: unknown strategy '{strat}'")
    return problems


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
    """Result of a strategy's signal evaluation."""
    contract_type:  str       # "EXPIRYRANGE" | "ONETOUCH" | "NOTOUCH" | "SKIP"
    symbol:         str
    barrier:        float     # unsigned magnitude. For EXPIRYRANGE: ± range
                               # half-width. For ONETOUCH/NOTOUCH: distance
                               # from entry in the direction below.
    p_win_mc:       float     = 0.0    # modeled probability estimate
    layer_score:    float     = 0.0    # weighted score from layers 1-4 (0-1)
    rq_score:       float     = 0.0    # RANGE_QUIET composite score (0-1);
                                        # 0 for touch/notouch, which don't
                                        # use this gate.
    reasons:        List[str] = field(default_factory=list)
    direction:      Optional[str] = None   # 'up' | 'down' — single-barrier
                                            # contracts only (touch/notouch).
                                            # None for expiryrange. Placed
                                            # LAST to preserve positional-arg
                                            # compatibility with existing
                                            # ContractSignal(...) call sites.


@dataclass
class TradeSignal:
    """Resolved trade instruction passed to place_trade()."""
    contract_type:  str
    symbol:         str
    barrier:        float     # unsigned magnitude — see ContractSignal
    p_win_mc:       float     = 0.0
    layer_score:    float     = 0.0
    rq_score:       float     = 0.0
    stake:          float     = 0.0
    reasons:        List[str] = field(default_factory=list)
    direction:      Optional[str] = None


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


def _metrics_snapshot(ts: TickStore, cfg: dict) -> dict:
    """
    Independent, read-only snapshot of every raw indicator value at this
    instant — used purely for logging/analysis, never for trading
    decisions. Kept separate from the scoring functions above so nothing
    here can accidentally change what the bot actually trades on.

    Recomputing these is cheap (small rolling windows) and keeps this
    completely decoupled from range_quiet_gate/evaluate_signal internals,
    so tuning the logger can never risk the live decision path.
    """
    m: dict = {}
    try:
        m["ou_theta"] = ts.ou_theta(cfg["rq_window"])
    except Exception:
        m["ou_theta"] = None
    try:
        m["rsi"] = ts.rsi(cfg["rq_rsi_window"])
    except Exception:
        m["rsi"] = None
    try:
        m["stoch_rsi"] = ts.stoch_rsi(cfg["rq_rsi_window"], cfg["rq_srsi_window"])
    except Exception:
        m["stoch_rsi"] = None
    try:
        boll = ts.bollinger(cfg["rq_boll_window"], cfg["rq_boll_k"])
        m["boll_width_pct"] = boll[3] if boll else None
    except Exception:
        m["boll_width_pct"] = None
    try:
        m["zscore"] = ts.zscore(cfg["rq_zscore_window"])
    except Exception:
        m["zscore"] = None
    try:
        sr = ts.support_resistance(cfg["rq_window"])
        if sr:
            m["sr_support"], m["sr_resistance"], m["sr_edge_ratio"] = sr
        else:
            m["sr_support"] = m["sr_resistance"] = m["sr_edge_ratio"] = None
    except Exception:
        m["sr_support"] = m["sr_resistance"] = m["sr_edge_ratio"] = None
    try:
        m["sigma"] = ts.local_vol(cfg["vol_window"])
    except Exception:
        m["sigma"] = None
    try:
        m["mc_sigma"] = ts.local_vol(60)   # same window mc_p_win_expiryrange uses
        m["mc_mu"]    = ts.local_drift(60)
    except Exception:
        m["mc_sigma"] = m["mc_mu"] = None
    try:
        m["hurst"] = ts.hurst(cfg["hurst_window"])
    except Exception:
        m["hurst"] = None
    try:
        prices = list(ts.prices)
        if len(prices) > cfg["momentum_medium_n"]:
            sp = prices[-cfg["momentum_short_n"]-1:]
            mp = prices[-cfg["momentum_medium_n"]-1:]
            m["momentum_short_pct"] = (sp[-1]-sp[0])/abs(sp[0]) if sp[0] else None
            m["momentum_med_pct"]   = (mp[-1]-mp[0])/abs(mp[0]) if mp[0] else None
        else:
            m["momentum_short_pct"] = m["momentum_med_pct"] = None
    except Exception:
        m["momentum_short_pct"] = m["momentum_med_pct"] = None
    m["price"] = ts.current_price()
    return m


# ML feature order — MUST match train_model.py's FEATURE_ORDER exactly,
# since the exported model is just a weight-per-feature-name mapping.
ML_FEATURE_ORDER = [
    "ou_theta", "rsi", "stoch_rsi", "boll_width_pct", "zscore",
    "sr_edge_ratio", "sigma", "hurst", "momentum_short_pct",
    "momentum_med_pct", "rq_score", "layer_score", "p_win_mc",
]


def _ml_predict(cfg: dict, metrics: dict, sig: "ContractSignal") -> Optional[Tuple[float, str]]:
    """
    SHADOW-MODE ONLY. Scores this evaluation with the latest ML model
    published to Supabase (range_bot_config, key='ml_model'), using a
    dependency-free logistic regression (dot product + sigmoid — no
    sklearn needed at runtime). Returns (p_win, model_version) or None
    if no model has been published yet.

    This value is logged for comparison against outcomes. It is NEVER
    used to gate, filter, or size a trade — see the explicit comment at
    the call site in _evaluate_symbol. Promoting it to actually influence
    trading is a deliberate, separate, human-approved step later.
    """
    model = cfg.get("ml_model")
    if not model or "weights" not in model:
        return None
    try:
        weights = model["weights"]          # {feature_name: coef}
        bias    = float(model.get("bias", 0.0))
        means   = model.get("feature_means", {})
        stds    = model.get("feature_stds", {})
        version = model.get("version", "unknown")

        feats = dict(metrics)
        feats["rq_score"]    = sig.rq_score
        feats["layer_score"] = sig.layer_score
        feats["p_win_mc"]    = sig.p_win_mc

        z = bias
        for name in ML_FEATURE_ORDER:
            v = feats.get(name)
            if v is None:
                return None    # incomplete feature vector — skip rather than guess
            mean = means.get(name, 0.0)
            std  = stds.get(name, 1.0) or 1.0
            x = (float(v) - mean) / std
            z += weights.get(name, 0.0) * x
        p_win = 1.0 / (1.0 + math.exp(-z))
        return p_win, version
    except Exception as exc:
        _log("ML", f"predict failed: {exc}")
        return None


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
                          n_sims: int) -> float:
    """
    Layer 5 — Monte Carlo terminal distribution.

    Draws N terminal prices from GBM(µ, σ, T=duration_s ticks).
    Returns P(|terminal_price - entry_price| < barrier).
    Closed-form draw: terminal log-return ~ N((µ-σ²/2)T, σ²T).
    """
    sigma = ts.local_vol(60)
    mu    = ts.local_drift(60)
    price = ts.current_price()
    if not price or sigma <= 0:
        return 0.0

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
    return wins / n_sims


def evaluate_expiryrange_signal(ts: TickStore, cfg: dict) -> ContractSignal:
    """
    Runs the RANGE_QUIET regime gate first; only if it passes do layers
    1-4 run, producing a weighted score. If that score >= signal_threshold,
    Layer 5 (MC) runs. If MC p(win) >= mc_min_confidence, signal is ACTIVE.
    """
    sym     = ts.symbol
    barrier = cfg["barriers"][sym]   # no fallback — see _validate_symbol_config
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
    s2, r2 = _vol_score(ts, cfg["vol_window"], _sym_vol_skip(cfg, sym), barrier, dur)
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
    p_win = mc_p_win_expiryrange(ts, barrier, dur, cfg["mc_n_sims"])

    reasons.append(f"MC p_win={p_win:.3f} "
                   f"(min={cfg['mc_min_confidence']:.2f})")

    if p_win < cfg["mc_min_confidence"]:
        return ContractSignal("SKIP", sym, barrier, p_win, layer_score, rq_score,
                              reasons + ["MC below confidence threshold"])

    return ContractSignal("EXPIRYRANGE", sym, barrier, p_win,
                          layer_score, rq_score, reasons)


# ============================================================================
# TOUCH / NO-TOUCH — single-barrier first-passage-time strategy
# ============================================================================
#
# Unlike EXPIRYRANGE (which only cares about the price at expiry),
# ONETOUCH/NOTOUCH depend on the whole path: did price ever touch a
# single barrier before expiry, or never. This needs a genuinely
# different probability model (first-passage time under GBM, via the
# reflection principle) — not a variant of the terminal-distribution MC
# used above. The formulas below were validated against brute-force
# Monte Carlo simulation before being added here (they converge to the
# continuous-monitoring limit as simulation step size shrinks, which is
# the right comparison since Deriv settles on live tick data, not
# discrete checkpoints).
#
# Added for RDBEAR specifically: RDBEAR's real logged volatility almost
# never clears the RANGE_QUIET gate (see the earlier structural
# analysis — its Bollinger width runs ~23x wider than 1HZ10V's), so
# EXPIRYRANGE essentially never fires for it. Touch/No-Touch bet on
# that same width instead of fighting it.

def _norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def p_touch_upper(S0: float, H: float, mu: float, sigma: float, T: float) -> float:
    """P(price touches upper barrier H before time T), H > S0."""
    if H <= S0 or sigma <= 0 or T <= 0:
        return 0.0
    m  = math.log(H / S0)
    nu = mu - 0.5 * sigma**2
    sT = sigma * math.sqrt(T)
    term1 = _norm_cdf((-m + nu * T) / sT)
    term2 = math.exp(2 * nu * m / sigma**2) * _norm_cdf((-m - nu * T) / sT)
    return min(max(term1 + term2, 0.0), 1.0)


def p_touch_lower(S0: float, L: float, mu: float, sigma: float, T: float) -> float:
    """P(price touches lower barrier L before time T), L < S0."""
    if L >= S0 or sigma <= 0 or T <= 0:
        return 0.0
    m  = math.log(L / S0)   # negative
    nu = mu - 0.5 * sigma**2
    sT = sigma * math.sqrt(T)
    term1 = _norm_cdf((m - nu * T) / sT)
    term2 = math.exp(2 * nu * m / sigma**2) * _norm_cdf((m + nu * T) / sT)
    return min(max(term1 + term2, 0.0), 1.0)


def evaluate_touch_signal(ts: TickStore, cfg: dict) -> ContractSignal:
    """
    Touch strategy: bet that price WILL touch a barrier before expiry.
    Does not use the RANGE_QUIET gate (that gate looks for the opposite
    condition — calm, centered price). Fires when the modeled touch
    probability clears touch_min_confidence.

    Direction heuristic (FIRST PASS, not backtested — flagged clearly so
    it's easy to revisit once shadow data accumulates): bet on the
    barrier in the direction of recent short-term momentum. The
    reasoning is simple trend-following, not validated against outcomes
    yet the way the barrier math above was.
    """
    sym     = ts.symbol
    barrier = cfg["touch_barriers"][sym]
    dur     = cfg["duration_s"]
    reasons = []

    price = ts.current_price()
    sigma = ts.local_vol(60)
    mu    = ts.local_drift(60)
    if not price or sigma <= 0:
        return ContractSignal("SKIP", sym, barrier, reasons=["no price/vol data"])

    mom_short, mom_reason = _momentum_score(ts, cfg["momentum_short_n"], cfg["momentum_medium_n"])
    reasons.append(mom_reason)

    # crude direction pick: use raw short-term momentum sign directly
    prices = list(ts.prices)
    n = cfg["momentum_short_n"]
    direction = "up"
    if len(prices) > n:
        raw_mom = prices[-1] - prices[-n - 1]
        direction = "up" if raw_mom >= 0 else "down"
    reasons.append(f"touch direction heuristic (short momentum) -> {direction}")

    if direction == "up":
        p_win = p_touch_upper(price, price + barrier, mu, sigma, dur)
    else:
        p_win = p_touch_lower(price, price - barrier, mu, sigma, dur)

    reasons.append(f"p_touch={p_win:.3f} (min={cfg['touch_min_confidence']:.2f}) "
                   f"barrier={barrier:.2f} direction={direction}")

    if p_win < cfg["touch_min_confidence"]:
        return ContractSignal("SKIP", sym, barrier, p_win, 0.0, 0.0,
                              reasons + ["below touch_min_confidence"], direction)

    return ContractSignal("ONETOUCH", sym, barrier, p_win, 0.0, 0.0,
                          reasons, direction)


def evaluate_notouch_signal(ts: TickStore, cfg: dict) -> ContractSignal:
    """
    No-Touch strategy: bet that price will NOT touch a (distant) barrier
    before expiry. Direction heuristic: pick the side OPPOSITE recent
    momentum (the side price is less likely to reach) — same "first
    pass, not backtested" caveat as evaluate_touch_signal.
    """
    sym     = ts.symbol
    barrier = cfg["notouch_barriers"][sym]
    dur     = cfg["duration_s"]
    reasons = []

    price = ts.current_price()
    sigma = ts.local_vol(60)
    mu    = ts.local_drift(60)
    if not price or sigma <= 0:
        return ContractSignal("SKIP", sym, barrier, reasons=["no price/vol data"])

    prices = list(ts.prices)
    n = cfg["momentum_short_n"]
    direction = "up"   # which barrier we're betting WON'T be touched
    if len(prices) > n:
        raw_mom = prices[-1] - prices[-n - 1]
        # opposite of momentum: if trending up, the upper barrier is MORE
        # likely to be touched (bad for notouch) — so bet on the lower one.
        direction = "down" if raw_mom >= 0 else "up"
    reasons.append(f"notouch direction heuristic (opposite momentum) -> {direction}")

    if direction == "up":
        p_touch = p_touch_upper(price, price + barrier, mu, sigma, dur)
    else:
        p_touch = p_touch_lower(price, price - barrier, mu, sigma, dur)
    p_win = 1.0 - p_touch

    reasons.append(f"p_notouch={p_win:.3f} (min={cfg['notouch_min_confidence']:.2f}) "
                   f"barrier={barrier:.2f} direction={direction}")

    if p_win < cfg["notouch_min_confidence"]:
        return ContractSignal("SKIP", sym, barrier, p_win, 0.0, 0.0,
                              reasons + ["below notouch_min_confidence"], direction)

    return ContractSignal("NOTOUCH", sym, barrier, p_win, 0.0, 0.0,
                          reasons, direction)


def evaluate_signal(ts: TickStore, cfg: dict, strategy: str) -> ContractSignal:
    """Dispatcher — routes to the strategy-specific evaluator. Every
    strategy for every symbol goes through here so call sites never need
    to know which strategy a symbol is running."""
    if strategy == "expiryrange":
        return evaluate_expiryrange_signal(ts, cfg)
    elif strategy == "touch":
        return evaluate_touch_signal(ts, cfg)
    elif strategy == "notouch":
        return evaluate_notouch_signal(ts, cfg)
    else:
        raise ValueError(f"Unknown strategy: {strategy}")




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

    def _insert(self, table: str, row: dict):
        """Plain insert (no merge-duplicates) — used for append-only log
        tables (range_signals, range_trades) where every row is a new
        event, not a keyed record to overwrite."""
        if not self.ok:
            return
        headers = dict(self._headers)
        headers["Prefer"] = "return=minimal"
        try:
            requests.post(f"{self.url}/rest/v1/{table}",
                          headers=headers, json=row, timeout=10)
        except Exception as exc:
            _log("STORE", f"Insert into {table} failed: {exc}")

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

    def load_remote_config(self) -> dict:
        """
        Fetch every row from range_bot_config — this is the mechanism
        that replaces 'edit a Railway env var and redeploy.' Anything
        tunable (thresholds, the ML model) lives here instead, and both
        the bot and train_model.py read/write it directly via Supabase.
        The bot polls this periodically (see RangeBot._remote_config_loop)
        so changes take effect within ~15 minutes, no redeploy required.

        Returns {} on any failure — callers must treat that as "no
        change," never as "reset to defaults."
        """
        if not self.ok:
            return {}
        try:
            resp = requests.get(
                f"{self.url}/rest/v1/range_bot_config?select=*",
                headers=self._headers, timeout=10)
            if resp.status_code != 200:
                return {}
            out = {}
            for row in resp.json():
                out[row["key"]] = row.get("value")
            return out
        except Exception as exc:
            _log("STORE", f"load_remote_config failed: {exc}")
            return {}

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

    def save_signal(self, row: dict):
        """Log every evaluation — fired or skipped — to range_signals.
        This is what makes gate/threshold tuning possible: without the
        skipped rows you can only ever see the trades that already
        passed your current thresholds, never how close/far the misses
        were."""
        self._insert("range_signals", row)

    def save_trade(self, row: dict):
        """Log one closed trade (entry snapshot + outcome + price path)
        to range_trades."""
        self._insert("range_trades", row)


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
        Proposal + buy at fixed barrier.
        EXPIRYRANGE: symmetric ± range, needs barrier + barrier2.
        ONETOUCH/NOTOUCH: single signed barrier, direction determines sign —
        Deriv accepts signed-relative barriers for synthetic indices even
        under 24h duration (confirmed against API docs before this was
        added — most other symbols need absolute barriers under 24h, but
        volatility indices are the documented exception).
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
        }
        if contract_type == "EXPIRYRANGE":
            req["barrier"]  = f"+{barrier:.2f}"
            req["barrier2"] = f"-{barrier:.2f}"
        elif contract_type in ("ONETOUCH", "NOTOUCH"):
            sign = "+" if sig.direction == "up" else "-"
            req["barrier"] = f"{sign}{barrier:.2f}"
        else:
            _log("PROPOSAL", f"Unknown contract_type: {contract_type}")
            return None

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
        barrier_desc = (f"±{barrier:.2f}" if contract_type == "EXPIRYRANGE"
                        else f"{req['barrier']}")
        _log("PROPOSAL",
             f"{contract_type} {sig.symbol} {barrier_desc} "
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
    symbol:           str        # underlying market symbol, e.g. "RDBEAR"
    strategy:         str        # "expiryrange" | "touch" | "notouch"
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
    # ── Trade analytics (per open trade) ────────────────────────────
    entry_price:      Optional[float] = None
    entry_metrics:    Optional[dict]  = None   # snapshot from _metrics_snapshot at signal time
    entry_ts_utc:     Optional[str]   = None
    entry_martingale_step: int        = 0
    price_path:       List[Tuple[float, float]] = field(default_factory=list)  # (t_offset_s, price)

    @property
    def key(self) -> str:
        """Composite state key, e.g. 'RDBEAR:touch'. Two strategies on the
        same symbol run fully independently — separate lock, separate
        stake/martingale state, separate open trade — they just happen to
        share the same incoming tick feed (see RangeBot.symbol_to_keys)."""
        return f"{self.symbol}:{self.strategy}"


# ============================================================================
# MAIN BOT
# ============================================================================

class RangeBot:
    def __init__(self, cfg: dict = CONFIG):
        self.cfg    = cfg
        self.client = DerivClient(cfg)
        self.store  = PersistenceStore(cfg)
        self.session_id: str = str(uuid.uuid4())   # groups all rows from this run

        # cfg["symbols"] is {symbol: [strategy, ...]}. self.symbols is the
        # underlying market symbols (for tick subscription); self.states is
        # keyed by "symbol:strategy" so each strategy gets its own lock,
        # stake/martingale tracking, and open-trade state, even when two
        # strategies share a symbol (e.g. RDBEAR touch + notouch running
        # concurrently, each free to fire independently on its own barrier).
        self.symbols: List[str] = list(cfg["symbols"].keys())
        self.states:  Dict[str, SymbolState] = {}
        self.symbol_to_keys: Dict[str, List[str]] = {}
        for sym, strategies in cfg["symbols"].items():
            self.symbol_to_keys[sym] = []
            for strat in strategies:
                st = SymbolState(symbol=sym, strategy=strat,
                                  engine=SignalEngine(sym, cfg))
                self.states[st.key] = st
                self.symbol_to_keys[sym].append(st.key)

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

        # Strategy-appropriate signal evaluation — RANGE_QUIET+5-layer for
        # expiryrange, first-passage-time model for touch/notouch.
        sig     = evaluate_signal(ts, self.cfg, st.strategy)
        metrics = _metrics_snapshot(ts, self.cfg)
        chosen: Optional[ContractSignal] = sig if sig.contract_type != "SKIP" else None

        # Always print the signal block
        print(f"\n{'='*60}")
        print(f"SIGNAL  {sym} [{st.strategy}]  {_ts()}")
        print(f"  rq_score={sig.rq_score:.3f}  p_win_mc={sig.p_win_mc:.3f}")
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
                  f"{'dir='+chosen.direction+' ' if chosen.direction else ''}"
                  f"p_win={chosen.p_win_mc:.3f} "
                  f"layer_score={chosen.layer_score:.3f} "
                  f"stake=${stake:.2f}{mg_tag}")
        print(f"{'='*60}")

        # SHADOW-MODE ML scoring — logged for comparison only, never used
        # to gate/filter/size this or any trade. See _ml_predict docstring.
        # Only meaningful for expiryrange today — the model was trained on
        # expiryrange trade features. Skipped for touch/notouch until/if a
        # strategy-specific model is trained.
        if st.strategy == "expiryrange":
            ml_result = _ml_predict(self.cfg, metrics, sig)
            ml_p_win, ml_version = ml_result if ml_result else (None, None)
        else:
            ml_p_win, ml_version = None, None

        # Log EVERY evaluation — fired or skipped — for gate/threshold tuning.
        self.store.save_signal({
            "session_id":    self.session_id,
            "symbol":        sym,
            "strategy":      st.strategy,
            "ts":            datetime.utcnow().isoformat(),
            "fired":         bool(chosen),
            "contract_type": sig.contract_type,
            "direction":     sig.direction,
            "rq_score":      sig.rq_score,
            "layer_score":   sig.layer_score,
            "p_win_mc":      sig.p_win_mc,
            "barrier":       sig.barrier,
            "vol_skip_thresh_used": _sym_vol_skip(self.cfg, sym),
            "rq_threshold_used":    self.cfg["rq_threshold"],
            "signal_threshold_used": self.cfg["signal_threshold"],
            "mc_min_confidence_used": self.cfg["mc_min_confidence"],
            "ml_p_win":         ml_p_win,     # SHADOW — not used for `chosen` above
            "ml_model_version": ml_version,
            **metrics,
            "reasons": " | ".join(sig.reasons),
        })

        if not chosen:
            return

        # Convert to TradeSignal for place_trade
        stake = self._current_stake(st)
        trade_sig = TradeSignal(
            contract_type = chosen.contract_type,
            symbol        = sym,
            barrier       = chosen.barrier,
            direction     = chosen.direction,
            p_win_mc      = chosen.p_win_mc,
            layer_score   = chosen.layer_score,
            rq_score      = chosen.rq_score,
            stake         = stake,
            reasons       = chosen.reasons,
        )

        # Snap balance before trade
        bal = await self.client.fetch_balance()
        if bal is not None:
            self.balance      = bal
            st.balance_before = bal

        # Place trade
        contract_id = await self.client.place_trade(trade_sig)
        if contract_id:
            st.waiting        = True
            st.contract_id    = contract_id
            st.current_sig    = trade_sig
            st.lock_since     = time.monotonic()
            st.entry_price    = metrics.get("price")
            st.entry_metrics  = metrics
            st.entry_ts_utc   = datetime.utcnow().isoformat()
            st.entry_martingale_step = st.martingale_step
            st.price_path     = [(0.0, metrics.get("price"))] if metrics.get("price") else []
            _log("LOCK", f"{sym}[{st.strategy}] waiting on {contract_id}")
        else:
            st.balance_before = None

    # ── Settlement ────────────────────────────────────────────────────────────

    async def _handle_settlement(self, key: str, data: dict):
        st = self.states.get(key)
        if st is None or not st.waiting:
            return
        sym = st.symbol
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
                 f"{sym}[{st.strategy}] pre=${st.balance_before:.2f} → post=${bal_after:.2f} "
                 f"| actual={actual:+.2f} | api={api_profit:+.2f}")
        else:
            actual = api_profit

        print(f"\n{'='*60}")
        print(f"RESULT  {sym}[{st.strategy}]  contract={cid}")
        print(f"        profit={actual:+.2f}")
        print(f"{'='*60}")

        if actual > 0:
            st.engine.wins += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses = 0
            if st.martingale_step > 0:
                _log("MARTINGALE", f"{sym}[{st.strategy}] win at step {st.martingale_step} → reset to base stake")
            st.martingale_step = 0
            _log("WIN", f"{sym}[{st.strategy}] +${actual:.2f} | "
                        f"session P&L ${self.total_profit:+.2f}")
        else:
            st.engine.losses += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses += 1
            _log("LOSS", f"{sym}[{st.strategy}] ${actual:.2f} | "
                         f"session P&L ${self.total_profit:+.2f} | "
                         f"streak={st.consec_losses}")

            if self.cfg.get("martingale_enabled", False):
                max_steps = self.cfg["martingale_max_steps"]
                if st.martingale_step >= max_steps:
                    _log("MARTINGALE",
                         f"{sym}[{st.strategy}] max steps ({max_steps}) reached without a win → "
                         f"reset to base stake")
                    st.martingale_step = 0
                else:
                    st.martingale_step += 1
                    next_stake = self._current_stake(st)
                    _log("MARTINGALE",
                         f"{sym}[{st.strategy}] step → {st.martingale_step}/{max_steps} "
                         f"(next stake ${next_stake:.2f})")

            limit = self.cfg["consec_loss_limit"]
            if st.consec_losses >= limit:
                pause = self.cfg["consec_pause_secs"]
                st.cb_paused_until = time.monotonic() + pause
                st.consec_losses   = 0
                _log("BREAKER",
                     f"{sym}[{st.strategy}] {limit} consecutive losses → pausing {pause}s")

        if bal_after is not None:
            self.balance = bal_after

        # Persist the full trade record: signal features at entry + the
        # actual tick-by-tick price path, so it can be compared offline
        # against the MC-modelled distribution that justified the trade.
        exit_price = st.engine.ts.current_price()
        em = st.entry_metrics or {}
        self.store.save_trade({
            "session_id":       self.session_id,
            "symbol":           sym,
            "strategy":         st.strategy,
            "contract_id":      cid,
            "opened_at":        st.entry_ts_utc,
            "closed_at":        datetime.utcnow().isoformat(),
            "outcome":          "win" if actual > 0 else "loss",
            "profit":           actual,
            "stake":            st.current_sig.stake if st.current_sig else None,
            "martingale_step":  st.entry_martingale_step,
            "barrier":          st.current_sig.barrier if st.current_sig else None,
            "direction":        st.current_sig.direction if st.current_sig else None,
            "duration_s":       self.cfg["duration_s"],
            "entry_price":      st.entry_price,
            "exit_price":       exit_price,
            "rq_score":         st.current_sig.rq_score if st.current_sig else None,
            "layer_score":      st.current_sig.layer_score if st.current_sig else None,
            "p_win_mc":         st.current_sig.p_win_mc if st.current_sig else None,
            "mc_mu":            em.get("mc_mu"),
            "mc_sigma":         em.get("mc_sigma"),
            "price_path":       json.dumps(st.price_path),
            # Raw indicator snapshot at entry — makes this table
            # self-contained for train_model.py (no join against
            # range_signals needed, and no risk of matching the wrong row).
            "ou_theta":            em.get("ou_theta"),
            "rsi":                 em.get("rsi"),
            "stoch_rsi":           em.get("stoch_rsi"),
            "boll_width_pct":      em.get("boll_width_pct"),
            "zscore":              em.get("zscore"),
            "sr_edge_ratio":       em.get("sr_edge_ratio"),
            "sigma":               em.get("sigma"),
            "hurst":               em.get("hurst"),
            "momentum_short_pct":  em.get("momentum_short_pct"),
            "momentum_med_pct":    em.get("momentum_med_pct"),
        })

        st.waiting      = False
        st.contract_id  = None
        st.current_sig  = None
        st.lock_since   = None
        st.balance_before  = None
        st.entry_price     = None
        st.entry_metrics   = None
        st.entry_ts_utc    = None
        st.entry_martingale_step = 0
        st.price_path      = []

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

    async def _apply_remote_config(self):
        """
        Pull range_bot_config from Supabase and merge it into self.cfg.
        This is the replacement for 'edit a Railway env var and redeploy':
        anything in range_bot_config takes effect within one poll cycle,
        no deploy needed.

        Two tiers, deliberately separated:
          - 'ml_model' is ALWAYS synced — it only affects the shadow-mode
            ml_p_win column that gets logged, never live trading, so
            there's no real-money risk in auto-picking up a new version.
          - Everything else (thresholds that actually gate real trades)
            only syncs if cfg['remote_config_enabled'] is explicitly True.
            Default is False: until you've decided you trust this pipeline,
            thresholds stay exactly where you set them in CONFIG/env vars.
        """
        remote = self.store.load_remote_config()
        if not remote:
            return

        if "ml_model" in remote:
            new_version = (remote["ml_model"] or {}).get("version")
            old_version = (self.cfg.get("ml_model") or {}).get("version")
            if new_version and new_version != old_version:
                self.cfg["ml_model"] = remote["ml_model"]
                _log("CONFIG", f"ML shadow model updated: "
                                f"{old_version} -> {new_version}")

        if not self.cfg.get("remote_config_enabled", False):
            return

        tunable = ("rq_threshold", "signal_threshold", "mc_min_confidence",
                   "vol_skip_thresh", "barriers")
        for key in tunable:
            if key in remote and remote[key] is not None and remote[key] != self.cfg.get(key):
                _log("CONFIG", f"Remote update: {key} "
                                f"{self.cfg.get(key)} -> {remote[key]}")
                self.cfg[key] = remote[key]

    async def _remote_config_loop(self):
        """Background poll — see _apply_remote_config for what it does
        and the safety split between ML (always synced) and live-trading
        thresholds (opt-in via remote_config_enabled)."""
        interval = self.cfg.get("remote_config_poll_s", 900)   # 15 min default
        while not self._stop:
            try:
                await self._apply_remote_config()
            except Exception as exc:
                _log("CONFIG", f"remote config poll failed: {exc}")
            await asyncio.sleep(interval)

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

        problems = _validate_symbol_config(self.cfg)
        if problems:
            for p in problems:
                _log("ERROR", p)
            _log("ERROR", "Refusing to start with unconfigured symbol(s) — "
                           "fix cfg['barriers'] above and restart.")
            return

        if not await self.client.connect():
            return

        self.balance           = self.client.initial_balance
        self.session_start_bal = self.client.initial_balance

        # Warm-start persisted stats — keyed by "symbol:strategy" so each
        # strategy's win/loss history is tracked separately even when two
        # strategies share a symbol (e.g. RDBEAR touch vs notouch).
        for key, st in self.states.items():
            self.store.load_symbol_stats(key, st.engine)

        # Subscribe to all symbols
        for sym in self.symbols:
            if not await self.client.subscribe_ticks(sym):
                _log("ERROR", f"Could not subscribe to {sym} — aborting")
                return

        _log("BOT", f"Live — warming up ({self.cfg['min_ticks']} ticks needed per symbol)...")

        # Pick up any ML model / (opt-in) tuned thresholds already
        # published to Supabase before we start evaluating.
        await self._apply_remote_config()

        console_task = asyncio.create_task(self._console(), name="console")
        remote_cfg_task = asyncio.create_task(self._remote_config_loop(), name="remote_config")

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

                # Route ticks to every state watching this market symbol —
                # RDBEAR touch and notouch both get every RDBEAR tick, even
                # though they're independent trade states.
                if "tick" in response:
                    tick = response["tick"]
                    sym  = tick.get("symbol") or tick.get("instrument_id", "")
                    price = float(tick.get("quote", 0))
                    for key in self.symbol_to_keys.get(sym, []):
                        st = self.states[key]
                        self._check_lock_timeout(st)
                        st.engine.add_tick(price)

                        # While a trade is open, record the actual price path
                        # so it can be compared against the MC-modelled path
                        # that justified the trade (see analyze_trades.py).
                        if st.waiting and st.lock_since is not None:
                            t_offset = time.monotonic() - st.lock_since
                            st.price_path.append((round(t_offset, 2), price))

                        # Evaluate (each strategy independently, own lock)
                        if st.engine.is_ready() and not st.waiting:
                            await self._evaluate_symbol(st)

                    # Periodic persist — once per tick event, not per state
                    now = time.monotonic()
                    if (self.symbol_to_keys.get(sym) and self.store.ok and
                            now - self._last_persist >= self.cfg["persist_every_secs"]):
                        self._last_persist = now
                        for s, sx in self.states.items():
                            self.store.save_symbol_stats(s, sx.engine)

                # Settlement
                if "proposal_open_contract" in response:
                    poc = response["proposal_open_contract"]
                    cid = str(poc.get("contract_id", ""))
                    for st in self.states.values():
                        if st.contract_id == cid:
                            await self._handle_settlement(st.key, poc)
                            break

                if "transaction" in response:
                    tx  = response["transaction"]
                    cid = str(tx.get("contract_id", ""))
                    if cid:
                        for st in self.states.values():
                            if st.contract_id == cid:
                                await self._handle_settlement(st.key, {
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
            remote_cfg_task.cancel()
            for key, st in self.states.items():
                self.store.save_symbol_stats(key, st.engine)
            if self.store.ok:
                _log("STORE", "Final stats saved.")
            await self.client.close()
            print(f"\n{'='*60}")
            print("  FINAL SESSION RESULTS")
            print(f"{'='*60}")
            for key, st in self.states.items():
                total = st.engine.wins + st.engine.losses
                wr    = st.engine.wins / total * 100 if total else 0
                print(f"  {key}: {total} trades | "
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
                    for key, st in self.states.items():
                        eng   = st.engine
                        sym   = st.symbol
                        total = eng.wins + eng.losses
                        wr    = eng.wins / total * 100 if total else 0
                        sigma   = eng.ts.local_vol(self.cfg["vol_window"])
                        hurst   = eng.ts.hurst(self.cfg["hurst_window"])
                        print(f"\n  {key}: W:{eng.wins} L:{eng.losses} "
                              f"WR:{wr:.1f}% P&L:${eng.total_profit:+.2f}")
                        vol_skip = _sym_vol_skip(self.cfg, sym)
                        print(f"    σ={sigma:.6f}  vol_skip={vol_skip:.6f}  "
                              f"H={hurst:.3f}  ticks={eng.ts.count}")
                        if st.strategy == "expiryrange":
                            barrier = self.cfg["barriers"].get(sym, "UNCONFIGURED")
                            rq_pass, rq_score, _ = range_quiet_gate(eng.ts, self.cfg)
                            print(f"    barrier ER=±{barrier}  "
                                  f"RANGE_QUIET={'PASS' if rq_pass else 'fail'} "
                                  f"score={rq_score:.3f}")
                        elif st.strategy == "touch":
                            barrier = self.cfg["touch_barriers"].get(sym, "UNCONFIGURED")
                            print(f"    barrier=±{barrier}  (single-sided touch)")
                        elif st.strategy == "notouch":
                            barrier = self.cfg["notouch_barriers"].get(sym, "UNCONFIGURED")
                            print(f"    barrier=±{barrier}  (single-sided no-touch)")
                        print(f"    waiting={st.waiting}  "
                              f"cb_paused={st.cb_paused_until > time.monotonic()}  "
                              f"martingale_step={st.martingale_step}/{self.cfg['martingale_max_steps']}  "
                              f"next_stake=${self._current_stake(st):.2f}")
                    print(f"\n  Session P&L: ${self.total_profit:+.2f}  "
                          f"Balance: ${self.balance:.2f}")
                elif cmd.startswith("u "):
                    target = cmd[2:].strip().upper()
                    # Accept either an exact "SYMBOL:strategy" key or a bare
                    # symbol (unlocks every strategy running on it).
                    matches = ([target] if target in self.states
                               else [k for k in self.states if k.upper().startswith(target + ":")])
                    if matches:
                        for key in matches:
                            st = self.states[key]
                            st.waiting = False
                            st.contract_id = None
                            st.current_sig = None
                            st.lock_since  = None
                            _log("CMD", f"Unlocked {key}")
                    else:
                        _log("CMD", f"Unknown symbol/key: {target}")
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
#
# -- Every evaluation cycle (fired AND skipped). This is the table that
# -- makes threshold tuning possible — without the skipped rows you can
# -- only see the trades that already passed today's thresholds, never
# -- how close the misses were.
# CREATE TABLE IF NOT EXISTS range_signals (
#     id                        BIGSERIAL PRIMARY KEY,
#     session_id                UUID NOT NULL,
#     symbol                    TEXT NOT NULL,
#     ts                        TIMESTAMPTZ NOT NULL DEFAULT now(),
#     fired                     BOOLEAN NOT NULL,
#     contract_type             TEXT,
#     rq_score                  DOUBLE PRECISION,
#     layer_score               DOUBLE PRECISION,
#     p_win_mc                  DOUBLE PRECISION,
#     barrier                   DOUBLE PRECISION,
#     vol_skip_thresh_used      DOUBLE PRECISION,
#     rq_threshold_used         DOUBLE PRECISION,
#     signal_threshold_used     DOUBLE PRECISION,
#     mc_min_confidence_used    DOUBLE PRECISION,
#     ou_theta                  DOUBLE PRECISION,
#     rsi                       DOUBLE PRECISION,
#     stoch_rsi                 DOUBLE PRECISION,
#     boll_width_pct            DOUBLE PRECISION,
#     zscore                    DOUBLE PRECISION,
#     sr_support                DOUBLE PRECISION,
#     sr_resistance             DOUBLE PRECISION,
#     sr_edge_ratio             DOUBLE PRECISION,
#     sigma                     DOUBLE PRECISION,
#     mc_sigma                  DOUBLE PRECISION,
#     mc_mu                     DOUBLE PRECISION,
#     hurst                     DOUBLE PRECISION,
#     momentum_short_pct        DOUBLE PRECISION,
#     momentum_med_pct          DOUBLE PRECISION,
#     price                     DOUBLE PRECISION,
#     reasons                   TEXT
# );
# CREATE INDEX IF NOT EXISTS range_signals_symbol_ts_idx
#     ON range_signals (symbol, ts);
# CREATE INDEX IF NOT EXISTS range_signals_fired_idx
#     ON range_signals (fired);
#
# -- One row per CLOSED trade: entry snapshot + outcome + the actual
# -- tick-by-tick price path, so it can be compared offline against the
# -- MC-modelled distribution (mc_mu/mc_sigma/entry_price/duration_s)
# -- that justified the trade. See analyze_trades.py.
# CREATE TABLE IF NOT EXISTS range_trades (
#     id                BIGSERIAL PRIMARY KEY,
#     session_id        UUID NOT NULL,
#     symbol            TEXT NOT NULL,
#     contract_id       TEXT NOT NULL,
#     opened_at         TIMESTAMPTZ,
#     closed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
#     outcome           TEXT NOT NULL,           -- 'win' | 'loss'
#     profit            DOUBLE PRECISION NOT NULL,
#     stake             DOUBLE PRECISION,
#     martingale_step   INTEGER,
#     barrier           DOUBLE PRECISION,
#     duration_s        INTEGER,
#     entry_price       DOUBLE PRECISION,
#     exit_price        DOUBLE PRECISION,
#     rq_score          DOUBLE PRECISION,
#     layer_score       DOUBLE PRECISION,
#     p_win_mc          DOUBLE PRECISION,
#     mc_mu             DOUBLE PRECISION,
#     mc_sigma          DOUBLE PRECISION,
#     price_path        JSONB              -- [[t_offset_seconds, price], ...]
# );
# CREATE INDEX IF NOT EXISTS range_trades_symbol_idx ON range_trades (symbol);
# CREATE INDEX IF NOT EXISTS range_trades_contract_id_idx ON range_trades (contract_id);
#
# -- Every closed trade's raw indicator snapshot at entry, so this table
# -- is self-contained for train_model.py — no need to join range_signals.
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS ou_theta             DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS rsi                  DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS stoch_rsi            DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS boll_width_pct       DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS zscore               DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS sr_edge_ratio        DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS sigma                DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS hurst                DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS momentum_short_pct   DOUBLE PRECISION;
# ALTER TABLE range_trades ADD COLUMN IF NOT EXISTS momentum_med_pct     DOUBLE PRECISION;
#
# -- Shadow-mode ML prediction, logged alongside every evaluation (fired or
# -- skipped) for later comparison against outcomes. Never used to gate a
# -- trade — see _ml_predict in bot.py.
# ALTER TABLE range_signals ADD COLUMN IF NOT EXISTS ml_p_win            DOUBLE PRECISION;
# ALTER TABLE range_signals ADD COLUMN IF NOT EXISTS ml_model_version    TEXT;
#
# -- Remote-tunable config — replaces "edit a Railway env var and redeploy."
# -- The bot polls this table every remote_config_poll_s seconds (default
# -- 15 min). key='ml_model' is always synced (shadow-mode only, zero
# -- trading risk). Any other key only takes effect if remote_config_enabled
# -- is explicitly True — see RangeBot._apply_remote_config.
# CREATE TABLE IF NOT EXISTS range_bot_config (
#     key          TEXT PRIMARY KEY,
#     value        JSONB NOT NULL,
#     updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
# );
#
# -- Multi-strategy support: each symbol can run several strategies
# -- concurrently (e.g. RDBEAR touch + notouch), each firing independently.
# ALTER TABLE range_signals ADD COLUMN IF NOT EXISTS strategy  TEXT;
# ALTER TABLE range_signals ADD COLUMN IF NOT EXISTS direction TEXT;   -- 'up' | 'down', touch/notouch only
# ALTER TABLE range_trades  ADD COLUMN IF NOT EXISTS strategy  TEXT;
# ALTER TABLE range_trades  ADD COLUMN IF NOT EXISTS direction TEXT;
#
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
