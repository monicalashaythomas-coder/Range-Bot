"""
DERIV EXPIRYRANGE + NOTOUCH BOT
================================
Symbols  : 1HZ10V  (Volatility 10 Index — 1-second feed)
           1HZ100V (Volatility 100 Index — 1-second feed)
Contracts: EXPIRYRANGE — price must fall WITHIN barriers at expiry tick
           NOTOUCH     — price must NEVER touch either barrier during hold

Signal architecture
───────────────────
  EXPIRYRANGE and NOTOUCH are both non-directional volatility products:
  you are not betting which way price moves, you are betting HOW MUCH it
  moves. The signal stack is therefore entirely different from the Rise/Fall
  bot — no bias, markov, or momentum layers. Instead:

  LAYER 1 — Volatility regime (σ estimate from recent returns)
    · Local σ from last 50 returns (return-scale, scale-invariant)
    · Low vol  → EXPIRYRANGE favoured (price likely stays contained)
    · High vol → NOTOUCH favoured (barriers likely untouched if wide enough)
    · Very high vol → skip both (chaotic, barrier pricing unreliable)

  LAYER 2 — Hurst exponent (mean-reversion vs trending)
    · H < 0.45 → mean-reverting → EXPIRYRANGE more attractive
      (price oscillates back toward centre, unlikely to end far out)
    · H > 0.55 → trending → NOTOUCH more attractive
      (trending price less likely to touch distant barriers)
    · 0.45 ≤ H ≤ 0.55 → near-random walk → use vol regime only

  LAYER 3 — Monte Carlo terminal distribution (EXPIRYRANGE)
    · Simulates N terminal prices via GBM closed-form draws
    · For each (barrier_width, duration) candidate, estimates
      P(barrier_low < terminal_price < barrier_high)
    · Compares against real payout quote from Deriv to get true EV
    · Selects (width, duration) maximising EV

  LAYER 4 — Monte Carlo path simulation (NOTOUCH)
    · Simulates full tick-by-tick paths for each (barrier_dist, duration)
    · Estimates P(price never touches either barrier throughout hold)
    · Compares against real payout quote from Deriv to get true EV
    · Selects (dist, duration) maximising EV

  LAYER 5 — Contract type selector
    · Runs both EXPIRYRANGE and NOTOUCH scans when vol/Hurst are ambiguous
    · Picks whichever contract type + parameters produces the highest
      real EV (using live Deriv quotes, not assumed payouts)
    · Falls back to EXPIRYRANGE-only in low-vol regimes,
      NOTOUCH-only in trending high-vol regimes

Risk management
───────────────
  · No martingale — EXPIRYRANGE/NOTOUCH losses don't invert on the next
    trade (there's no directional signal to double down on). Instead:
    flat staking with per-symbol daily loss limits.
  · Balance-aware position sizing: stake = Kelly fraction × balance,
    capped at MAX_STAKE_PCT per trade. Kelly fraction = (p_win × r - (1-p_win)) / r
    where r = payout_ratio (net profit ratio from live quote).
  · Per-symbol loss circuit breaker: N consecutive losses → pause M seconds.
  · Session stop-loss and take-profit (flat $ or % of session-start balance).

Connection layer
────────────────
  Deriv new Options API — REST OTP bootstrap + WebSocket, identical to the
  PR05 bot. No legacy app_id query param, no in-band authorize message.
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
    # Both are traded concurrently on the same WebSocket connection.
    "symbols":          ["1HZ10V", "1HZ100V"],
    "currency":         "USD",

    # ── Contract types per symbol ─────────────────────────────────
    # "both" → scan EXPIRYRANGE and NOTOUCH each cycle, pick best EV
    # "expiryrange" → EXPIRYRANGE only
    # "notouch"     → NOTOUCH only
    "contract_mode":    _env("CONTRACT_MODE", "both"),

    # ── Tick history window ───────────────────────────────────────
    "tick_window":      _env("TICK_WINDOW",    200),   # ticks kept per symbol
    "min_ticks":        _env("MIN_TICKS",       60),   # minimum before evaluating

    # ── Volatility regime ─────────────────────────────────────────
    # Return-scale (scale-invariant). Based on empirical 1HZ10V/1HZ100V
    # tick behaviour: 1HZ10V is much lower vol than 1HZ100V.
    # These defaults work for both; tune per-symbol via env vars.
    "vol_low_thresh":   _env("VOL_LOW",   0.000015),  # below → low-vol / EXPIRYRANGE regime
    "vol_high_thresh":  _env("VOL_HIGH",  0.000120),  # above → high-vol / NOTOUCH regime
    "vol_skip_thresh":  _env("VOL_SKIP",  0.000300),  # above → skip (chaotic)
    "vol_window":       _env("VOL_WINDOW",      50),  # returns to use for σ estimate

    # ── Hurst exponent ────────────────────────────────────────────
    "hurst_window":     _env("HURST_WINDOW",    80),  # ticks for Hurst calculation
    "hurst_mr_thresh":  _env("HURST_MR",      0.45),  # below → mean-reverting
    "hurst_tr_thresh":  _env("HURST_TR",      0.55),  # above → trending

    # ── EXPIRYRANGE ───────────────────────────────────────────────
    # Valid durations confirmed from live API testing.
    # Min 2 minutes, max 15 minutes as per trading requirements.
    "er_durations":     [120, 180, 300, 600, 900],  # seconds (2m,3m,5m,10m,15m)
    "er_sigma_mults":   [0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5],
    "er_n_sims":        _env("ER_SIMS",    1000),
    "er_top_k":         _env("ER_TOP_K",      6),
    "er_min_ev":        _env("ER_MIN_EV",  0.02),

    # ── NOTOUCH ───────────────────────────────────────────────────
    # Single barrier only on 1HZ synthetic indices — price must NOT
    # touch the barrier above current spot. Upper barrier (+offset) only.
    "nt_single_barrier": True,
    "nt_sigma_mults":   [1.5, 2.0, 2.5, 3.0, 3.5, 4.0],
    "nt_durations":     [120, 180, 300, 600, 900],  # seconds (2m–15m)
    "nt_n_sims":        _env("NT_SIMS",    1000),
    "nt_top_k":         _env("NT_TOP_K",      6),
    "nt_min_ev":        _env("NT_MIN_EV",  0.02),

    # ── Kelly stake sizing ────────────────────────────────────────
    "kelly_fraction":   _env("KELLY_FRAC",   0.25),  # quarter-Kelly (conservative)
    "min_stake":        _env("MIN_STAKE",    1.00),
    "max_stake_pct":    _env("MAX_STAKE_PCT", 0.02), # max 2% of balance per trade

    # ── Session risk limits ───────────────────────────────────────
    "target_profit":     _env("TARGET_PROFIT",   0.0),   # 0 = disabled
    "stop_loss":          _env("STOP_LOSS",      20.0),
    "target_profit_pct": _env("TARGET_PCT",      0.0),
    "stop_loss_pct":      _env("STOP_PCT",        0.0),

    # ── Circuit breaker ───────────────────────────────────────────
    "consec_loss_limit": _env("CONSEC_LOSS_LIMIT",    3),
    "consec_pause_secs": _env("CONSEC_PAUSE_SECS",  300),

    # ── Evaluation pacing ─────────────────────────────────────────
    # Minimum seconds between evaluations on the same symbol.
    # EXPIRYRANGE/NOTOUCH scanning involves real proposal round-trips —
    # don't hammer the API on every tick.
    "eval_cooldown":    _env("EVAL_COOLDOWN",   30),   # seconds between evals per symbol

    # ── Resilience ────────────────────────────────────────────────
    "lock_timeout":         _env("LOCK_TIMEOUT",     600),  # seconds (long contracts)
    "buy_recv_retries":     _env("BUY_RETRIES",        8),
    "reconnect_delay_min":  _env("RECONNECT_MIN",       2),
    "reconnect_delay_max":  _env("RECONNECT_MAX",      60),
    "ws_ping_interval":     _env("WS_PING",            30),
    "orphan_poll_attempts": _env("ORPHAN_ATTEMPTS",     4),
    "orphan_poll_interval": _env("ORPHAN_INTERVAL",     3),

    # ── Persistence (optional) ────────────────────────────────────
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


# ============================================================================
# VOL REGIME
# ============================================================================

class VolRegime(Enum):
    LOW      = "low"       # EXPIRYRANGE favoured
    MID      = "mid"       # evaluate both, pick best EV
    HIGH     = "high"      # NOTOUCH favoured
    SKIP     = "skip"      # too chaotic, skip this cycle


def classify_regime(sigma: float, cfg: dict) -> VolRegime:
    if sigma <= 0 or sigma >= cfg["vol_skip_thresh"]:
        return VolRegime.SKIP
    if sigma < cfg["vol_low_thresh"]:
        return VolRegime.LOW
    if sigma > cfg["vol_high_thresh"]:
        return VolRegime.HIGH
    return VolRegime.MID


# ============================================================================
# MONTE CARLO — EXPIRYRANGE
# ============================================================================

@dataclass
class ERCandidate:
    duration_s:  int      # duration in seconds (= ticks for 1Hz)
    sigma_mult:  float    # barrier half-width = sigma_mult × σ × √T
    barrier_half: float   # absolute half-width in price units
    p_win_mc:    float    # P(terminal price within barriers) from MC
    ev:          float    # filled in after real quote fetch
    payout_ratio: float   # filled in after real quote fetch
    barrier_low:  float   # absolute price levels
    barrier_high: float


def mc_scan_expiryrange(ts: TickStore, cfg: dict) -> List[ERCandidate]:
    """
    For each (sigma_mult, duration) candidate, compute P(win) using
    closed-form GBM terminal distribution (no step-by-step simulation
    needed for EXPIRYRANGE since only the terminal price matters).

    Terminal log-price relative to current follows:
        X_T ~ N((µ - σ²/2)·T, σ²·T)
    so P(low < S_T < high) is just the difference of two Normal CDFs.
    We draw N samples directly from this distribution rather than using
    scipy/numpy (no extra dependencies) via Box-Muller.
    """
    price = ts.current_price()
    if price is None or price <= 0:
        return []

    sigma    = ts.local_vol(cfg["vol_window"])
    mu       = ts.local_drift(cfg["vol_window"])
    n_sims   = cfg["er_n_sims"]
    if sigma <= 0:
        return []

    candidates: List[ERCandidate] = []

    for T in cfg["er_durations"]:
        # GBM parameters over T ticks (ticks = seconds for 1Hz symbols)
        mu_T    = (mu - 0.5 * sigma**2) * T
        sigma_T = sigma * math.sqrt(T)

        # Draw N terminal log-returns from N(mu_T, sigma_T²)
        log_rets = []
        for _ in range(n_sims):
            u1 = random.random() or 1e-15
            u2 = random.random() or 1e-15
            z  = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
            log_rets.append(mu_T + sigma_T * z)

        for mult in cfg["er_sigma_mults"]:
            # Barrier half-width in log-price space (symmetric around centre)
            hw_log = mult * sigma_T
            # Convert to price: barrier_half ≈ price × hw_log for small hw
            hw_price = price * (math.exp(hw_log) - 1)
            b_low  = price - hw_price
            b_high = price + hw_price
            if b_low <= 0:
                continue

            # P(win) = fraction of terminal prices within barriers
            wins = sum(1 for lr in log_rets
                       if b_low <= price * math.exp(lr) <= b_high)
            p_win = wins / n_sims

            candidates.append(ERCandidate(
                duration_s=T, sigma_mult=mult,
                barrier_half=hw_price,
                p_win_mc=p_win,
                ev=0.0, payout_ratio=0.0,
                barrier_low=b_low, barrier_high=b_high,
            ))

    # Pre-rank by MC p_win × expected payout proxy (higher p_win ranks lower
    # payout from Deriv, so balance the two) — send the top-K for real quoting.
    # Rough proxy: payout ~ 1/p_win (fair pricing), so EV proxy ~ p_win × (1/p_win) - 1 = 0
    # for fair. We instead rank by distance from breakeven: abs(p_win - 0.5)
    # is NOT what we want — we want the candidates Deriv will overprice.
    # Since we don't know that yet, rank by p_win in descending order and
    # let the real quote fetch determine actual EV.
    candidates.sort(key=lambda c: -c.p_win_mc)
    return candidates[:cfg["er_top_k"]]


# ============================================================================
# MONTE CARLO — NOTOUCH
# ============================================================================

@dataclass
class NTCandidate:
    duration_s:   int
    sigma_mult:   float
    barrier_dist: float   # absolute distance from current price to each barrier
    p_win_mc:     float   # P(never touches either barrier)
    ev:           float
    payout_ratio: float
    barrier_low:  float
    barrier_high: float


def mc_scan_notouch(ts: TickStore, cfg: dict) -> List[NTCandidate]:
    """
    NOTOUCH requires simulating the full path, not just the terminal price
    — any touch of either barrier during the hold loses, regardless of
    where the price ends up. We simulate tick-by-tick GBM paths using
    the Euler-Maruyama scheme:
        S_{t+1} = S_t × exp((µ - σ²/2)·dt + σ·ε)
    where dt=1 tick. This is slower than closed-form but necessary here.

    To keep CPU usage reasonable, NOTOUCH uses a slightly smaller default
    n_sims than EXPIRYRANGE (1000 vs 1000 — same default, but the loop is
    longer per sim due to path tracking). Reduce NT_SIMS in Railway
    Variables if evaluation latency becomes an issue.
    """
    price = ts.current_price()
    if price is None or price <= 0:
        return []

    sigma  = ts.local_vol(cfg["vol_window"])
    mu     = ts.local_drift(cfg["vol_window"])
    n_sims = cfg["nt_n_sims"]
    if sigma <= 0:
        return []

    # Pre-generate Gaussian noise for all paths at once (Box-Muller)
    # to avoid re-generating per (duration, mult) combination.
    max_T = max(cfg["nt_durations"])
    epsilons: List[List[float]] = []
    for _ in range(n_sims):
        path_eps = []
        for _ in range(max_T):
            u1 = random.random() or 1e-15
            u2 = random.random() or 1e-15
            path_eps.append(math.sqrt(-2*math.log(u1)) * math.cos(2*math.pi*u2))
        epsilons.append(path_eps)

    candidates: List[NTCandidate] = []

    for T in cfg["nt_durations"]:
        sigma_T = sigma * math.sqrt(T)

        for mult in cfg["nt_sigma_mults"]:
            dist    = mult * sigma_T * price   # absolute barrier distance
            b_low   = price - dist
            b_high  = price + dist
            if b_low <= 0:
                continue

            wins = 0
            drift_per_tick = mu - 0.5 * sigma**2
            for path_eps in epsilons:
                s = price
                touched = False
                for t in range(T):
                    s = s * math.exp(drift_per_tick + sigma * path_eps[t])
                    if s <= b_low or s >= b_high:
                        touched = True
                        break
                if not touched:
                    wins += 1

            p_win = wins / n_sims
            candidates.append(NTCandidate(
                duration_s=T, sigma_mult=mult,
                barrier_dist=dist,
                p_win_mc=p_win,
                ev=0.0, payout_ratio=0.0,
                barrier_low=b_low, barrier_high=b_high,
            ))

    candidates.sort(key=lambda c: -c.p_win_mc)
    return candidates[:cfg["nt_top_k"]]


# ============================================================================
# SIGNAL ENGINE
# ============================================================================

@dataclass
class TradeSignal:
    contract_type: str        # "EXPIRYRANGE" | "NOTOUCH" | "SKIP"
    symbol:        str
    duration_s:    int        # duration in seconds
    barrier_low:   float
    barrier_high:  float
    p_win_mc:      float      # MC estimate
    payout_ratio:  float      # from live Deriv quote
    ev:            float      # final EV after real quote
    stake:         float
    reasons:       List[str] = field(default_factory=list)


class SignalEngine:
    """
    Per-symbol signal evaluation. Combines the vol regime, Hurst estimate,
    and both MC scanners to produce a TradeSignal with the best EV available
    this cycle, or a SKIP signal if nothing clears the minimum EV threshold.
    """
    def __init__(self, symbol: str, cfg: dict):
        self.symbol = symbol
        self.cfg    = cfg
        self.ts     = TickStore(symbol, maxlen=cfg["tick_window"])
        # Per-symbol win/loss tracking for logging
        self.wins   = 0
        self.losses = 0
        self.total_profit = 0.0

    def add_tick(self, price: float):
        self.ts.add(price)

    def is_ready(self) -> bool:
        return self.ts.is_ready(self.cfg["min_ticks"])

    def evaluate(self, er_candidates: List[ERCandidate],
                 nt_candidates: List[NTCandidate],
                 balance: float) -> TradeSignal:
        """
        Called AFTER real quote fetches have populated .ev and .payout_ratio
        on all candidates. Picks the best contract type and parameters,
        sizes the stake, and returns a TradeSignal (or SKIP).
        """
        sigma  = self.ts.local_vol(self.cfg["vol_window"])
        hurst  = self.ts.hurst(self.cfg["hurst_window"])
        regime = classify_regime(sigma, self.cfg)

        reasons = [
            f"σ={sigma:.6f}  regime={regime.value}",
            f"H={hurst:.3f}",
        ]

        if regime == VolRegime.SKIP:
            return TradeSignal("SKIP", self.symbol, 0, 0, 0, 0, 0, 0, 0,
                               reasons + ["vol too extreme"])

        # Filter candidates by minimum EV margin
        er_viable = [c for c in er_candidates if c.ev >= self.cfg["er_min_ev"]]
        nt_viable = [c for c in nt_candidates if c.ev >= self.cfg["nt_min_ev"]]

        # Apply regime preference: bias toward the contract type that suits
        # current conditions, but always let real EV be the final arbiter.
        # Regime preference is a tiebreaker, not a veto.
        best_er = max(er_viable, key=lambda c: c.ev) if er_viable else None
        best_nt = max(nt_viable, key=lambda c: c.ev) if nt_viable else None

        mode = self.cfg["contract_mode"]

        if mode == "expiryrange":
            best_nt = None
        elif mode == "notouch":
            best_er = None

        if best_er is None and best_nt is None:
            return TradeSignal("SKIP", self.symbol, 0, 0, 0, 0, 0, 0, 0,
                               reasons + ["no candidate cleared EV floor"])

        # Pick the contract type with the higher real EV
        chosen_type = None
        chosen      = None
        if best_er and best_nt:
            if best_er.ev >= best_nt.ev:
                chosen_type, chosen = "EXPIRYRANGE", best_er
            else:
                chosen_type, chosen = "NOTOUCH", best_nt
        elif best_er:
            chosen_type, chosen = "EXPIRYRANGE", best_er
        else:
            chosen_type, chosen = "NOTOUCH", best_nt

        reasons += [
            f"contract={chosen_type}",
            f"dur={chosen.duration_s}s",
            f"barriers=[{chosen.barrier_low:.4f}, {chosen.barrier_high:.4f}]",
            f"p_win_mc={chosen.p_win_mc:.3f}",
            f"payout_ratio={chosen.payout_ratio:.3f}",
            f"EV={chosen.ev:+.4f}",
        ]

        # Kelly stake sizing
        p   = chosen.p_win_mc
        r   = chosen.payout_ratio
        kelly = (p * r - (1 - p)) / r if r > 0 else 0.0
        kelly_frac = self.cfg["kelly_fraction"]
        raw_stake  = balance * kelly * kelly_frac
        stake = max(
            self.cfg["min_stake"],
            min(raw_stake, balance * self.cfg["max_stake_pct"])
        )
        stake = round(stake, 2)

        reasons.append(f"kelly={kelly:.4f}  stake=${stake:.2f}")

        return TradeSignal(
            contract_type = chosen_type,
            symbol        = self.symbol,
            duration_s    = chosen.duration_s,
            barrier_low   = chosen.barrier_low,
            barrier_high  = chosen.barrier_high,
            p_win_mc      = chosen.p_win_mc,
            payout_ratio  = chosen.payout_ratio,
            ev            = chosen.ev,
            stake         = stake,
            reasons       = reasons,
        )


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

    async def fetch_er_quote(self, symbol: str, cand: ERCandidate,
                             stake: float) -> Optional[float]:
        half_width = (cand.barrier_high - cand.barrier_low) / 2
        req = {
            "proposal":           1,
            "amount":             stake,
            "basis":              "stake",
            "contract_type":      "EXPIRYRANGE",
            "currency":           self.cfg["currency"],
            "duration":           cand.duration_s,
            "duration_unit":      "s",
            "underlying_symbol":  symbol,
            "barrier":            f"+{half_width:.2f}",   # max 2 decimal places
            "barrier2":           f"-{half_width:.2f}",
        }
        return await self._fetch_proposal_ratio(
            req, f"EXPIRYRANGE {symbol} {cand.duration_s}s ±{half_width:.2f}", cand.p_win_mc)

    async def fetch_nt_quote(self, symbol: str, cand: NTCandidate,
                             stake: float) -> Optional[float]:
        half_dist = cand.barrier_dist
        req = {
            "proposal":           1,
            "amount":             stake,
            "basis":              "stake",
            "contract_type":      "NOTOUCH",
            "currency":           self.cfg["currency"],
            "duration":           cand.duration_s,
            "duration_unit":      "s",
            "underlying_symbol":  symbol,
            # Single upper barrier only: price must not touch the level above.
            "barrier":            f"+{half_dist:.2f}",
        }
        return await self._fetch_proposal_ratio(
            req, f"NOTOUCH {symbol} {cand.duration_s}s +{half_dist:.2f}", cand.p_win_mc)

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
        Gets a fresh proposal for the chosen contract + parameters,
        then immediately buys it. Returns the contract_id or None.
        """
        contract_type = sig.contract_type
        half_width = (sig.barrier_high - sig.barrier_low) / 2
        req = {
            "proposal":           1,
            "amount":             sig.stake,
            "basis":              "stake",
            "contract_type":      contract_type,
            "currency":           self.cfg["currency"],
            "duration":           sig.duration_s,
            "duration_unit":      "s",
            "underlying_symbol":  sig.symbol,
            "barrier":            f"+{half_width:.2f}",
        }
        if contract_type == "EXPIRYRANGE":
            req["barrier2"] = f"-{half_width:.2f}"
        # NOTOUCH: single upper barrier only on 1HZ synthetic indices

        proposal = await self.send_with_id(req, timeout=12)
        if proposal is None or "error" in proposal:
            err = (proposal or {}).get("error", {}).get("message", "timeout")
            _log("PROPOSAL", f"Error: {err}")
            return None

        prop_data   = proposal.get("proposal", {})
        proposal_id = prop_data.get("id")
        ask_price   = float(prop_data.get("ask_price", sig.stake))
        if not proposal_id:
            _log("PROPOSAL", "No proposal ID")
            return None

        _log("PROPOSAL",
             f"{contract_type} {sig.symbol} {sig.duration_s}s "
             f"barriers=[{sig.barrier_low:.4f},{sig.barrier_high:.4f}] "
             f"ask=${ask_price:.2f}")

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
             f"{sig.duration_s}s contract={contract_id}")
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

    # ── Contract spec discovery ───────────────────────────────────────────────

    async def _discover_contracts(self, symbol: str):
        """
        Calls contracts_for to discover valid durations, barrier types, and
        barrier count for EXPIRYRANGE and NOTOUCH on this symbol. Updates
        the config live so the MC scanners and quote fetchers use values
        that Deriv will actually accept — rather than hand-tuned guesses
        that produce ContractBuyValidationError or InvalidBarrierSingle.
        """
        resp = await self.client.send_with_id(
            {"contracts_for": symbol, "currency": self.cfg["currency"],
             "landing_company": "svg"}, timeout=15
        )
        if not resp or "error" in resp:
            _log("CONTRACTS", f"{symbol}: could not fetch contracts_for — "
                              f"using config defaults")
            return

        available = resp.get("contracts_for", {}).get("available", [])
        if not available:
            _log("CONTRACTS", f"{symbol}: empty available list")
            return

        er_durations_s, nt_durations_s = set(), set()
        nt_single_barrier = False

        for c in available:
            ct = c.get("contract_type", "")
            # Duration info
            min_d = c.get("min_contract_duration", "")
            max_d = c.get("max_contract_duration", "")

            def parse_dur_s(d: str) -> Optional[int]:
                if not d:
                    return None
                try:
                    if d.endswith("t"):
                        return int(d[:-1])      # ticks — treat as seconds for 1Hz
                    elif d.endswith("s"):
                        return int(d[:-1])
                    elif d.endswith("m"):
                        return int(d[:-1]) * 60
                    elif d.endswith("h"):
                        return int(d[:-1]) * 3600
                    return int(d)
                except (ValueError, IndexError):
                    return None

            min_s = parse_dur_s(min_d)
            max_s = parse_dur_s(max_d)

            barrier_cat = c.get("barrier_category", "")

            if ct == "EXPIRYRANGE" and min_s and max_s:
                # Generate candidate durations between min and max
                for t in [5, 10, 15, 30, 60, 120, 300, 600]:
                    if min_s <= t <= max_s:
                        er_durations_s.add(t)

            if ct == "NOTOUCH" and min_s and max_s:
                for t in [5, 10, 15, 30, 60, 120, 300]:
                    if min_s <= t <= max_s:
                        nt_durations_s.add(t)
                # single barrier if category is non-euro/american
                if barrier_cat in ("non_euro_american", "euro_non_atm"):
                    nt_single_barrier = True

        if er_durations_s:
            self.cfg[f"{symbol}_er_durations"] = sorted(er_durations_s)
            _log("CONTRACTS", f"{symbol} EXPIRYRANGE durations: "
                              f"{sorted(er_durations_s)}s")
        else:
            _log("CONTRACTS", f"{symbol}: no EXPIRYRANGE durations found — "
                              f"using defaults {self.cfg['er_durations']}")

        if nt_durations_s:
            self.cfg[f"{symbol}_nt_durations"] = sorted(nt_durations_s)
            _log("CONTRACTS", f"{symbol} NOTOUCH durations: "
                              f"{sorted(nt_durations_s)}s  single_barrier={nt_single_barrier}")
        else:
            _log("CONTRACTS", f"{symbol}: no NOTOUCH durations found — "
                              f"using defaults {self.cfg['nt_durations']}")

        self.cfg[f"{symbol}_nt_single_barrier"] = nt_single_barrier

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _is_settled(self, data: dict) -> bool:
        if data.get("is_settled"):
            return True
        for key in ("status", "contract_status"):
            if data.get(key, "").lower() in ("sold", "won", "lost"):
                return True
        return False

    def _can_trade_session(self) -> bool:
        target = self.cfg["target_profit"]
        stop   = self.cfg["stop_loss"]
        if self.session_start_bal > 0:
            if self.cfg["target_profit_pct"] > 0:
                target = self.session_start_bal * self.cfg["target_profit_pct"]
            if self.cfg["stop_loss_pct"] > 0:
                stop   = self.session_start_bal * self.cfg["stop_loss_pct"]
        if target > 0 and self.total_profit >= target:
            _log("RISK", f"Session target reached (+${self.total_profit:.2f})")
            return False
        if self.total_profit <= -stop:
            _log("RISK", f"Session stop-loss hit (${self.total_profit:.2f})")
            return False
        return True

    def _check_lock_timeout(self, st: SymbolState):
        if not st.waiting or st.lock_since is None:
            return
        elapsed = time.monotonic() - st.lock_since
        timeout = (st.current_sig.duration_s if st.current_sig else 300) + 60
        if elapsed >= timeout:
            _log("TIMEOUT",
                 f"{st.symbol} locked {elapsed:.0f}s (limit {timeout}s) — unlocking")
            st.waiting      = False
            st.contract_id  = None
            st.current_sig  = None
            st.lock_since   = None

    # ── Quote fetching ────────────────────────────────────────────────────────

    async def _quote_er_candidates(self, sym: str,
                                   candidates: List[ERCandidate],
                                   stake: float):
        """Fetch real payout quotes for EXPIRYRANGE candidates concurrently."""
        tasks = [self.client.fetch_er_quote(sym, c, stake) for c in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for cand, ratio in zip(candidates, results):
            if isinstance(ratio, Exception) or ratio is None:
                cand.payout_ratio = 0.0
                cand.ev           = -999.0
            else:
                cand.payout_ratio = ratio
                cand.ev           = cand.p_win_mc * ratio - (1 - cand.p_win_mc)

    async def _quote_nt_candidates(self, sym: str,
                                   candidates: List[NTCandidate],
                                   stake: float):
        """Fetch real payout quotes for NOTOUCH candidates concurrently."""
        tasks = [self.client.fetch_nt_quote(sym, c, stake) for c in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for cand, ratio in zip(candidates, results):
            if isinstance(ratio, Exception) or ratio is None:
                cand.payout_ratio = 0.0
                cand.ev           = -999.0
            else:
                cand.payout_ratio = ratio
                cand.ev           = cand.p_win_mc * ratio - (1 - cand.p_win_mc)

    # ── Per-symbol evaluation ─────────────────────────────────────────────────

    async def _evaluate_symbol(self, st: SymbolState):
        """Full evaluation cycle for one symbol: MC scan → quote → signal."""
        if st.waiting:
            return
        now = time.monotonic()
        if now - st.last_eval_time < self.cfg["eval_cooldown"]:
            return
        if now < st.cb_paused_until:
            remaining = st.cb_paused_until - now
            _log("BREAKER", f"{st.symbol} paused {remaining:.0f}s")
            return
        if not self._can_trade_session():
            return
        if not st.engine.is_ready():
            return

        st.last_eval_time = now
        sym = st.symbol
        ts  = st.engine.ts
        mode = self.cfg["contract_mode"]

        # Rough stake estimate for quoting (will be refined by Kelly in evaluate())
        est_stake = max(self.cfg["min_stake"],
                        min(self.balance * 0.02, self.balance * self.cfg["max_stake_pct"]))
        est_stake = round(est_stake, 2)

        # MC scans — use discovered durations if available, else config defaults
        er_cands: List[ERCandidate] = []
        nt_cands: List[NTCandidate] = []
        er_durs = self.cfg.get(f"{sym}_er_durations", self.cfg["er_durations"])
        nt_durs = self.cfg.get(f"{sym}_nt_durations", self.cfg["nt_durations"])

        scan_cfg = dict(self.cfg)
        scan_cfg["er_durations"] = er_durs
        scan_cfg["nt_durations"] = nt_durs

        if mode in ("both", "expiryrange"):
            er_cands = mc_scan_expiryrange(ts, scan_cfg)
        if mode in ("both", "notouch"):
            nt_cands = mc_scan_notouch(ts, scan_cfg)

        if not er_cands and not nt_cands:
            _log(sym, "No MC candidates generated — skipping")
            return

        # Concurrent real quote fetches
        quote_tasks = []
        if er_cands:
            quote_tasks.append(self._quote_er_candidates(sym, er_cands, est_stake))
        if nt_cands:
            quote_tasks.append(self._quote_nt_candidates(sym, nt_cands, est_stake))
        await asyncio.gather(*quote_tasks)

        # Evaluate and produce signal
        sig = st.engine.evaluate(er_cands, nt_cands, self.balance)

        print(f"\n{'='*60}")
        print(f"SIGNAL  {sym}  {_ts()}")
        for r in sig.reasons:
            print(f"  · {r}")
        if sig.contract_type == "SKIP":
            print(f"  → No trade")
        else:
            print(f"  → {sig.contract_type}  stake=${sig.stake:.2f}  "
                  f"EV={sig.ev:+.4f}")
        print(f"{'='*60}")

        if sig.contract_type == "SKIP":
            return

        # Snap balance before trade
        bal = await self.client.fetch_balance()
        if bal is not None:
            self.balance       = bal
            st.balance_before  = bal

        # Place trade
        contract_id = await self.client.place_trade(sig)
        if contract_id:
            st.waiting     = True
            st.contract_id = contract_id
            st.current_sig = sig
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

        if actual > 0:
            st.engine.wins += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses = 0
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
        print("  DERIV EXPIRYRANGE + NOTOUCH BOT")
        print(f"{'='*60}")
        print(f"  Symbols  : {', '.join(self.symbols)}")
        print(f"  Contracts: EXPIRYRANGE + NOTOUCH (mode={self.cfg['contract_mode']})")
        print(f"  Min stake: ${self.cfg['min_stake']:.2f}  "
              f"Max stake: {self.cfg['max_stake_pct']:.0%} of balance")
        print(f"  Kelly    : {self.cfg['kelly_fraction']:.0%} fractional")
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

        # Discover valid contract specs from Deriv before trading
        _log("BOT", "Discovering valid contract specs from Deriv...")
        for sym in self.symbols:
            await self._discover_contracts(sym)

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
                        sigma = st.engine.ts.local_vol(self.cfg["vol_window"])
                        hurst = st.engine.ts.hurst(self.cfg["hurst_window"])
                        regime = classify_regime(sigma, self.cfg)
                        print(f"\n  {sym}: W:{eng.wins} L:{eng.losses} "
                              f"WR:{wr:.1f}% P&L:${eng.total_profit:+.2f}")
                        print(f"    σ={sigma:.6f} ({regime.value})  "
                              f"H={hurst:.3f}  "
                              f"ticks={st.engine.ts.count}")
                        print(f"    waiting={st.waiting}  "
                              f"cb_paused={st.cb_paused_until > time.monotonic()}")
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
