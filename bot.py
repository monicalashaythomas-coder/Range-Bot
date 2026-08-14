"""
DERIV EXPIRYRANGE + NOTOUCH BOT  v2
====================================
Symbols  : 1HZ10V  (Volatility 10 Index)
           1HZ100V (Volatility 100 Index)
Contracts: EXPIRYRANGE — terminal price must land WITHIN fixed barriers at expiry
           NOTOUCH     — price must NEVER touch the upper barrier during hold

Fixed barriers (2-minute expiry, from live testing):
  1HZ10V  EXPIRYRANGE : ±1.60 price units
  1HZ10V  NOTOUCH     : +1.70 price units (upper barrier only)
  1HZ100V EXPIRYRANGE : ±1.16 price units
  1HZ100V NOTOUCH     : +1.16 price units (upper barrier only)

Signal philosophy
─────────────────
  We do NOT scan for the best EV across a grid of barriers.
  The barriers are fixed. The only question is: does the market,
  RIGHT NOW, give us enough confidence that price will behave as
  required over the next 2 minutes?

  Five intelligence layers must reach agreement before a trade fires.
  Layers are evaluated per-contract-type — EXPIRYRANGE and NOTOUCH
  have different requirements because they measure different things
  (terminal containment vs path non-touch):

  LAYER 1 — Momentum direction (last 5 and 20 ticks)
    · Short momentum (5-tick): direction and magnitude of recent move
    · Medium momentum (20-tick): whether momentum is sustained
    · EXPIRYRANGE: wants LOW momentum in both windows (price is drifting,
      not running — a price that is running is likely to exit barriers)
    · NOTOUCH: wants price MOVING AWAY from the upper barrier — downward
      or flat momentum is bullish for a NOTOUCH upper barrier bet

  LAYER 2 — Volatility level (σ from last 60 ticks)
    · EXPIRYRANGE: low σ strongly favours containment (±1.6 is wide
      relative to the expected move if vol is low)
    · NOTOUCH: moderate vol is acceptable if momentum is downward;
      very high vol makes upper barrier touch more likely

  LAYER 3 — Hurst exponent (mean-reversion vs trend, last 80 ticks)
    · EXPIRYRANGE: H < 0.50 (mean-reverting) strongly favoured —
      price is likely to oscillate back to centre, not drift to edges
    · NOTOUCH: H > 0.50 (trending) favoured when price is trending
      downward; OR H < 0.45 (strong mean-reversion) also works because
      a mean-reverting market is unlikely to make a sustained run to touch

  LAYER 4 — Recent price level vs upper barrier
    · How far is current price from the upper barrier?
    · NOTOUCH: the closer price is to the barrier, the higher the
      touch risk — gate blocks if price is already within 40% of
      the barrier distance from above
    · EXPIRYRANGE: symmetric, so gate checks that price is near
      the centre of the range, not already pushed to one edge

  LAYER 5 — Monte Carlo path/terminal simulation
    · EXPIRYRANGE: draws N terminal prices from GBM(µ, σ, T=120s)
      P(|terminal - entry| < barrier) is the direct estimate
    · NOTOUCH: simulates N full tick-by-tick paths
      P(max(price) < entry + barrier throughout 120s) is the estimate
    · Both must clear MIN_MC_CONFIDENCE threshold
    · MC is the final arbiter — all other layers passing but MC
      saying P(win) < threshold → no trade

  AGREEMENT LOGIC:
    · Each layer votes pass/fail with a confidence weight
    · Total weighted score must exceed SIGNAL_THRESHOLD
    · MC confidence must independently exceed MIN_MC_CONFIDENCE
    · Both conditions must hold simultaneously

Risk management
───────────────
  · Flat stake per trade (configurable), not Kelly
    (payout ratios are too thin for Kelly to size meaningfully)
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
    "symbols":          ["1HZ10V", "1HZ100V"],
    "currency":         "USD",

    # ── Barrier scan parameters ────────────────────────────────────
    # Multiples of σ×√T×price (expected absolute price move over duration).
    # For 1HZ10V: sigma≈0.00002, √120≈10.95, price≈9527 → sigma_T≈2.09
    # For 1HZ100V: sigma≈0.00018, √120≈10.95, price≈720 → sigma_T≈1.42
    # Confirmed valid barrier from API testing: 1HZ10V ±1.60, 1HZ100V ±1.16
    # These correspond roughly to 0.5-0.8× sigma_T, so scan that range.
    "er_barrier_mults": [0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00],
    "nt_barrier_mults": [0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.20, 1.50],
    "min_return":       _env("MIN_RETURN",  0.40),
    "min_edge_margin":  _env("MIN_EDGE",    0.08),
    "scan_top_k":       _env("SCAN_TOP_K",     5),

    # ── Contract duration ─────────────────────────────────────────
    "duration_s":       120,   # 2 minutes in seconds
    "duration_unit":    "s",

    # ── Tick history ──────────────────────────────────────────────
    "tick_window":      300,   # keep last 300 ticks (5 minutes)
    "min_ticks":        90,    # minimum before evaluating (1.5 min warmup)

    # ── Signal layer parameters ───────────────────────────────────
    # Layers 1-4 evaluate market conditions only — no barrier reference.
    # Barrier selection happens after in the quote scan phase.
    "momentum_short_n":   5,
    "momentum_medium_n":  20,
    "vol_window":         60,
    "vol_skip_thresh":    _env("VOL_SKIP", 0.000300),
    "hurst_window":       80,
    # Proximity now uses a rolling price range rather than a fixed barrier
    "proximity_range_n":  30,   # ticks for rolling range estimate
    "proximity_er_gate":  0.70, # skip ER if price is in top/bottom 30% of recent range
    "proximity_nt_gate":  0.60, # skip NT if price trended up >60% of recent range
    # Monte Carlo
    "mc_n_sims":          2000,
    # Signal threshold for layers 1-4
    "signal_threshold":   _env("SIGNAL_THRESH", 0.55),

    # ── Contract mode ─────────────────────────────────────────────
    # "both" → evaluate EXPIRYRANGE and NOTOUCH each cycle, pick best MC score
    # "expiryrange" → only EXPIRYRANGE
    # "notouch"     → only NOTOUCH
    # Global default, overridden per-symbol below.
    "contract_mode":          _env("CONTRACT_MODE", "both"),
    # Per-symbol contract mode — 1HZ100V EXPIRYRANGE disabled because it
    # performs poorly; 1HZ10V runs both. Change via Railway Variables.
    "contract_mode_1HZ10V":   _env("CONTRACT_MODE_1HZ10V",  "both"),
    "contract_mode_1HZ100V":  _env("CONTRACT_MODE_1HZ100V", "notouch"),

    # ── Martingale ────────────────────────────────────────────────
    # After a loss, multiply stake by martingale_factor up to max_steps.
    # On a win or after max_steps losses, stake resets to base.
    # Sequence guard: total committed in one sequence must not exceed
    # martingale_guard_pct of the balance at sequence start.
    "martingale_factor":    _env("MARTINGALE_FACTOR", 1.45),
    "martingale_max_steps": _env("MARTINGALE_STEPS",     3),
    "martingale_guard_pct": _env("MARTINGALE_GUARD",  0.20),

    # ── Stake sizing ─────────────────────────────────────────────
    # Flat stake — not Kelly (payouts are too thin for Kelly to be meaningful)
    "stake":            _env("STAKE", 1.00),
    "max_stake":        _env("MAX_STAKE", 50.00),

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
# SIGNAL ENGINE — 5-layer intelligence for fixed-barrier contracts
# ============================================================================

@dataclass
class ContractSignal:
    """Result of the full layer evaluation for one contract type."""
    contract_type:    str   # "EXPIRYRANGE" | "NOTOUCH" — the intended contract
    symbol:           str
    barrier:          float
    p_win_mc:         float
    layer_score:      float
    reasons:          List[str]
    passed_layers:    bool = False  # True = layers 1-4 passed, ready for barrier scan


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


def _momentum_score(ts: TickStore, short_n: int, medium_n: int,
                    contract_type: str) -> Tuple[float, str]:
    """
    Layer 1 — Momentum (barrier-agnostic).
    EXPIRYRANGE: wants low momentum in both windows.
    NOTOUCH (upper): wants price moving down or flat.
    """
    prices = list(ts.prices)
    if len(prices) < medium_n + 1:
        return 0.5, "momentum=insufficient_data"

    short_prices = prices[-short_n-1:]
    short_move   = (short_prices[-1] - short_prices[0]) / abs(short_prices[0]) \
                   if short_prices[0] != 0 else 0.0
    med_prices   = prices[-medium_n-1:]
    med_move     = (med_prices[-1] - med_prices[0]) / abs(med_prices[0]) \
                   if med_prices[0] != 0 else 0.0

    norm = 0.0005
    if contract_type == "EXPIRYRANGE":
        s_score = max(0.0, 1.0 - abs(short_move) / norm)
        m_score = max(0.0, 1.0 - abs(med_move) / (norm * 2))
        score   = 0.5 * s_score + 0.5 * m_score
        reason  = (f"momentum_er short={short_move*100:+.3f}% "
                   f"med={med_move*100:+.3f}% score={score:.2f}")
    else:
        s_score = max(0.0, min(1.0, 0.5 - short_move / norm * 0.5))
        m_score = max(0.0, min(1.0, 0.5 - med_move / (norm * 2) * 0.5))
        score   = 0.5 * s_score + 0.5 * m_score
        direction = "down" if (short_move + med_move) < 0 else "up/flat"
        reason  = (f"momentum_nt {direction} short={short_move*100:+.3f}% "
                   f"med={med_move*100:+.3f}% score={score:.2f}")

    return score, reason


def _vol_score(ts: TickStore, vol_window: int, vol_skip: float,
               contract_type: str) -> Tuple[float, str]:
    """
    Layer 2 — Volatility (barrier-agnostic).
    No longer compares against a fixed barrier — just characterises the
    vol regime. Low vol → EXPIRYRANGE favoured. High-but-not-chaotic vol
    → NOTOUCH favoured (price has structure, not chaos).
    """
    sigma = ts.local_vol(vol_window)
    if sigma <= 0 or sigma >= vol_skip:
        return 0.0, f"vol=skip (σ={sigma:.6f})"

    # Normalise against a typical mid-range sigma for these instruments
    mid_sigma = 0.000080  # empirically observed mid-point between 1HZ10V and 1HZ100V
    ratio     = sigma / mid_sigma  # < 1 = low vol, > 1 = high vol

    if contract_type == "EXPIRYRANGE":
        # Low vol is good — score decreases as vol rises
        score  = max(0.0, min(1.0, 1.5 - ratio))
        reason = f"vol_er σ={sigma:.5f} ratio_to_mid={ratio:.2f} score={score:.2f}"
    else:
        # Moderate vol is good — score peaks near mid_sigma, drops for very low or very high
        score  = max(0.0, min(1.0, 1.0 - abs(ratio - 1.0)))
        reason = f"vol_nt σ={sigma:.5f} ratio_to_mid={ratio:.2f} score={score:.2f}"

    return score, reason


def _hurst_score(ts: TickStore, hurst_window: int, contract_type: str) -> Tuple[float, str]:
    """Layer 3 — Hurst (unchanged — already barrier-agnostic)."""
    H = ts.hurst(hurst_window)
    if contract_type == "EXPIRYRANGE":
        score  = max(0.0, min(1.0, (0.70 - H) / 0.40))
        reason = f"hurst_er H={H:.3f} score={score:.2f}"
    else:
        deviation = abs(H - 0.5)
        score     = min(1.0, deviation / 0.25)
        regime    = "mean-rev" if H < 0.5 else "trending"
        reason    = f"hurst_nt H={H:.3f} ({regime}) score={score:.2f}"
    return score, reason


def _proximity_score(ts: TickStore, contract_type: str,
                     range_n: int, er_gate: float,
                     nt_gate: float) -> Tuple[float, str]:
    """
    Layer 4 — Proximity (now uses rolling price range, not fixed barrier).

    Instead of asking "how close is price to the barrier?" (which required
    knowing the barrier in advance), asks "where in the recent price range
    is the current price?" — a market condition that's meaningful regardless
    of which specific barrier we'll end up trading.

    EXPIRYRANGE: price near the middle of the recent range is good.
    Price near the top or bottom means it's already at an extreme — likely
    to overshoot one side of any symmetric barrier.

    NOTOUCH (upper): price trending toward the top of recent range is bad
    (it's already moving toward where an upper barrier would be).
    Price at the bottom of the range is ideal.
    """
    prices = list(ts.prices)
    if len(prices) < range_n:
        return 0.5, "proximity=insufficient_data"

    window  = prices[-range_n:]
    lo, hi  = min(window), max(window)
    rng     = hi - lo
    price   = prices[-1]

    if rng < 1e-10:
        return 0.5, "proximity=flat_market"

    # Where in [lo, hi] is current price? 0 = at bottom, 1 = at top
    position = (price - lo) / rng

    if contract_type == "EXPIRYRANGE":
        # Ideal at centre (position=0.5), worst at extremes (0 or 1)
        centrality = 1.0 - 2 * abs(position - 0.5)
        if centrality < (1.0 - er_gate):
            return 0.0, (f"proximity_er BLOCKED price at extreme "
                         f"position={position:.2f} centrality={centrality:.2f}")
        score  = centrality
        reason = f"proximity_er position={position:.2f} centrality={centrality:.2f} score={score:.2f}"
    else:  # NOTOUCH upper
        # Ideal at bottom of range (position≈0), worst at top (position≈1)
        if position > nt_gate:
            return 0.0, (f"proximity_nt BLOCKED price near top of range "
                         f"position={position:.2f}")
        score  = max(0.0, 1.0 - position / nt_gate)
        reason = f"proximity_nt position={position:.2f} score={score:.2f}"

    return score, reason


def evaluate_signal(ts: TickStore, cfg: dict,
                    contract_type: str) -> ContractSignal:
    """
    Runs layers 1-4 (barrier-agnostic market condition check).
    Returns a ContractSignal with contract_type=ACTIVE if layers pass,
    SKIP otherwise. MC and barrier selection happen AFTER in the quote
    scan phase once we know real Deriv payouts.

    barrier field is set to 0.0 here — it will be filled in by the caller
    after the quote scan selects the best barrier.
    """
    sym     = ts.symbol
    reasons = []

    WEIGHTS = {
        "momentum":  0.30,
        "vol":       0.35,
        "hurst":     0.20,
        "proximity": 0.15,
    }

    s1, r1 = _momentum_score(ts, cfg["momentum_short_n"],
                              cfg["momentum_medium_n"], contract_type)
    reasons.append(r1)

    s2, r2 = _vol_score(ts, cfg["vol_window"], cfg["vol_skip_thresh"], contract_type)
    reasons.append(r2)
    if s2 == 0.0 and "skip" in r2:
        return ContractSignal(contract_type, sym, 0.0, 0.0, 0.0,
                              reasons + ["vol_skip triggered"],
                              passed_layers=False)

    s3, r3 = _hurst_score(ts, cfg["hurst_window"], contract_type)
    reasons.append(r3)

    s4, r4 = _proximity_score(ts, contract_type,
                               cfg["proximity_range_n"],
                               cfg["proximity_er_gate"],
                               cfg["proximity_nt_gate"])
    reasons.append(r4)
    if s4 == 0.0 and "BLOCKED" in r4:
        return ContractSignal(contract_type, sym, 0.0, 0.0, 0.0,
                              reasons + ["proximity gate blocked"],
                              passed_layers=False)

    layer_score = (WEIGHTS["momentum"]  * s1 +
                   WEIGHTS["vol"]       * s2 +
                   WEIGHTS["hurst"]     * s3 +
                   WEIGHTS["proximity"] * s4)
    reasons.append(f"layer_score={layer_score:.3f} "
                   f"(threshold={cfg['signal_threshold']:.2f})")

    if layer_score < cfg["signal_threshold"]:
        return ContractSignal(contract_type, sym, 0.0, 0.0, layer_score,
                              reasons + ["below signal threshold"],
                              passed_layers=False)

    # Layers passed — barrier and MC determined by quote scan
    return ContractSignal(contract_type, sym, 0.0, 0.0, layer_score, reasons,
                          passed_layers=True)


def mc_p_win_expiryrange(ts: TickStore, barrier: float, duration_s: int,
                          n_sims: int) -> float:
    """GBM closed-form terminal draw — P(|terminal - entry| < barrier)."""
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


def mc_p_win_notouch(ts: TickStore, barrier: float, duration_s: int,
                      n_sims: int) -> float:
    """Tick-by-tick GBM path sim — P(price never touches entry + barrier)."""
    sigma = ts.local_vol(60)
    mu    = ts.local_drift(60)
    price = ts.current_price()
    if not price or sigma <= 0:
        return 0.0
    upper_barrier  = price + barrier
    drift_per_tick = mu - 0.5 * sigma**2
    wins = 0
    for _ in range(n_sims):
        s = price
        touched = False
        for _ in range(duration_s):
            u1 = random.random() or 1e-15
            u2 = random.random() or 1e-15
            z  = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
            s  = s * math.exp(drift_per_tick + sigma * z)
            if s >= upper_barrier:
                touched = True
                break
        if not touched:
            wins += 1
    return wins / n_sims

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

    async def scan_barriers(self, contract_type: str, symbol: str,
                            ts: TickStore, cfg: dict) -> Optional[dict]:
        """
        Scans a range of barrier widths, fetches real Deriv quotes for each
        concurrently, and finds the best one where:
          1. Payout meets the minimum return threshold (e.g. ≥40% profit)
          2. Our MC p_win beats the Deriv-implied breakeven p_win by
             at least min_edge_margin

        Returns a dict with the chosen barrier details, or None if no
        candidate clears both gates.

        The edge check:
          Deriv implied breakeven p_win = 1 / (1 + payout_ratio)
          Our MC p_win must exceed this by min_edge_margin.
          This is the only honest way to know if we have an edge —
          Deriv's payout reflects their model of p(win). If our MC
          says the true p(win) is materially higher, we have edge.
          If our MC agrees with Deriv's implied p(win) or is lower,
          we're paying fair price or worse.
        """
        sigma    = ts.local_vol(cfg["vol_window"])
        price    = ts.current_price()
        if not price or sigma <= 0:
            return None

        duration_s  = cfg["duration_s"]
        min_return  = cfg["min_return"]
        min_edge    = cfg["min_edge_margin"]
        n_sims      = cfg["mc_n_sims"]
        stake       = min(cfg["stake"], cfg["max_stake"])
        mults       = (cfg["er_barrier_mults"] if contract_type == "EXPIRYRANGE"
                       else cfg["nt_barrier_mults"])

        # Expected move in ABSOLUTE price units over the duration.
        # σ is return-scale (relative), so multiply by current price
        # to get price units. This is the right scale for barrier offsets.
        sigma_T  = sigma * math.sqrt(duration_s) * price

        # Build candidate barriers from sigma multiples
        candidates = []
        for mult in mults:
            b = round(mult * sigma_T, 2)
            if b < 0.01:
                continue
            candidates.append(b)
        if not candidates:
            return None

        # Fetch quotes concurrently for all candidates
        async def fetch_one(barrier: float) -> Tuple[float, float]:
            """Returns (barrier, payout_ratio) or (barrier, -1) on failure."""
            req = {
                "proposal":          1,
                "amount":            stake,
                "basis":             "stake",
                "contract_type":     contract_type,
                "currency":          cfg["currency"],
                "duration":          duration_s,
                "duration_unit":     cfg["duration_unit"],
                "underlying_symbol": symbol,
                "barrier":           f"+{barrier:.2f}",
            }
            if contract_type == "EXPIRYRANGE":
                req["barrier2"] = f"-{barrier:.2f}"
            resp = await self.send_with_id(req, timeout=12)
            if not resp or "error" in resp:
                err = (resp or {}).get("error", {}).get("message", "?")
                _log("SCAN", f"{contract_type} {symbol} ±{barrier:.2f}: {err}")
                return barrier, -1.0
            data = resp.get("proposal", {})
            ask  = float(data.get("ask_price", 0))
            pout = float(data.get("payout", 0))
            if ask <= 0:
                return barrier, -1.0
            ratio = (pout - ask) / ask
            _log("SCAN", f"{contract_type} {symbol} ±{barrier:.2f}: "
                         f"return={ratio*100:.1f}%  "
                         f"Deriv_implied_p={1/(1+ratio)*100:.1f}%  "
                         f"(need ≥{min_return*100:.0f}% return)")
            return barrier, ratio

        results = await asyncio.gather(*[fetch_one(b) for b in candidates])

        # Filter: must meet min return threshold
        viable = [(b, r) for b, r in results
                  if r >= min_return]
        if not viable:
            _log("SCAN", f"{contract_type} {symbol}: no barrier cleared "
                         f"{min_return*100:.0f}% return floor")
            return None

        # For each viable barrier, run MC and check edge
        best = None
        for barrier, payout_ratio in viable:
            # Deriv's implied breakeven: we need p_win > this to have edge
            deriv_implied_p = 1.0 / (1.0 + payout_ratio)

            if contract_type == "EXPIRYRANGE":
                mc_p = mc_p_win_expiryrange(ts, barrier, duration_s, n_sims)
            else:
                mc_p = mc_p_win_notouch(ts, barrier, duration_s, n_sims)

            edge = mc_p - deriv_implied_p
            _log("SCAN", f"{contract_type} {symbol} ±{barrier:.2f}: "
                         f"mc_p={mc_p:.3f} deriv_implied_p={deriv_implied_p:.3f} "
                         f"edge={edge:+.3f} (need ≥{min_edge:+.3f})")

            if edge < min_edge:
                continue

            # This candidate clears both gates — pick the one with most edge
            if best is None or edge > best["edge"]:
                best = {
                    "barrier":        barrier,
                    "payout_ratio":   payout_ratio,
                    "mc_p_win":       mc_p,
                    "deriv_implied_p": deriv_implied_p,
                    "edge":           edge,
                    "contract_type":  contract_type,
                    "symbol":         symbol,
                }

        if best:
            _log("SCAN", f"SELECTED {best['contract_type']} {best['symbol']} "
                         f"±{best['barrier']:.2f} "
                         f"return={best['payout_ratio']*100:.1f}% "
                         f"mc_p={best['mc_p_win']:.3f} "
                         f"edge={best['edge']:+.3f}")
        return best

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
        NOTOUCH: upper barrier only (+offset)
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
        }
        if contract_type == "EXPIRYRANGE":
            req["barrier2"] = f"-{barrier:.2f}"
        # NOTOUCH: single upper barrier only

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
    # Martingale state — resets on win or after max_steps losses
    martingale_step:      int   = 0    # 0 = base stake, 1+ = recovery steps
    martingale_committed: float = 0.0  # total staked in current sequence
    seq_start_balance:    float = 0.0  # balance snapshotted at sequence start


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

    # ── Martingale helpers ────────────────────────────────────────────────────

    def _get_stake(self, st: SymbolState, balance: float) -> float:
        """
        Returns the stake for this trade, factoring in martingale position.
        Applies the sequence-start balance guard before returning.
        """
        base   = min(self.cfg["stake"], self.cfg["max_stake"])
        factor = self.cfg["martingale_factor"]
        step   = st.martingale_step

        stake = round(base * (factor ** step), 2)
        stake = min(stake, self.cfg["max_stake"])

        # Guard: total committed in this sequence must not exceed
        # martingale_guard_pct of the balance at sequence start.
        if step > 0 and st.seq_start_balance > 0:
            guard_cap   = st.seq_start_balance * self.cfg["martingale_guard_pct"]
            would_commit = st.martingale_committed + stake
            if would_commit > guard_cap:
                _log("MARTINGALE",
                     f"{st.symbol} sequence guard — committed "
                     f"${st.martingale_committed:.2f} + ${stake:.2f} "
                     f"> cap ${guard_cap:.2f} — aborting sequence, reset to base")
                self._reset_martingale(st)
                stake = round(base, 2)

        return stake

    def _reset_martingale(self, st: SymbolState):
        st.martingale_step      = 0
        st.martingale_committed = 0.0
        st.seq_start_balance    = 0.0

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
        # Per-symbol mode override — e.g. 1HZ100V runs NOTOUCH only
        mode = self.cfg.get(f"contract_mode_{sym}",
                            self.cfg["contract_mode"])

        # Layers 1-4: barrier-agnostic market condition check
        er_sig = nt_sig = None
        if mode in ("both", "expiryrange"):
            er_sig = evaluate_signal(ts, self.cfg, "EXPIRYRANGE")
        if mode in ("both", "notouch"):
            nt_sig = evaluate_signal(ts, self.cfg, "NOTOUCH")

        # Print layer results
        print(f"\n{'='*60}")
        print(f"SIGNAL  {sym}  {_ts()}")
        for sig_candidate, label in [(er_sig, "ER"), (nt_sig, "NT")]:
            if sig_candidate:
                status = "ACTIVE" if sig_candidate.passed_layers else "SKIP"
                print(f"  [{label}] {sig_candidate.contract_type} {status}  "
                      f"layer={sig_candidate.layer_score:.3f}")
                for r in sig_candidate.reasons:
                    print(f"    · {r}")

        # Only proceed to barrier scan for contract types that passed layers 1-4
        active = [s for s in [er_sig, nt_sig]
                  if s and s.passed_layers]

        if not active:
            print(f"  → No trade (layers blocked)")
            print(f"{'='*60}")
            return

        # Barrier scan: concurrent quote fetch + MC edge check
        # s.contract_type is "EXPIRYRANGE" or "NOTOUCH" — the real type
        scan_tasks = [
            self.client.scan_barriers(s.contract_type, sym, ts, self.cfg)
            for s in active
        ]
        scan_results = await asyncio.gather(*scan_tasks)

        # Pick the result with the best edge (our MC advantage over Deriv's implied p)
        best = None
        for result in scan_results:
            if result is None:
                continue
            if best is None or result["edge"] > best["edge"]:
                best = result

        if not best:
            print(f"  → No trade (no barrier cleared return+edge gates)")
            print(f"{'='*60}")
            return

        print(f"  → {best['contract_type']} ±{best['barrier']:.2f} "
              f"return={best['payout_ratio']*100:.1f}% "
              f"mc_p={best['mc_p_win']:.3f} "
              f"edge={best['edge']:+.3f}")
        print(f"{'='*60}")

        # Snap balance before trade
        bal = await self.client.fetch_balance()
        if bal is not None:
            self.balance      = bal
            st.balance_before = bal

        # Martingale stake — respects the sequence guard
        stake = self._get_stake(st, self.balance)
        if st.martingale_step > 0:
            _log("MARTINGALE",
                 f"{sym} step={st.martingale_step} stake=${stake:.2f} "
                 f"(committed so far ${st.martingale_committed:.2f})")

        trade_sig = TradeSignal(
            contract_type = best["contract_type"],
            symbol        = sym,
            barrier       = best["barrier"],
            p_win_mc      = best["mc_p_win"],
            layer_score   = max(s.layer_score for s in active),
            stake         = stake,
            reasons       = [f"edge={best['edge']:+.3f} "
                             f"return={best['payout_ratio']*100:.1f}%"],
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

        if actual > 0:
            st.engine.wins += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses = 0
            self._reset_martingale(st)
            _log("WIN", f"{sym} +${actual:.2f} | "
                        f"session P&L ${self.total_profit:+.2f}")
        else:
            st.engine.losses += 1
            st.engine.total_profit += actual
            self.total_profit += actual
            st.consec_losses += 1

            # Martingale progression
            stake_placed = st.current_sig.stake if st.current_sig else self.cfg["stake"]
            if st.martingale_step == 0:
                # First loss in a new sequence — snapshot balance
                st.seq_start_balance = st.balance_before or self.balance
                st.martingale_committed = 0.0
            st.martingale_committed += stake_placed

            if st.martingale_step < self.cfg["martingale_max_steps"]:
                st.martingale_step += 1
                _log("MARTINGALE",
                     f"{sym} loss — advancing to step {st.martingale_step} "
                     f"(next stake "
                     f"${round(min(self.cfg['stake'], self.cfg['max_stake']) * self.cfg['martingale_factor']**st.martingale_step, 2):.2f}) "
                     f"committed so far ${st.martingale_committed:.2f}")
            else:
                _log("MARTINGALE",
                     f"{sym} max steps ({self.cfg['martingale_max_steps']}) "
                     f"reached — resetting sequence")
                self._reset_martingale(st)

            _log("LOSS", f"{sym} ${actual:.2f} | "
                         f"session P&L ${self.total_profit:+.2f} | "
                         f"streak={st.consec_losses}")
            limit = self.cfg["consec_loss_limit"]
            if st.consec_losses >= limit:
                pause = self.cfg["consec_pause_secs"]
                st.cb_paused_until = time.monotonic() + pause
                st.consec_losses   = 0
                self._reset_martingale(st)
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
                        sigma = eng.ts.local_vol(self.cfg["vol_window"])
                        hurst = eng.ts.hurst(self.cfg["hurst_window"])
                        barriers = self.cfg["barriers"].get(sym, {})
                        print(f"\n  {sym}: W:{eng.wins} L:{eng.losses} "
                              f"WR:{wr:.1f}% P&L:${eng.total_profit:+.2f}")
                        print(f"    σ={sigma:.6f}  H={hurst:.3f}  ticks={eng.ts.count}")
                        print(f"    barriers ER=±{barriers.get('er','?')} "
                              f"NT=+{barriers.get('nt','?')}")
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
