#!/usr/bin/env python3
"""
TRAIN_MODEL.PY — trains the shadow-mode ML win/loss classifier
================================================================

Pulls range_trades from Supabase (every closed trade, with the raw
indicator snapshot at entry + outcome), trains a regularized logistic
regression, evaluates it honestly with cross-validation, and — if it
clears a minimum sample size and isn't degenerate — publishes it to
Supabase's range_bot_config table as key='ml_model'.

The bot (bot.py) polls range_bot_config every ~15 minutes and picks up
whatever's published here automatically — no Railway redeploy needed.
The model is SHADOW-MODE ONLY: the bot logs what this model would have
predicted (ml_p_win) next to every signal, but never uses it to gate,
filter, or size an actual trade. That's a deliberate, separate,
human-approved step for later — this script only trains and publishes,
it never flips that switch.

WHY NOT JUST PICKLE THE SKLEARN MODEL:
The bot's runtime has no sklearn dependency and shouldn't need one just
to run this classifier. Logistic regression is nothing more than a dot
product + sigmoid, so we export standardization stats (mean/std per
feature) and the trained weights as plain JSON. bot.py re-implements the
6-line prediction in pure Python (_ml_predict). No pickle, no version-
skew risk between training and runtime environments.

USAGE
    export SUPABASE_URL=...
    export SUPABASE_KEY=...
    python3 train_model.py                    # train + report only
    python3 train_model.py --publish           # train + report + publish
    python3 train_model.py --since 2026-08-16 --symbol 1HZ10V --publish
"""

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    sys.exit("requests not installed — run: pip install requests")

try:
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import StratifiedKFold, cross_val_score
    from sklearn.metrics import roc_auc_score
except ImportError:
    sys.exit("scikit-learn/numpy not installed — run: "
              "pip install scikit-learn numpy")


# Must match bot.py's ML_FEATURE_ORDER exactly.
FEATURE_ORDER = [
    "ou_theta", "rsi", "stoch_rsi", "boll_width_pct", "zscore",
    "sr_edge_ratio", "sigma", "hurst", "momentum_short_pct",
    "momentum_med_pct", "rq_score", "layer_score", "p_win_mc",
]

MIN_TRADES_TO_PUBLISH = 150   # below this, a model is more noise than signal


def fetch_trades(url, key, since=None, symbol=None):
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    rows, offset, page = [], 0, 1000
    while True:
        params = {"select": "*", "order": "closed_at.asc",
                   "limit": str(page), "offset": str(offset)}
        if since:
            params["closed_at"] = f"gte.{since}"
        if symbol:
            params["symbol"] = f"eq.{symbol}"
        resp = requests.get(f"{url}/rest/v1/range_trades",
                             headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            sys.exit(f"Fetch failed: {resp.status_code} {resp.text[:200]}")
        batch = resp.json()
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


def build_matrix(rows):
    X, y, kept = [], [], []
    for r in rows:
        feats = []
        ok = True
        for f in FEATURE_ORDER:
            v = r.get(f)
            if v is None:
                ok = False
                break
            feats.append(float(v))
        if not ok:
            continue
        X.append(feats)
        y.append(1 if r.get("outcome") == "win" else 0)
        kept.append(r)
    return np.array(X), np.array(y), kept


def publish_model(url, key, weights: dict, bias: float, means: dict,
                   stds: dict, n: int, auc: float):
    version = datetime.now(timezone.utc).isoformat()
    payload = {
        "key": "ml_model",
        "value": {
            "version": version,
            "weights": weights,
            "bias": bias,
            "feature_means": means,
            "feature_stds": stds,
            "trained_on_n": n,
            "cv_auc": auc,
            "shadow_only": True,
        },
        "updated_at": version,
    }
    headers = {"apikey": key, "Authorization": f"Bearer {key}",
               "Content-Type": "application/json",
               "Prefer": "resolution=merge-duplicates"}
    resp = requests.post(f"{url}/rest/v1/range_bot_config",
                          headers=headers, json=payload, timeout=15)
    if resp.status_code not in (200, 201, 204):
        sys.exit(f"Publish failed: {resp.status_code} {resp.text[:300]}")
    print(f"\nPublished model version {version} to range_bot_config.")
    print("The bot will pick this up on its next remote-config poll "
          "(default: within 15 minutes) and start logging ml_p_win — "
          "it will NOT start using it to trade; that's a separate, "
          "manual step.")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since")
    ap.add_argument("--symbol")
    ap.add_argument("--publish", action="store_true",
                     help="Publish to Supabase if the model clears quality bars")
    args = ap.parse_args()

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        sys.exit("Set SUPABASE_URL and SUPABASE_KEY env vars first.")

    print("Fetching range_trades...")
    rows = fetch_trades(url, key, since=args.since, symbol=args.symbol)
    X, y, kept = build_matrix(rows)
    n = len(y)
    print(f"Loaded {len(rows)} trade rows, {n} usable "
          f"(had every required feature: {', '.join(FEATURE_ORDER)}).")

    if n < 30:
        sys.exit(f"Only {n} usable trades — need at least 30 to even "
                  f"attempt a fit. Let more trades accumulate first.")

    win_rate = y.mean()
    print(f"Win rate in this sample: {win_rate*100:.1f}%  "
          f"({int(y.sum())}W / {n - int(y.sum())}L)")
    if win_rate < 0.05 or win_rate > 0.95:
        sys.exit("Class balance too extreme to fit a meaningful classifier "
                  "(need both wins and losses well represented).")

    # Standardize
    means = X.mean(axis=0)
    stds  = X.std(axis=0)
    stds[stds == 0] = 1.0
    Xs = (X - means) / stds

    # Cross-validated honesty check BEFORE fitting on everything.
    n_splits = min(5, int(y.sum()), n - int(y.sum()))
    if n_splits < 2:
        sys.exit("Not enough of both classes for cross-validation yet.")
    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
    clf = LogisticRegression(C=0.5, max_iter=1000)
    aucs = []
    for tr_idx, te_idx in cv.split(Xs, y):
        clf.fit(Xs[tr_idx], y[tr_idx])
        proba = clf.predict_proba(Xs[te_idx])[:, 1]
        if len(set(y[te_idx])) > 1:
            aucs.append(roc_auc_score(y[te_idx], proba))
    mean_auc = float(np.mean(aucs)) if aucs else float("nan")

    print(f"\n{n_splits}-fold cross-validated AUC: {mean_auc:.3f}  "
          f"(0.50 = no better than chance, 1.00 = perfect separation)")
    if math.isnan(mean_auc):
        print("Could not compute AUC (degenerate folds) — treat with caution.")
    elif mean_auc < 0.55:
        print("This is barely better than chance. The existing hand-tuned "
              "heuristics likely aren't leaving much on the table for a "
              "simple linear model to find yet — that's a legitimate "
              "result, not a bug. More data or better features may help.")
    elif mean_auc < 0.65:
        print("Weak but real signal. Worth having in shadow mode, not "
              "worth trusting yet.")
    else:
        print("Meaningful signal. Still recommend a longer shadow-mode "
              "track record before ever letting this touch live trades.")

    # Final fit on all data for the version we might publish
    clf.fit(Xs, y)
    weights = {f: float(w) for f, w in zip(FEATURE_ORDER, clf.coef_[0])}
    bias = float(clf.intercept_[0])
    means_d = {f: float(m) for f, m in zip(FEATURE_ORDER, means)}
    stds_d  = {f: float(s) for f, s in zip(FEATURE_ORDER, stds)}

    print("\nLearned weights (standardized features — larger |weight| = "
          "more influence):")
    for f, w in sorted(weights.items(), key=lambda kv: -abs(kv[1])):
        print(f"  {f:<20} {w:+.3f}")

    if not args.publish:
        print("\n(Dry run — pass --publish to write this to Supabase.)")
        return

    if n < MIN_TRADES_TO_PUBLISH:
        print(f"\nNOT publishing: {n} trades is below the "
              f"{MIN_TRADES_TO_PUBLISH}-trade minimum this script enforces "
              f"before letting a model out even into shadow mode. "
              f"Re-run once you have more data.")
        return

    publish_model(url, key, weights, bias, means_d, stds_d, n, mean_auc)


if __name__ == "__main__":
    main()
