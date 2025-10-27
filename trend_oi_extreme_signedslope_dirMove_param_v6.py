# -*- coding: utf-8 -*-
"""
Trend Detection by OI Extremes — v6 (Volatility Adaptive with Standard Floor)
------------------------------------------------------------------------------
Stable GitHub version
- Fully CLI parameterized, JSON config-compatible
- Safe for CI/CD (GitHub Actions, cron pipelines)
- Output: <input>_trend_v6.png  (e.g., docs/JM2601/JM2601_trend_v6.png)
------------------------------------------------------------------------------
New logic:
    slope_thresh = min(dynamic_slope, static_slope)
    move_thresh  = min(dynamic_move,  static_move)
Avoids over-filtering for high-volatility commodities (e.g. JM, I, RB).
"""

import argparse, os, numpy as np, pandas as pd, matplotlib.pyplot as plt

# ---------- Utility Functions ----------
def pct(a, b): return (b - a) / a if a != 0 else 0.0
def sign(x): return 1 if x > 0 else (-1 if x < 0 else 0)
def clean_columns(df): df.columns = [c.lower().strip().replace('\ufeff', '') for c in df.columns]; return df
def signed_mean_slope(pr): 
    r = np.diff(pr) / pr[:-1]
    return r.mean() if len(r) > 0 else 0.0

# ---------- Core Trend Detection ----------
def detect_trends_v6(prices, oi, alpha, min_move, min_slope, min_oi_delta, min_oi_up, min_bars, dir_ratio):
    n = len(prices)
    segs = []
    for s in range(0, n - min_bars):
        for e in range(s + min_bars, n):
            pr0 = prices[s:e+1]
            move0 = pct(prices[s], prices[e])
            dir0 = sign(move0)
            if dir0 == 0:
                continue

            slope0 = abs(signed_mean_slope(pr0))
            if slope0 < min_slope or dir0 * move0 < min_move:
                continue

            oi0 = oi[s:e+1]
            oi_delta0 = pct(oi0[0], oi0[-1])
            oi_up0 = (np.diff(oi0) > 0).mean() if len(oi0) > 1 else 0
            if oi_delta0 < min_oi_delta or oi_up0 < min_oi_up:
                continue

            # retracement
            if dir0 > 0:
                peak = np.max(pr0)
                retr = (peak - pr0[-1]) / (peak - pr0[0]) if (peak - pr0[0]) > 0 else 0.0
            else:
                trough = np.min(pr0)
                retr = (pr0[-1] - trough) / (pr0[0] - trough) if (pr0[0] - trough) > 0 else 0.0
            if retr > alpha:
                continue

            # OI min→max boundary
            s_oi = s + int(np.argmin(oi0))
            e_oi = s + int(np.argmax(oi[s_oi:e+1]))
            if e_oi - s_oi < min_bars:
                continue

            pr1 = prices[s_oi:e_oi+1]
            move1 = pct(prices[s_oi], prices[e_oi])
            dir1 = sign(move1)
            if dir1 == 0:
                continue

            dir_ratio_now = (np.diff(pr1) > 0).mean() if dir1 > 0 else (np.diff(pr1) < 0).mean()
            if dir_ratio_now < dir_ratio:
                continue

            slope1 = abs(signed_mean_slope(pr1))
            move1_dir = dir1 * move1
            oi_delta1 = pct(oi[s_oi], oi[e_oi])
            oi_up1 = (np.diff(oi[s_oi:e_oi+1]) > 0).mean() if (e_oi - s_oi) >= 1 else 0

            if move1_dir >= min_move and slope1 >= min_slope and oi_delta1 >= min_oi_delta and oi_up1 >= min_oi_up:
                segs.append({'s': s_oi, 'e': e_oi, 'dir': dir1})

    # filter overlapping
    segs = sorted(segs, key=lambda x: (x['s'], -(x['e'] - x['s'])))
    filtered = []
    for seg in segs:
        if not any(o['s'] <= seg['s'] <= o['e'] or o['s'] <= seg['e'] <= o['e'] for o in filtered):
            filtered.append(seg)
    return filtered

# ---------- CLI Entrypoint ----------
def main():
    parser = argparse.ArgumentParser(description="Trend Detection v6 (Volatility Adaptive with Standard Floor)")
    parser.add_argument("file", type=str, help="Input CSV file path")
    parser.add_argument("--alpha", type=float, default=0.382)
    parser.add_argument("--slope_ratio", type=float, default=0.2)
    parser.add_argument("--move_ratio", type=float, default=0.3)
    parser.add_argument("--slope", type=float, default=0.0015)
    parser.add_argument("--move", type=float, default=0.01)
    parser.add_argument("--delta_oi", type=float, default=0.01)
    parser.add_argument("--oi_up", type=float, default=0.55)
    parser.add_argument("--min_bars", type=int, default=6)
    parser.add_argument("--dir_ratio", type=float, default=0.55)
    parser.add_argument("--oi_ema_span", type=int, default=5)
    parser.add_argument("--use_dynamic_vol", action="store_true")
    args = parser.parse_args()

    # --- Load data ---
    df = pd.read_csv(args.file)
    df = clean_columns(df)
    df["bar"] = np.arange(len(df))

    prices = df["close"].astype(float).values
    oi_col = next((c for c in df.columns if any(k in c for k in ["open_interest","oi","position","hold","持仓"])), None)
    if oi_col is None:
        raise ValueError(f"No OI column found in {df.columns}")

    oi_raw = df[oi_col].astype(float).values
    oi = pd.Series(oi_raw).ewm(span=args.oi_ema_span, adjust=False).mean().values

    # --- Dynamic volatility normalization ---
    volatility_base = (prices.max() - prices.min()) / prices.min()
    n = len(prices)
    slope_dyn = volatility_base * args.slope_ratio / n
    move_dyn = volatility_base * args.move_ratio

    slope_thresh = min(slope_dyn, args.slope) if args.use_dynamic_vol else args.slope
    move_thresh = min(move_dyn, args.move) if args.use_dynamic_vol else args.move

    # --- Trend detection ---
    segs = detect_trends_v6(prices, oi, args.alpha, move_thresh, slope_thresh, args.delta_oi,
                            args.oi_up, args.min_bars, args.dir_ratio)

    # --- Plotting ---
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True, gridspec_kw={'height_ratios':[2,1]})
    axes[0].plot(df["bar"], prices, color='gray', lw=1.2)
    for seg in segs:
        c = 'green' if seg['dir'] == 1 else 'red'
        axes[0].plot(df["bar"].iloc[seg["s"]:seg["e"]+1], prices[seg["s"]:seg["e"]+1], color=c, lw=2.5)
    axes[0].set_title(f"Trend v6 (α={args.alpha}, slope≥{slope_thresh*100:.3f}%/bar, move≥{move_thresh*100:.2f}%, vol={volatility_base*100:.1f}%)", fontsize=13)
    axes[0].grid(True, ls='--', alpha=0.6)
    axes[0].set_ylabel("Close Price")

    axes[1].plot(df["bar"], oi_raw, color='blue', lw=1.0, alpha=0.5, label='OI (raw)')
    axes[1].plot(df["bar"], oi, color='blue', lw=1.6, label=f"OI (EMA{args.oi_ema_span})")
    for seg in segs:
        c = 'green' if seg['dir'] == 1 else 'red'
        axes[1].axvspan(df["bar"].iloc[seg["s"]], df["bar"].iloc[seg["e"]], color=c, alpha=0.15)
    axes[1].set_xlabel("Trading Bars (Sequential)")
    axes[1].set_ylabel("Open Interest")
    axes[1].legend()
    axes[1].grid(True, ls='--', alpha=0.6)

    plt.tight_layout()

    out = os.path.splitext(args.file)[0] + "_trend_v6.png"
    plt.savefig(out, dpi=300)
    print(f"✅ Trend plot saved to: {out}")
    print(f"Dynamic volatility={volatility_base:.4f} | slope_dyn={slope_dyn:.6f} | move_dyn={move_dyn:.4f}")
    print(f"Final thresholds => slope≥{slope_thresh:.6f} | move≥{move_thresh:.4f}")

if __name__ == "__main__":
    main()

