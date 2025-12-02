# -*- coding: utf-8 -*-
"""
asi_chipzones_plot_filtered_v3.8.2_hybrid.py
---------------------------------------------
- 中文安全字体
- 筹码区标注左侧、单行 "区间 强度: X.X"
- CSV 输出精度控制
- 输出 chipzones 图，并自动清空 docs/<symbol>/ 下所有旧文件
"""

import os
import argparse
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib import font_manager

# =============================
# 中文字体支持
# =============================
font_candidates = [
    "/System/Library/Fonts/PingFang.ttc",
    "/Library/Fonts/Arial Unicode.ttf",
    "/System/Library/Fonts/STHeiti Light.ttc",
    "C:/Windows/Fonts/msyh.ttc",
    "C:/Windows/Fonts/simhei.ttf",
    "/usr/share/fonts/truetype/arphic/ukai.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
]

for f in font_candidates:
    if os.path.exists(f):
        try:
            font_manager.fontManager.addfont(f)
        except:
            pass

plt.rcParams["font.sans-serif"] = [
    "Arial Unicode MS", "PingFang SC", "Microsoft YaHei",
    "SimHei", "Heiti TC", "Noto Sans CJK SC", "Source Han Sans CN"
]
plt.rcParams["axes.unicode_minus"] = False


# =============================
# 删除 docs/<symbol>/ 下所有旧文件
# =============================
def delete_all_files(symbol):
    save_dir = f"docs/{symbol}"
    os.makedirs(save_dir, exist_ok=True)
    for file in os.listdir(save_dir):
        file_path = os.path.join(save_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"[INFO] 删除文件：{file_path}")


# =============================
# 计算强度
# =============================
def calc_accum_strength(df, window_strength=20, smooth=7):
    df = df.copy()
    df['ret'] = df['close'].pct_change().fillna(0.0)

    vol_roll = df['volume'].rolling(window_strength).mean().replace(0, np.nan)
    df['vol_norm'] = (df['volume'] / vol_roll).fillna(0.0)

    long_raw = df['ret'].rolling(window_strength).apply(
        lambda x: np.sum(np.clip(x, 0, None)) / (np.sum(np.abs(x)) + 1e-9), raw=True
    )
    short_raw = df['ret'].rolling(window_strength).apply(
        lambda x: np.sum(np.clip(-x, 0, None)) / (np.sum(np.abs(x)) + 1e-9), raw=True
    )

    df['long_strength'] = (long_raw * df['vol_norm']).ewm(span=smooth, adjust=False).mean()
    df['short_strength'] = (short_raw * df['vol_norm']).ewm(span=smooth, adjust=False).mean()

    for col in ['long_strength', 'short_strength']:
        s = df[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        lo, hi = s.quantile(0.02), s.quantile(0.98)
        rng = hi - lo if hi > lo else 1.0
        df[col] = ((s - lo) / rng).clip(0, 1)

    return df


# =============================
# 筹码密集区
# =============================
def estimate_chipzones(df, window_zone=60, bins_pct=0.5, beta=0.7, half_life=10, quantile=0.8):
    df = df.copy()
    df['delta_oi'] = df['open_interest'].diff().clip(lower=0).fillna(0.0)
    df['days_from_end'] = np.arange(len(df))[::-1] / 24.0
    decay = np.exp(-np.log(2) * df['days_from_end'] / max(half_life, 1e-6))
    df['weight'] = df['volume'] * np.power(1.0 + df['delta_oi'], beta) * decay

    mean_price = df['close'].mean()
    bin_w = max(mean_price * (bins_pct / 100.0), 1e-6)
    price_bins = np.arange(df['close'].min(), df['close'].max() + bin_w, bin_w)

    def strength_hist(sub_df):
        hist, _ = np.histogram(sub_df['close'], bins=price_bins, weights=sub_df['weight'])
        return hist / np.max(hist) if np.max(hist) > 0 else hist

    h_recent = strength_hist(df.tail(window_zone))
    h_all = strength_hist(df)
    rr = h_recent[h_recent > 0]
    aa = h_all[h_all > 0]

    thr_recent = np.quantile(rr, quantile) if rr.size else 1.0
    thr_all = np.quantile(aa, quantile) if aa.size else 1.0

    lows, highs = price_bins[:-1], price_bins[1:]
    rows = []

    for i in range(len(lows)):
        rec = h_recent[i] >= thr_recent if rr.size else False
        all_ = h_all[i] >= thr_all if aa.size else False
        if rec or all_:
            avg = (h_recent[i] + h_all[i]) / 2.0
            rows.append(dict(
                low=lows[i], high=highs[i], recent_strength=h_recent[i],
                all_strength=h_all[i], avg_strength=avg,
                persistent=bool(rec and all_), zone_type='持久区' if rec and all_ else '短期区'
            ))
    return pd.DataFrame(rows)


# =============================
# 绘制 chipzones 图（唯一输出）
# =============================
def plot_chart(df, zones, symbol):
    x = np.arange(len(df))
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(14, 8), sharex=True,
        gridspec_kw={'height_ratios': [2, 1]}
    )

    # 价格线
    ax1.plot(x, df['close'], color='black', lw=1.2)

    # 区间渲染
    for _, r in zones.iterrows():
        color = '#FF8A33' if r['zone_type'] == '短期区' else '#CC5522'
        ax1.axhspan(r['low'], r['high'], color=color, alpha=0.25)

    # 成交量
    ax2.bar(df.index, df["volume"], color="gray")
    ax3 = ax2.twinx()
    ax3.plot(df.index, df["open_interest"], color="blue")

    plt.tight_layout()

    # 删除旧文件
    delete_all_files(symbol)

    # 时间戳
    ts = datetime.now().strftime("%Y%m%d_%H")

    save_dir = f"docs/{symbol}"
    os.makedirs(save_dir, exist_ok=True)

    out_png = f"{save_dir}/{symbol}_chipzones_hybrid_{ts}.png"
    plt.savefig(out_png, dpi=300)
    plt.close(fig)

    print(f"[OK] Chipzones 图像已保存：{out_png}")


# =============================
# CSV 高亮
# =============================
def highlight_csv(df, thresholds):
    for c in ['recent_strength', 'all_strength', 'avg_strength']:
        q = thresholds[c]
        df[c] = df[c].apply(lambda v: f'强：{v:.1f}' if v >= q else f'{v:.1f}')
    df['low'] = df['low'].round(0).astype(int)
    df['high'] = df['high'].round(0).astype(int)
    return df


# =============================
# 主函数
# =============================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--window_strength", type=int, default=20)
    parser.add_argument("--window_zone", type=int, default=60)
    parser.add_argument("--bins_pct", type=float, default=0.5)
    parser.add_argument("--beta", type=float, default=0.7)
    parser.add_argument("--half_life", type=float, default=10)
    parser.add_argument("--quantile", type=float, default=0.8)
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    df.columns = [c.lower() for c in df.columns]

    if 'date' in df.columns and 'time' in df.columns:
        df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
    elif 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    for c in ['close', 'volume', 'open_interest']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    df = df.dropna(subset=['close', 'volume', 'open_interest'])
    df = df[df['volume'] > 0].reset_index(drop=True)

    df = calc_accum_strength(df, args.window_strength)
    zones = estimate_chipzones(df, args.window_zone, args.bins_pct, args.beta, args.half_life, args.quantile)

    symbol = os.path.splitext(os.path.basename(args.csv))[0].split('_')[0].upper()
    plot_chart(df, zones, symbol)

    if not zones.empty:
        thresholds = {c: zones[c].quantile(0.8) for c in [
            'recent_strength', 'all_strength', 'avg_strength'
        ]}
        zones_fmt = highlight_csv(zones.copy(), thresholds)
        csv_cols = ['low', 'high', 'recent_strength',
                    'all_strength', 'avg_strength', 'persistent', 'zone_type']
        zones_fmt.to_csv(f"{symbol}_chipzones_hybrid.csv",
                         index=False, encoding='utf-8-sig', columns=csv_cols)

        print(f"[OK] 数据表已保存：{symbol}_chipzones_hybrid.csv")


if __name__ == "__main__":
    main()
