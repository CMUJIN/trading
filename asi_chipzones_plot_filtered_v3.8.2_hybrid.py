# -*- coding: utf-8 -*-
"""
asi_chipzones_plot_filtered_v3.8.2_hybrid.py
---------------------------------------------
- 中文安全字体
- 筹码区标注左侧、单行 "区间 强度: X.X"
- CSV 输出精度控制与前20%红色高亮 (<font color="red">x.x</font>)
"""

import os
import argparse
import numpy as np
import pandas as pd
# =============================
# ✅ 解决中文乱码
# =============================
# ===== 中文字体设置（兼容 macOS / Windows / Linux） =====
import matplotlib
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib import font_manager

# 优先使用系统可用字体（Arial Unicode MS 支持全语言字符）
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
        except Exception:
            pass

plt.rcParams["font.sans-serif"] = [
    "Arial Unicode MS",  # ✅ 最兼容
    "PingFang SC",
    "Microsoft YaHei",
    "SimHei",
    "Heiti TC",
    "Noto Sans CJK SC",
    "Source Han Sans CN",
]
plt.rcParams["axes.unicode_minus"] = False


font_path = None
for f in font_candidates:
    if os.path.exists(f) or f in matplotlib.font_manager.findSystemFonts(fontpaths=None, fontext='ttf'):
        font_path = f
        break

if font_path:
    try:
        matplotlib.font_manager.fontManager.addfont(font_path)
        font_name = matplotlib.font_manager.FontProperties(fname=font_path).get_name()
        matplotlib.rcParams['font.sans-serif'] = [font_name]
        print(f"[INFO] ✅ 使用字体: {font_name}")
    except Exception as e:
        print(f"[WARN] 字体加载失败: {e}")
else:
    matplotlib.rcParams['font.sans-serif'] = ['Noto Sans CJK SC', 'SimHei', 'Microsoft YaHei']
    print("[WARN] 未找到指定字体, 尝试系统默认中文字体")

matplotlib.rcParams['axes.unicode_minus'] = False  # 修复负号显示问题

from matplotlib import font_manager

def setup_chinese_font():
    plt.rcParams['font.sans-serif'] = ['PingFang SC','Microsoft YaHei','SimHei','Heiti TC','Noto Sans CJK SC','Source Han Sans CN','Arial Unicode MS','DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        os.path.expandvars(r"C:\Windows\Fonts\msyh.ttc"),
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    ]
    for p in candidates:
        try:
            if os.path.exists(p):
                font_manager.fontManager.addfont(p)
        except Exception:
            pass
setup_chinese_font()

def calc_accum_strength(df, window_strength=20, smooth=7):
    df = df.copy()
    df['ret'] = df['close'].pct_change().fillna(0.0)
    vol_roll = df['volume'].rolling(window_strength).mean().replace(0, np.nan)
    df['vol_norm'] = (df['volume'] / vol_roll).fillna(0.0)
    long_raw = df['ret'].rolling(window_strength).apply(lambda x: np.sum(np.clip(x,0,None))/(np.sum(np.abs(x))+1e-9), raw=True)
    short_raw = df['ret'].rolling(window_strength).apply(lambda x: np.sum(np.clip(-x,0,None))/(np.sum(np.abs(x))+1e-9), raw=True)
    df['long_strength'] = (long_raw * df['vol_norm']).ewm(span=smooth, adjust=False).mean()
    df['short_strength'] = (short_raw * df['vol_norm']).ewm(span=smooth, adjust=False).mean()
    for col in ['long_strength','short_strength']:
        s = df[col].replace([np.inf,-np.inf],np.nan).fillna(0.0)
        lo, hi = s.quantile(0.02), s.quantile(0.98)
        rng = hi-lo if hi>lo else 1.0
        df[col] = ((s-lo)/rng).clip(0,1)
    return df

def estimate_chipzones(df, window_zone=60, bins_pct=0.5, beta=0.7, half_life=10, quantile=0.8):
    df = df.copy()
    df['delta_oi'] = df['open_interest'].diff().clip(lower=0).fillna(0.0)
    df['days_from_end'] = np.arange(len(df))[::-1]/24.0
    decay = np.exp(-np.log(2)*df['days_from_end']/max(half_life,1e-6))
    df['weight'] = df['volume']*np.power(1.0+df['delta_oi'],beta)*decay
    mean_price = df['close'].mean()
    bin_w = max(mean_price*(bins_pct/100.0),1e-6)
    price_bins = np.arange(df['close'].min(),df['close'].max()+bin_w,bin_w)
    def strength_hist(sub_df):
        hist,_=np.histogram(sub_df['close'],bins=price_bins,weights=sub_df['weight'])
        if np.max(hist)>0: hist=hist/np.max(hist)
        return hist
    sub_recent=df.tail(window_zone)
    h_recent=strength_hist(sub_recent)
    h_all=strength_hist(df)
    rr=h_recent[h_recent>0]; aa=h_all[h_all>0]
    thr_recent=np.quantile(rr,quantile) if rr.size else 1.0
    thr_all=np.quantile(aa,quantile) if aa.size else 1.0
    lows,highs=price_bins[:-1],price_bins[1:]
    rows=[]
    for i in range(len(lows)):
        rec=h_recent[i]>=thr_recent if rr.size else False
        all_=h_all[i]>=thr_all if aa.size else False
        if rec or all_:
            avg=(h_recent[i]+h_all[i])/2.0
            rows.append(dict(low=lows[i],high=highs[i],recent_strength=h_recent[i],all_strength=h_all[i],
                             avg_strength=avg,persistent=bool(rec and all_),zone_type='持久区' if rec and all_ else '短期区'))
    return pd.DataFrame(rows)

# === 自动删除目录下所有文件 ===
def delete_all_files(symbol):
    save_dir = f"docs/{symbol}"
    os.makedirs(save_dir, exist_ok=True)

    # 删除目录下所有文件
    for file in os.listdir(save_dir):
        file_path = os.path.join(save_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"[INFO] 删除文件：{file_path}")

def plot_chart(df, zones, symbol):
    x = np.arange(len(df))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
    ax1.plot(x, df['close'], color='black', lw=1.2, label='price')

    for _, r in zones.iterrows():
        color = '#FF8A33' if r['zone_type'] == '短期区' else '#CC5522'
        ax1.axhspan(r['low'], r['high'], color=color, alpha=0.25)

    if not zones.empty:
        zones = zones.sort_values('low')
        xmin = x[0]
        offsets = {}
        for _, r in zones.iterrows():
            mid = (r['low'] + r['high']) / 2.0
            key = round(mid, -1)
            offsets[key] = offsets.get(key, 0) + 1
            dy = (offsets[key] - 1) * 0.0
            txt = f"{int(round(r['low']))}-{int(round(r['high']))} strength:{r['avg_strength']:.1f}"
            color = '#C24E1A' if r['zone_type'] == 'persistent_zone' else '#B35A00'
            ax1.text(xmin, mid + dy, txt, fontsize=8, color=color, va='center', ha='left')

    ax1.set_title(f"{symbol} chip_analysis (v3.8.2 hybrid+)", fontsize=12)

    # 📊 副图：成交量 + 持仓量（双轴）
    ax2.bar(df.index, df["volume"], color="gray", label="volume")
    ax2.set_ylabel("成交量", color="gray")
    ax2.tick_params(axis='y', labelcolor='gray')

    ax3 = ax2.twinx()
    ax3.plot(df.index, df["open_interest"], color="blue", label="OI")
    ax3.set_ylabel("OI", color="blue")
    ax3.tick_params(axis='y', labelcolor='blue')

    # 自动设置持仓量上下限
    oi_min = df["open_interest"].min()
    oi_max = df["open_interest"].max()
    margin = (oi_max - oi_min) * 0.05
    ax3.set_ylim(oi_min - margin, oi_max + margin)

    ax3.legend(loc="upper right")

    if 'datetime' in df.columns:
        step = max(1, len(df) // 10)
        ax2.set_xticks(x[::step])
        ax2.set_xticklabels(df['datetime'].dt.strftime('%m-%d %H:%M')[::step], rotation=30, ha='right')

    plt.tight_layout()

    # === 使用小时级别的时间戳（YYYYMMDD_HH） ===
    ts = datetime.now().strftime("%Y%m%d_%H")

    # 删除目录下所有文件
    delete_all_files(symbol)

    # 确保 docs/<symbol>/ 目录存在
    save_dir = f"docs/{symbol}"
    os.makedirs(save_dir, exist_ok=True)

    # 输出文件名：symbol_trend_v6_YYYYMMDD_HH.png
    trend_out_png = f"{save_dir}/{symbol}_trend_v6_{ts}.png"

    plt.savefig(trend_out_png, dpi=300)
    plt.close(fig)

    print(f"[OK] Trend_v6 图像已保存：{trend_out_png}")

def highlight_csv(df, thresholds):
    for c in ['recent_strength', 'all_strength', 'avg_strength']:
        q = thresholds[c]
        df[c] = df[c].apply(lambda v: f'强：{v:.1f}' if v >= q else f'{v:.1f}')
    df['low'] = df['low'].round(0).astype(int)
    df['high'] = df['high'].round(0).astype(int)
    return df

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
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), errors='coerce')
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
        thresholds = {c: zones[c].quantile(0.8) for c in ['recent_strength', 'all_strength', 'avg_strength']}
        zones_fmt = highlight_csv(zones.copy(), thresholds)
        csv_cols = ['low', 'high', 'recent_strength', 'all_strength', 'avg_strength', 'persistent', 'zone_type']
        zones_fmt.to_csv(f"{symbol}_chipzones_hybrid.csv", index=False, encoding='utf-8-sig', columns=csv_cols)
        print(f"[OK] 数据表已保存：{symbol}_chipzones_hybrid.csv")

if __name__ == "__main__":
    main()
