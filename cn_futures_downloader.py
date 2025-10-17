# -*- coding: utf-8 -*-
"""
cn_futures_downloader_stable_v2.py
---------------------------------
数据源: 东方财富 (ak.futures_zh_minute_em)
增强: 加入随机延迟 + 自动重试机制，避免频繁请求被拒绝
"""

import akshare as ak
import pandas as pd
import os
import time
import random
import argparse
from datetime import datetime, timedelta, timezone

# ================================
#      安全下载函数（增强版）
# ================================
def fetch_em_with_retry(symbol, freq, max_retries=3, delay_min=3, delay_max=8):
    """从东方财富抓取分钟数据，带随机延迟和重试机制"""
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.futures_zh_minute_em(symbol=symbol, period=freq)
            if df is not None and not df.empty:
                print(f"[OK] {symbol} fetched successfully on attempt {attempt} (rows={len(df)})")
                return df
            else:
                raise ValueError("Empty dataframe returned.")
        except Exception as e:
            if attempt < max_retries:
                wait_time = random.randint(delay_min, delay_max)
                print(f"[Retry] {symbol} failed (attempt {attempt}/{max_retries}), retrying in {wait_time}s... Error: {e}")
                time.sleep(wait_time)
            else:
                print(f"[Error] {symbol} failed after {max_retries} attempts: {e}")
                return pd.DataFrame()

# ================================
#         主函数逻辑
# ================================
def main():
    parser = argparse.ArgumentParser(description="Download CN Futures minute data with retry/delay")
    parser.add_argument("--symbols", required=True, help="Comma separated futures symbols, e.g., JM2601,M2601")
    parser.add_argument("--freq", default="60m", help="Data frequency (default: 60m)")
    parser.add_argument("--start", help="Start date (optional)")
    parser.add_argument("--end", help="End date (optional)")
    parser.add_argument("--out", default="./data", help="Output directory")
    parser.add_argument("--tz", default="Asia/Shanghai", help="Timezone")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    symbols = [s.strip() for s in args.symbols.split(",")]

    for symbol in symbols:
        print(f"[RUN] Fetching {symbol} from 东方财富 ({args.freq})")

        df = fetch_em_with_retry(symbol, args.freq)
        if df is None or df.empty:
            print(f"[WARN] No data fetched for {symbol}")
            continue

        # 确保关键列存在
        expected_cols = ["date", "open", "high", "low", "close", "volume", "open_interest"]
        for col in expected_cols:
            if col not in df.columns:
                if col == "open_interest":
                    df[col] = 0  # 若缺失则补0
                    print(f"[Fix] Added missing column: {col}")
                else:
                    print(f"[WARN] Missing column: {col}")

        out_path = os.path.join(args.out, f"{symbol}_{args.freq}.csv")
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Saved -> {out_path}  (rows={len(df)})")

        # 每个请求后增加随机延迟
        sleep_s = random.randint(3, 8)
        print(f"[Delay] Waiting {sleep_s}s before next symbol...\n")
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
