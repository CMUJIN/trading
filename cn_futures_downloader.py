# -*- coding: utf-8 -*-
"""
cn_futures_downloader_stable_delay.py
在原稳定版本基础上，仅增加：
✅ 每个品种抓取完后随机延迟 3–8 秒，避免被接口限流。
其他逻辑完全不变。
"""

import akshare as ak
import pandas as pd
import os
import time
import random
import argparse
from datetime import datetime, timedelta, timezone

def main():
    parser = argparse.ArgumentParser(description="Download CN Futures minute data (stable with delay)")
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

        try:
            df = ak.futures_zh_minute_em(symbol=symbol, period=args.freq)
            if df is None or df.empty:
                print(f"[WARN] {symbol} returned empty dataframe.")
                continue

            # 检查并补全必要列
            expected_cols = ["date", "open", "high", "low", "close", "volume", "open_interest"]
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = 0
                    print(f"[Fix] Added missing column: {col}")

            out_path = os.path.join(args.out, f"{symbol}_{args.freq}.csv")
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"[OK] Saved -> {out_path}  (rows={len(df)})")

        except Exception as e:
            print(f"[Error] Failed to fetch {symbol}: {e}")

        # ✅ 新增延迟逻辑
        delay = random.randint(3, 8)
        print(f"[Delay] 等待 {delay}s 后继续下一个品种...\n")
        time.sleep(delay)


if __name__ == "__main__":
    main()
