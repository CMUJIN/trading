import os
import time
import random
import argparse
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta, timezone

# ==============================================================
#  带自动重试与随机延迟的稳定版本 cn_futures_downloader.py
# ==============================================================

def safe_fetch(symbol: str, freq: str, date: str = None, max_retries: int = 3):
    """带延迟与重试机制的安全抓取函数"""
    for attempt in range(max_retries):
        try:
            df = ak.futures_zh_minute_sina(symbol=symbol, period=freq)
            if df is not None and not df.empty:
                return df
            else:
                print(f"[Warn] {symbol} {freq} {date}: Empty or None, retry {attempt+1}/{max_retries}")
        except Exception as e:
            print(f"[Warn] {symbol} {freq} {date}: {e}, retry {attempt+1}/{max_retries}")
        time.sleep(random.uniform(1.0, 3.0))  # 随机延迟避免封禁
    print(f"[Error] {symbol} {freq} {date}: Max retries reached, skip.")
    return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description="Download CN futures minute data")
    parser.add_argument("--symbols", type=str, required=True, help="Comma separated futures symbols")
    parser.add_argument("--freq", type=str, default="60m", help="Frequency, e.g. 60m")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)")
    parser.add_argument("--out", type=str, default="./data", help="Output directory")
    parser.add_argument("--tz", type=str, default="Asia/Shanghai", help="Timezone")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    symbols = args.symbols.split(",")

    for symbol in symbols:
        all_data = []
        print(f"[RUN] Fetching {symbol} from {args.start} to {args.end} ({args.freq})")

        df = safe_fetch(symbol, args.freq, args.start)
        if df.empty:
            print(f"[Warn] No data for {symbol}, skip.")
            continue

        # 数据保存
        out_path = os.path.join(args.out, f"{symbol}_{args.freq}.csv")
        df.to_csv(out_path, index=False)
        print(f"[OK] Saved -> {out_path}  (rows={len(df)})")

        # 抓取间歇延迟，防止触发限流
        time.sleep(random.uniform(2.0, 5.0))


if __name__ == "__main__":
    main()
