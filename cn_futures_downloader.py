# -*- coding: utf-8 -*-
"""
cn_futures_downloader.py — 东方财富多月分钟数据稳定版
-------------------------------------------------
✅ 支持多月历史数据
✅ 使用 ak.futures_zh_minute_em（替代新浪接口）
✅ 加入重试与延迟机制
✅ 最小侵入式修改，保持原版逻辑一致
"""

import os
import sys
import time
import datetime as dt
import pandas as pd
import akshare as ak


def ensure_akshare():
    try:
        import akshare
    except ImportError:
        print("[Error] akshare is not installed. Please run: pip install akshare")
        sys.exit(1)


def daterange(start_date, end_date):
    """生成日期范围（原逻辑保留，但东方财富接口不再按日请求）"""
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + dt.timedelta(n)


def fetch_minute_chunk(symbol: str, period_code: str) -> pd.DataFrame:
    """调用东方财富接口，支持多月历史"""
    ensure_akshare()
    backoffs = [3, 7, 15]
    for attempt, delay in enumerate(backoffs, start=1):
        try:
            df = ak.futures_zh_minute_em(symbol=symbol.upper(), period=period_code)
            if df is None or df.empty:
                raise RuntimeError("Empty dataframe returned")

            df.columns = [c.lower() for c in df.columns]
            df["symbol"] = symbol
            df["fetch_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[OK] {symbol} data fetched: {len(df)} rows")
            return df

        except Exception as e:
            print(f"[Retry] {symbol} {period_code} failed ({attempt}/{len(backoffs)}): {e}")
            if attempt < len(backoffs):
                print(f"[Backoff] Sleep {delay}s before retrying...")
                time.sleep(delay)
    raise RuntimeError(f"AkShare minute fetch failed for {symbol} after retries.")


def save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved -> {path}  (rows={len(df)})")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download CN futures minute data (东方财富接口)")
    parser.add_argument("--symbols", required=True, help="Comma separated symbols, e.g., JM2601,M2601,AL2511")
    parser.add_argument("--freq", default="60m", help="Frequency: 1m/5m/15m/30m/60m")
    parser.add_argument("--start", type=str, default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--out", type=str, default="./data", help="Output directory")
    parser.add_argument("--tz", type=str, default="Asia/Shanghai", help="Timezone")
    parser.add_argument("--sleep_between_symbols", type=int, default=5, help="Delay between symbols (s)")
    args = parser.parse_args()

    start_date = dt.datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
    end_date = dt.datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else dt.date.today()
    symbols = [s.strip() for s in args.symbols.split(",")]

    for i, symbol in enumerate(symbols):
        print(f"[RUN] Fetching {symbol} from 东方财富 ({args.freq})")

        try:
            period_code = args.freq.replace("m", "")
            df = fetch_minute_chunk(symbol, period_code)
            if df is not None and not df.empty:
                df = df.drop_duplicates(subset=["datetime"], keep="last")
                save_csv(df, os.path.join(args.out, f"{symbol}_{args.freq}.csv"))
            else:
                print(f"[WARN] No data fetched for {symbol}")

        except Exception as e:
            print(f"[WARN] {symbol} {args.freq}: {e}")

        if i < len(symbols) - 1:
            print(f"[Sleep] Wait {args.sleep_between_symbols}s before next symbol...")
            time.sleep(args.sleep_between_symbols)


if __name__ == "__main__":
    main()
