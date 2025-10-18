#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
China Futures History Downloader (AkShare-based) - stable version with delay & retry
-------------------------------------------------------------------------------------
✅ 保留原始数据抓取逻辑 (使用 futures_zh_minute_sina)
✅ 增加重试机制：指数退避 3 次（3s -> 9s -> 27s）
✅ 增加品种间延迟：每个品种抓取完后随机延迟 5–10 秒
"""

import argparse
import os
import sys
import time
import random
from datetime import datetime, timedelta, date
from dateutil.parser import parse as dtparse
import pandas as pd

try:
    import akshare as ak
except Exception:
    ak = None

SUPPORTED_MINUTE_FREQS = {"1m": "1", "5m": "5", "15m": "15", "30m": "30", "60m": "60"}

# ======================================================
# 核心函数：与原版完全一致，只增加重试机制
# ======================================================

def ensure_akshare():
    if ak is None:
        print("[Error] akshare is not installed. Please run: pip install akshare", file=sys.stderr)
        sys.exit(1)

def parse_args():
    p = argparse.ArgumentParser(description="Download China futures history via AkShare and export normalized CSVs.")
    p.add_argument("--symbols", required=True)
    p.add_argument("--freq", required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--lookback", type=int)
    p.add_argument("--out", default="./data")
    p.add_argument("--tz", default="Asia/Shanghai")
    p.add_argument("--sleep", type=float, default=0.8)
    p.add_argument("--chunk-days", type=int, default=60)
    return p.parse_args()

def normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for c in df.columns:
        cl = c.lower()
        if cl in ("日期", "date"): rename_map[c] = "date"
        elif cl in ("开盘价", "open"): rename_map[c] = "open"
        elif cl in ("最高价", "high"): rename_map[c] = "high"
        elif cl in ("最低价", "low"): rename_map[c] = "low"
        elif cl in ("收盘价", "close"): rename_map[c] = "close"
        elif cl in ("成交量", "volume"): rename_map[c] = "volume"
        elif cl in ("持仓量", "hold", "oi", "open_interest"): rename_map[c] = "open_interest"

    df = df.rename(columns=rename_map)
    for col in ["date", "open", "high", "low", "close"]:
        if col not in df.columns:
            raise ValueError(f"Daily data missing required column: {col}")

    if "volume" not in df.columns: df["volume"] = None
    if "open_interest" not in df.columns: df["open_interest"] = None

    df["time"] = ""
    df = df[["date", "time", "open", "high", "low", "close", "volume", "open_interest"]].copy()
    for col in ["open", "high", "low", "close"]: df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df.drop_duplicates().sort_values(["date"]).reset_index(drop=True)
    return df

def normalize_minute(df: pd.DataFrame, tzname: str) -> pd.DataFrame:
    rename_map = {}
    for c in df.columns:
        cl = c.lower()
        if cl in ("datetime", "时间", "时间戳", "date", "日期"): rename_map[c] = "dt"
        elif cl in ("open", "开盘价"): rename_map[c] = "open"
        elif cl in ("high", "最高价"): rename_map[c] = "high"
        elif cl in ("low", "最低价"): rename_map[c] = "low"
        elif cl in ("close", "收盘价"): rename_map[c] = "close"
        elif cl in ("volume", "成交量"): rename_map[c] = "volume"
        elif cl in ("oi", "hold", "open_interest", "持仓量"): rename_map[c] = "open_interest"

    df = df.rename(columns=rename_map)
    if "dt" not in df.columns:
        if "date" in df.columns and "time" in df.columns:
            df["dt"] = df["date"].astype(str) + " " + df["time"].astype(str)
        else:
            raise ValueError("Minute data is missing a datetime column.")

    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    df = df.dropna(subset=["dt"]).copy()
    df["date"] = df["dt"].dt.strftime("%Y-%m-%d")
    df["time"] = df["dt"].dt.strftime("%H:%M")

    for col in ["open", "high", "low", "close", "volume", "open_interest"]:
        if col not in df.columns: df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    out = df[["date", "time", "open", "high", "low", "close", "volume", "open_interest"]].copy()
    out = out.drop_duplicates().sort_values(["date", "time"]).reset_index(drop=True)
    return out

def fetch_daily(symbol: str) -> pd.DataFrame:
    ensure_akshare()
    df = ak.futures_zh_daily_sina(symbol=symbol.upper())
    if df is None or df.empty:
        raise ValueError(f"No daily data returned by AkShare for symbol {symbol}")
    return normalize_daily(df)

def daterange(start_date: date, end_date: date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

# ✅ 关键修改：加入指数退避重试
def fetch_minute_chunk(symbol: str, period_code: str, day_str: str) -> pd.DataFrame:
    ensure_akshare()
    backoffs = [3, 9, 27]
    for attempt, delay in enumerate(backoffs, start=1):
        try:
            df = ak.futures_zh_minute_sina(symbol=symbol.upper(), period=period_code, date=day_str)
            if df is not None and not df.empty:
                df["source_date"] = day_str
                return df
            else:
                raise RuntimeError("Empty dataframe returned.")
        except Exception as e:
            print(f"[Retry] {symbol} {period_code} {day_str} failed ({attempt}/{len(backoffs)}): {e}", file=sys.stderr)
            if attempt < len(backoffs):
                print(f"[Backoff] Sleep {delay}s before retrying...", file=sys.stderr)
                time.sleep(delay)
    raise RuntimeError(f"AkShare minute fetch failed for {symbol} {day_str} after retries.")

def fetch_minute(symbol: str, freq: str, start: date, end: date, tzname: str, chunk_days: int = 60) -> pd.DataFrame:
    period_code = SUPPORTED_MINUTE_FREQS[freq]
    all_frames = []
    cur_start = start
    while cur_start <= end:
        cur_end = min(cur_start + timedelta(days=chunk_days - 1), end)
        for d in daterange(cur_start, cur_end):
            day_str = d.strftime("%Y-%m-%d")
            try:
                df = fetch_minute_chunk(symbol, period_code, day_str)
                if df is not None and not df.empty:
                    all_frames.append(df)
            except Exception as e:
                print(f"[Warn] {symbol} {freq} {day_str}: {e}", file=sys.stderr)
                continue
        cur_start = cur_end + timedelta(days=1)

    if not all_frames:
        raise ValueError(f"No minute data retrieved for {symbol} {freq} between {start} and {end}.")

    raw = pd.concat(all_frames, ignore_index=True)
    norm = normalize_minute(raw, tzname=tzname)
    return norm

def save_csv(df: pd.DataFrame, out_dir: str, symbol: str, freq: str):
    os.makedirs(out_dir, exist_ok=True)
    fname = f"{symbol.upper()}_{freq}.csv"
    path = os.path.join(out_dir, fname)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[OK] Saved -> {path}  (rows={len(df)})")
    return path

# ======================================================
# 主程序 (仅增加品种间随机延迟)
# ======================================================
def main():
    args = parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        print("[Error] No symbols parsed from --symbols", file=sys.stderr)
        sys.exit(2)

    freq = args.freq.strip().lower()
    if freq != "daily" and freq not in SUPPORTED_MINUTE_FREQS:
        print(f"[Error] Unsupported --freq {freq}.", file=sys.stderr)
        sys.exit(2)

    today = datetime.now().date()
    if args.lookback:
        start_date = today - timedelta(days=int(args.lookback))
        end_date = today
    else:
        start_date = dtparse(args.start).date() if args.start else (date(2010,1,1) if freq == "daily" else today - timedelta(days=60))
        end_date = dtparse(args.end).date() if args.end else today

    any_paths = []
    for i, sym in enumerate(symbols, 1):
        try:
            if freq == "daily":
                df = fetch_daily(sym)
                df["date_dt"] = pd.to_datetime(df["date"])
                mask = (df["date_dt"].dt.date >= start_date) & (df["date_dt"].dt.date <= end_date)
                out_df = df.loc[mask].drop(columns=["date_dt"]).reset_index(drop=True)
            else:
                out_df = fetch_minute(sym, freq, start=start_date, end=end_date, tzname=args.tz, chunk_days=args.chunk_days)
                out_df["datetime"] = pd.to_datetime(out_df["date"] + " " + out_df["time"])
                start_dt = pd.to_datetime(f"{start_date} 00:00:00")
                end_dt = pd.to_datetime(f"{end_date} 23:59:59")
                out_df = out_df[(out_df["datetime"] >= start_dt) & (out_df["datetime"] <= end_dt)]
                out_df = out_df.drop(columns=["datetime"]).drop_duplicates().reset_index(drop=True)

            path = save_csv(out_df, args.out, sym, freq)
            any_paths.append(path)
            time.sleep(max(0.0, args.sleep))

            # ✅ 新增：每个品种抓取完后随机延迟
            if i < len(symbols):
                delay = random.randint(5, 10)
                print(f"[Delay] Finished {sym}, wait {delay}s before next symbol...\n")
                time.sleep(delay)

        except Exception as e:
            print(f"[Error] {sym}: {e}", file=sys.stderr)

    if not any_paths:
        print("[Error] No files were saved. Please check symbols, freq, and date range.", file=sys.stderr)
        sys.exit(3)

if __name__ == "__main__":
    main()
