import argparse
import os
import sys
import time
import random
import importlib
from datetime import datetime
from dateutil.parser import parse as dtparse
import traceback
import pandas as pd
import pytz
import yaml
import akshare as ak

def load_config(config_file="config.yaml"):
    with open(config_file, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config

def convert_to_timezone(dt, timezone="Asia/Shanghai"):
    local_tz = pytz.timezone(timezone)
    return dt.astimezone(local_tz)

def fetch_data_for_symbol(symbol, start, end, freq, output_path):
    try:
        print(f"[Start] Fetching {symbol}")
        importlib.reload(ak)
        data = ak.futures_zh_minute_sina(symbol=symbol, start_date=start, end_date=end, freq=freq)
        
        if data is None or len(data) == 0:
            print(f"[Warn] {symbol}: Data fetch failed or empty response")
            return
        
        output_file = os.path.join(output_path, f"{symbol}_{start}_{end}_{freq}.csv")
        data.to_csv(output_file, index=False)
        print(f"[Success] {symbol}: Data saved to {output_file}")
        
    except Exception as e:
        print(f"[Error] {symbol}: {e}")
        traceback.print_exc()

def fetch_multiple_symbols(symbols, start, end, freq, output_path, max_processes=4):
    config = load_config()
    timezone = config.get("timezone", "Asia/Shanghai")
    
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    start = convert_to_timezone(dtparse(start), timezone)
    end = convert_to_timezone(dtparse(end), timezone)
    
    with multiprocessing.Pool(processes=max_processes) as pool:
        tasks = [(symbol, start, end, freq, output_path) for symbol in symbols]
        pool.starmap(fetch_data_for_symbol, tasks)

def parse_args():
    parser = argparse.ArgumentParser(description="Download futures data via AkShare with multiprocessing")
    parser.add_argument("--symbols", type=str, required=True, help="Comma-separated list of symbols (e.g., JM2601,M2501)")
    parser.add_argument("--start", type=str, required=True, help="Start date in format YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="End date in format YYYY-MM-DD")
    parser.add_argument("--freq", type=str, required=True, help="Data frequency (e.g., 1m, 5m, 15m, 60m)")
    parser.add_argument("--out", type=str, required=True, help="Output directory for CSV files")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    symbols = args.symbols.split(',')
    start = args.start
    end = args.end
    freq = args.freq
    output_path = args.out
    fetch_multiple_symbols(symbols, start, end, freq, output_path)
