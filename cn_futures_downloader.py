
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
import akshare as ak  # Ensure AkShare is imported in each process

# Function to load configuration from config.yaml
def load_config(config_file="config.yaml"):
    with open(config_file, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config

# Function to convert datetime to specified timezone
def convert_to_timezone(dt, timezone="Asia/Shanghai"):
    local_tz = pytz.timezone(timezone)
    return dt.astimezone(local_tz)

def fetch_data_for_symbol(symbol, start, end, freq, output_path):
    try:
        print(f"[Start] Fetching {symbol}")
        importlib.reload(ak)  # Reload AkShare to ensure fresh session for each process
        
        # Fetch the data
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
    # Load configuration and timezone from config.yaml
    config = load_config()
    timezone = config.get("timezone", "Asia/Shanghai")  # Default to "Asia/Shanghai" if not found
    
    # Create the output directory if it doesn't exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Convert start and end times to the specified timezone
    start = convert_to_timezone(dtparse(start), timezone)
    end = convert_to_timezone(dtparse(end), timezone)
    
    # Create a pool of processes for parallel fetching
    with multiprocessing.Pool(processes=max_processes) as pool:
        tasks = [(symbol, start, end, freq, output_path) for symbol in symbols]
        pool.starmap(fetch_data_for_symbol, tasks)

def parse_args():
    parser = argparse.ArgumentParser(description="Download China futures history via AkShare and export normalized CSVs.")
    parser.add_argument("--symbols", required=True, help="Comma-separated list, e.g., RB0,CU0 or JM2601,RB2501")
    parser.add_argument("--freq", required=True, help="daily or one of: 1m,5m,15m,30m,60m")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD). Inclusive. Optional if --lookback is given.")
    parser.add_argument("--end", help="End date (YYYY-MM-DD). Inclusive. Defaults to today if omitted.")
    parser.add_argument("--lookback", type=int, help="Lookback days alternative to --start/--end (e.g., 60)")
    parser.add_argument("--out", default="./data", help="Output folder (default: ./data)")
    parser.add_argument("--tz", default="Asia/Shanghai", help="Timezone for output timestamps (default: Asia/Shanghai)")
    parser.add_argument("--sleep", type=float, default=0.8, help="Seconds to sleep between symbols (default 0.8)")
    parser.add_argument("--chunk-days", type=int, default=60, help="For minute data, fetch in chunks of N days (default 60)")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    # Use the command line tz argument, otherwise fall back to config.yaml
    tzname = args.tz if args.tz else load_config().get("timezone", "Asia/Shanghai")

    # Extract arguments
    symbols = args.symbols.split(',')
    start = args.start
    end = args.end
    freq = args.freq
    output_path = args.out

    # Fetch data for multiple symbols in parallel
    fetch_multiple_symbols(symbols, start, end, freq, output_path)
