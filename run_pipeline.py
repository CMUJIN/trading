#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, subprocess, argparse, glob, datetime as dt, yaml, pathlib, shutil

BASE = pathlib.Path(__file__).resolve().parent
DOWNLOADER = str(BASE / "cn_futures_downloader.py")
ANALYZER   = str(BASE / "asi_chipzones_plot_filtered_v3.8.2_hybrid.py")

def run(cmd, cwd=None):
    print("[RUN]", " ".join(map(str, cmd)), flush=True)
    p = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if p.stdout: print(p.stdout)
    if p.returncode != 0:
        print(p.stderr, file=sys.stderr)
        raise SystemExit(p.returncode)
    return p

def load_cfg(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def latest_file(pattern):
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    symbols = cfg["symbols"]
    freq    = cfg["freq"]
    out_dir = os.path.abspath(os.path.join(BASE, cfg.get("out_dir","./data")))
    tz      = cfg.get("timezone","Asia/Shanghai")
    ana     = cfg.get("analysis", {})
    ensure_dir(out_dir)

    pages_root = os.path.abspath(os.path.join(BASE, "docs"))
    ensure_dir(pages_root)

    today = dt.date.today().strftime("%Y-%m-%d")

    for item in symbols:
        sym   = item["code"]
        start = str(item["start"])
        end   = today

        # 1) download
        dl_cmd = [
            sys.executable, DOWNLOADER,
            "--symbols", sym,
            "--freq", freq,
            "--start", start,
            "--end", end,
            "--out", out_dir,
            "--tz", tz,
        ]
        try:
            run(dl_cmd)
        except SystemExit:
            print(f"[WARN] Downloader failed for {sym}, continue next.", file=sys.stderr)
            continue

        # 2) analyze
        csv_in = latest_file(os.path.join(out_dir, f"*{sym}*{freq}*.csv")) or latest_file(os.path.join(out_dir, f"*{sym}*.csv"))
        if not csv_in:
            print(f"[WARN] No CSV for {sym}. Skipping analysis.")
            continue

        ana_cmd = [
            sys.executable, ANALYZER,
            "--csv", csv_in,
            "--window_strength", str(ana.get("window_strength",20)),
            "--window_zone",     str(ana.get("window_zone",60)),
            "--bins_pct",        str(ana.get("bins_pct",0.5)),
            "--beta",            str(ana.get("beta",0.7)),
            "--half_life",       str(ana.get("half_life",10)),
            "--quantile",        str(ana.get("quantile",0.8)),
        ]
        try:
            run(ana_cmd, cwd=str(BASE))
        except SystemExit:
            print(f"[WARN] Analysis failed for {sym}.", file=sys.stderr)
            continue

        # 3) collect outputs -> docs/{symbol}/
        sym_dir = os.path.join(pages_root, sym)
        ensure_dir(sym_dir)

        # png
        latest_png = latest_file(os.path.join(BASE, f"*{sym}*_chipzones_hybrid.png")) or latest_file(os.path.join(BASE, f"*{sym}*.png"))
        if latest_png:
            shutil.copy2(latest_png, os.path.join(sym_dir, os.path.basename(latest_png)))
            shutil.copy2(latest_png, os.path.join(sym_dir, f"{sym}_chipzones_hybrid.png"))  # stable name

        # csv (zones)
        latest_zcsv = latest_file(os.path.join(BASE, f"*{sym}*_chipzones_hybrid.csv"))
        if latest_zcsv:
            shutil.copy2(latest_zcsv, os.path.join(sym_dir, os.path.basename(latest_zcsv)))
            shutil.copy2(latest_zcsv, os.path.join(sym_dir, f"{sym}_chipzones_hybrid.csv"))

        # cleanup: ignore xlsx artifacts
        for f in glob.glob(os.path.join(BASE, f"*{sym}*.xlsx")):
            try: os.remove(f)
            except: pass

    print("Pipeline complete ✅")

if __name__ == "__main__":
    main()
