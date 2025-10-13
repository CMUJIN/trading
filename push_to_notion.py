#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, glob, csv, datetime as dt
from notion_client import Client

TOKEN = os.environ.get("NOTION_TOKEN")
DB_ID = os.environ.get("NOTION_DB", "").strip()
PARENT = os.environ.get("NOTION_PARENT_PAGE", "").strip()
# 如果 DB_ID 是占位符，则清空
if DB_ID.lower().startswith("placeholder") or DB_ID == "None":
    DB_ID = ""

PAGES_BASE = os.environ.get("PAGES_BASE", "").strip()

if not TOKEN:
    print("[push_to_notion] NOTION_TOKEN is required.", file=sys.stderr)
    sys.exit(2)

notion = Client(auth=TOKEN)

def ensure_database(sample_fields):
    global DB_ID
    if DB_ID:
        return DB_ID

    title = "Futures Chip Analysis"
    # Create if needed
    if not PARENT:
        print("[push_to_notion] NOTION_DB not set and NOTION_PARENT_PAGE missing; cannot create database.", file=sys.stderr)
        sys.exit(3)

    # Build property schema: Title + Date + Url + all CSV fields (numbers or text)
    props = {
        "品种": {"title": {}},
        "日期": {"date": {}},
        "图表链接": {"url": {}},
    }
    # Map CSV fields to number/text automatically
    for key in sample_fields:
        # Heuristic: numeric-like fields
        props[key] = {"number": {}}

    db = notion.databases.create(
        parent={"type":"page_id","page_id":PARENT},
        title=[{"type":"text","text":{"content": title}}],
        properties=props
    )
    DB_ID = db["id"]
    print("[push_to_notion] Created database:", DB_ID)
    return DB_ID

def upsert_rows(symbol, png_url, csv_path):
    # Read CSV
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    if not rows:
        return

    # Ensure DB with these fields
    dbid = ensure_database(reader.fieldnames)

    today = dt.date.today().isoformat()
    # Push top N rows (to avoid Notion limits); you can adjust
    for r in rows[:100]:
        props = {
            "品种": {"title": [{"text": {"content": symbol}}]},
            "日期": {"date": {"start": today}},
            "图表链接": {"url": png_url},
        }
        for k, v in r.items():
            try:
                num = float(v)
            except:
                num = None
            if num is not None:
                props[k] = {"number": num}
            else:
                # fallback as rich_text
                props[k] = {"rich_text": [{"text": {"content": str(v)}}]}

        notion.pages.create(parent={"database_id": dbid}, properties=props)

def main():
    if not PAGES_BASE:
        print("[push_to_notion] PAGES_BASE not set; skipping Notion push.", file=sys.stderr)
        return

    docs = "docs"
    for symdir in sorted(glob.glob(os.path.join(docs, "*"))):
        if not os.path.isdir(symdir): 
            continue
        symbol = os.path.basename(symdir)
        png   = os.path.join(symdir, f"{symbol}_chipzones_hybrid.png")
        csvf  = os.path.join(symdir, f"{symbol}_chipzones_hybrid.csv")
        if not os.path.exists(png) or not os.path.exists(csvf):
            continue
        png_url = f"{PAGES_BASE}/{symbol}/{os.path.basename(png)}"
        upsert_rows(symbol, png_url, csvf)
        print(f"[push_to_notion] Pushed {symbol}: {png_url} & {os.path.basename(csvf)}")

if __name__ == "__main__":
    main()
