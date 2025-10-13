#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, glob, datetime as dt
from notion_client import Client

TOKEN = os.environ.get("NOTION_TOKEN")
DB_ID = os.environ.get("NOTION_DB", "").strip()
PARENT = os.environ.get("NOTION_PARENT_PAGE", "").strip()
PAGES_BASE = os.environ.get("PAGES_BASE", "").strip()

if not TOKEN:
    print("[push_to_notion] NOTION_TOKEN is required.", file=sys.stderr)
    sys.exit(2)

notion = Client(auth=TOKEN)

def ensure_database():
    global DB_ID
    if DB_ID:
        return DB_ID
    # Search existing
    target_title = "Futures Chip Analysis"
    try:
        res = notion.search(query=target_title, filter={"value":"database","property":"object"})
        for obj in res.get("results", []):
            if obj["object"] == "database":
                title = ""
                tprop = obj.get("title", [])
                if tprop:
                    if "text" in tprop[0]:
                        title = tprop[0]["text"]["content"]
                    elif "plain_text" in tprop[0]:
                        title = tprop[0]["plain_text"]
                if title == target_title:
                    DB_ID = obj["id"]
                    return DB_ID
    except Exception as e:
        print("[search database] warn:", e, file=sys.stderr)

    if not PARENT:
        print("[push_to_notion] NOTION_DB not set and NOTION_PARENT_PAGE missing; cannot create database.", file=sys.stderr)
        sys.exit(3)

    db = notion.databases.create(
        parent={"type":"page_id","page_id":PARENT},
        title=[{"type":"text","text":{"content": target_title}}],
        properties={
            "品种": {"title": {}},
            "日期": {"date": {}},
            "图表链接": {"url": {}},
            "备注": {"rich_text": {}},
        }
    )
    DB_ID = db["id"]
    print("[push_to_notion] Created database:", DB_ID)
    return DB_ID

def upsert_record(symbol, url):
    dbid = ensure_database()
    today = dt.date.today().isoformat()
    notion.pages.create(
        parent={"database_id": dbid},
        properties={
            "品种": {"title": [{"text": {"content": symbol}}]},
            "日期": {"date": {"start": today}},
            "图表链接": {"url": url},
        }
    )

def main():
    if not PAGES_BASE:
        print("[push_to_notion] PAGES_BASE not set; skipping (set to your GitHub Pages base URL).", file=sys.stderr)
        return

    docs = "docs"
    for symdir in sorted(glob.glob(os.path.join(docs, "*"))):
        if not os.path.isdir(symdir):
            continue
        symbol = os.path.basename(symdir)
        latest = os.path.join(symdir, f"{symbol}_latest.png")
        if not os.path.exists(latest):
            pngs = glob.glob(os.path.join(symdir, "*.png"))
            if not pngs:
                continue
            pngs.sort(key=os.path.getmtime, reverse=True)
            latest = pngs[0]
        url = f"{PAGES_BASE}/{symbol}/{os.path.basename(latest)}"
        upsert_record(symbol, url)
        print(f"[push_to_notion] Upserted {symbol} -> {url}")

if __name__ == "__main__":
    main()
