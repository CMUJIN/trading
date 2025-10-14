# -*- coding: utf-8 -*-
"""
push_to_notion_v_textonly.py
----------------------------------------
100% 稳定版：所有字段均为文本类型，彻底杜绝 number 类型错误。
"""

import os
import csv
import traceback
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB", "").strip()
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").rstrip("/")

notion = Client(auth=NOTION_TOKEN)

# =============================
# 建库函数（全部字段为文本）
# =============================
def ensure_database(fieldnames):
    """创建全文本类型的数据库"""
    global NOTION_DB

    # 优先复用缓存
    if os.path.exists("notion_db_id.txt"):
        with open("notion_db_id.txt", "r") as f:
            NOTION_DB = f.read().strip()

    if NOTION_DB and not NOTION_DB.lower().startswith("placeholder"):
        print(f"[push_to_notion] Using existing DB: {NOTION_DB}")
        return NOTION_DB

    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 未设置 NOTION_PARENT_PAGE 环境变量")

    print("[push_to_notion] Creating new Notion database (text-only mode)...")

    props = {
        "Name": {"title": {}},
        "Symbol": {"rich_text": {}},
        "Image": {"url": {}},
        "CSV": {"url": {}},
    }

    # 所有字段均为 rich_text
    for f in fieldnames:
        if f not in props:
            props[f] = {"rich_text": {}}

    db = notion.databases.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis (Text Only)"}}],
        properties=props,
    )

    dbid = db["id"]
    NOTION_DB = dbid
    with open("notion_db_id.txt", "w") as f:
        f.write(dbid)
    print(f"[push_to_notion] Created database: {dbid}")
    return dbid


# =============================
# 属性转换（全转为文本）
# =============================
def to_property(value):
    """强制所有值转为字符串"""
    if value is None:
        value = ""
    return {"rich_text": [{"text": {"content": str(value)}}]}


# =============================
# 上传函数
# =============================
def upsert_rows(symbol, csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        dbid = ensure_database(reader.fieldnames)
        count = 0

        for row in reader:
            try:
                props = {}
                for k, v in row.items():
                    props[k] = to_property(v)

                props["Name"] = {"title": [{"text": {"content": symbol}}]}
                props["Symbol"] = {"rich_text": [{"text": {"content": symbol}}]}
                props["Image"] = {"url": f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.png"}
                props["CSV"] = {"url": f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.csv"}

                notion.pages.create(parent={"database_id": dbid}, properties=props)
                count += 1

            except Exception as e:
                print(f"[WARN] Failed row: {row.get('date', '?')} | {e}")
                traceback.print_exc()

        print(f"[push_to_notion] ✅ Uploaded {count} rows to Notion.")


# =============================
# 主函数
# =============================
def main():
    if not NOTION_TOKEN:
        raise ValueError("❌ 未设置 NOTION_TOKEN 环境变量")

    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
    if not csv_files:
        print("⚠️ No CSV file found.")
        return

    for csvf in csv_files:
        symbol = csvf.split("_")[0].upper()
        upsert_rows(symbol, csvf)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        traceback.print_exc()
        exit(1)
