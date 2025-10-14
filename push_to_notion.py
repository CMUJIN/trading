# -*- coding: utf-8 -*-
"""
push_to_notion_v1.3.py
----------------------------------------
自动上传分析结果 (CSV + PNG 链接) 到 Notion 数据库
改进版：
- ✅ 自动复用数据库（防止重复创建）
- ✅ 精准区分字段类型（数字/文本）
- ✅ 自动类型识别防止 number 报错
- ✅ 输出清晰日志，兼容 GitHub Actions
----------------------------------------
依赖:
    pip install notion-client pyyaml
"""

import os
import csv
import traceback
from notion_client import Client

# =============================
# 环境变量读取
# =============================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB", "").strip()
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").rstrip("/")

notion = Client(auth=NOTION_TOKEN)

# =============================
# 数据库创建与复用
# =============================
def ensure_database(fieldnames):
    """确保数据库存在，否则创建"""
    global NOTION_DB

    # 1️⃣ 优先从缓存文件读取
    if os.path.exists("notion_db_id.txt"):
        with open("notion_db_id.txt", "r") as f:
            NOTION_DB = f.read().strip()

    # 2️⃣ 若 DB_ID 已存在则直接复用
    if NOTION_DB and not NOTION_DB.lower().startswith("placeholder"):
        print(f"[push_to_notion] Using existing DB: {NOTION_DB}")
        return NOTION_DB

    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 未设置 NOTION_PARENT_PAGE 环境变量")

    print("[push_to_notion] Creating new Notion database...")

    # 3️⃣ 字段定义（部分为数值字段）
    props = {
        "Name": {"title": {}},
        "Symbol": {"rich_text": {}},
        "Image": {"url": {}},
        "CSV": {"url": {}}
    }

    numeric_fields = {"recent_strength", "all_strength", "avg_strength", "persistent", "zone_type"}
    for f in fieldnames:
        if f in props:
            continue
        if f in numeric_fields:
            props[f] = {"number": {}}
        else:
            props[f] = {"rich_text": {}}

    db = notion.databases.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis"}}],
        properties=props,
    )

    dbid = db["id"]
    NOTION_DB = dbid
    with open("notion_db_id.txt", "w") as f:
        f.write(dbid)

    print(f"[push_to_notion] Created database: {dbid}")
    return dbid


# =============================
# 类型识别函数
# =============================
def to_property(value, field_name=None):
    """自动匹配字段类型"""
    if value is None or value == "":
        return {"rich_text": [{"text": {"content": ""}}]}

    # 若该字段是数字类型字段，则尽力转成 number
    numeric_fields = {"recent_strength", "all_strength", "avg_strength", "persistent", "zone_type"}
    try:
        if field_name in numeric_fields:
            return {"number": float(value)}
        # 尝试判断字符串是否可转数字
        if isinstance(value, (int, float)):
            return {"number": float(value)}
        v_str = str(value).strip()
        if v_str.replace(".", "", 1).isdigit():
            return {"number": float(v_str)}
        return {"rich_text": [{"text": {"content": v_str}}]}
    except Exception:
        return {"rich_text": [{"text": {"content": str(value)}}]}


# =============================
# 上传 CSV 数据
# =============================
def upsert_rows(symbol, csv_file):
    """上传 CSV 行数据到 Notion"""
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        dbid = ensure_database(reader.fieldnames)
        count = 0

        for row in reader:
            try:
                props = {}
                for k, v in row.items():
                    props[k] = to_property(v, field_name=k)

                # 附加元信息
                props["Name"] = {"title": [{"text": {"content": f"{symbol}"}}]}
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
# 主执行入口
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
