#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_to_notion_v1.5_fixed.py
-----------------------------------
功能：
1️⃣ 自动检测 Notion 数据库是否存在，不重复创建
2️⃣ 每次执行前自动清空旧数据（仅归档，不删库）
3️⃣ 所有字段统一为文本类型（避免类型冲突）
4️⃣ 自动过滤 GitHub Pages URL 中的 /docs 路径
5️⃣ 上传图片和 CSV 链接到 Notion

依赖：
pip install notion-client
环境变量：
- NOTION_TOKEN
- NOTION_PARENT_PAGE
- NOTION_DB（可选）
- PAGES_BASE（例如 https://用户名.github.io/仓库名）
"""

import os
import csv
from notion_client import Client
from notion_client.errors import APIResponseError

# ========== 环境变量 ==========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
NOTION_DB = os.getenv("NOTION_DB")
PAGES_BASE = os.getenv("PAGES_BASE", "").strip().rstrip("/")
PAGES_BASE = PAGES_BASE.replace("/docs", "")  # ✅ 自动移除多余 /docs

# 初始化客户端
notion = Client(auth=NOTION_TOKEN)


# ========== 创建 / 复用数据库 ==========
def ensure_database(fieldnames):
    """确保数据库存在，不重复创建"""
    global NOTION_DB

    # ✅ 优先使用已有数据库文件
    if os.path.exists("notion_db_id.txt"):
        with open("notion_db_id.txt", "r") as f:
            dbid = f.read().strip()
            if dbid:
                print(f"[push_to_notion] ✅ Using existing database: {dbid}")
                NOTION_DB = dbid
                return dbid

    # ✅ 若环境变量中已有则直接使用
    if NOTION_DB:
        print(f"[push_to_notion] ✅ Using NOTION_DB from env: {NOTION_DB}")
        return NOTION_DB

    print("[push_to_notion] Creating new Notion database...")
    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 未设置 NOTION_PARENT_PAGE 环境变量")

    # ✅ 所有字段设为文本，兼容性最好
    props = {
        "Name": {"title": {}},
        "Symbol": {"rich_text": {}},
        "Image": {"url": {}},
        "CSV": {"url": {}},
    }

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
    print(f"[push_to_notion] ✅ Created database: {dbid}")
    return dbid


# ========== 清空数据库 ==========
def clear_database(dbid):
    """归档数据库中所有旧页面"""
    try:
        results = notion.databases.query(database_id=dbid).get("results", [])
        for page in results:
            page_id = page["id"]
            notion.pages.update(page_id=page_id, archived=True)
        print(f"[push_to_notion] 🧹 Cleared {len(results)} old records")
    except Exception as e:
        print(f"[push_to_notion] ⚠️ Failed to clear old records: {e}")


# ========== 上传数据 ==========
def upsert_rows(symbol, png_url, csv_path):
    dbid = ensure_database(read_csv_fieldnames(csv_path))
    clear_database(dbid)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        success, fail = 0, 0
        for row in reader:
            try:
                props = make_properties(row, symbol, png_url, csv_path)
                notion.pages.create(parent={"database_id": dbid}, properties=props)
                success += 1
            except APIResponseError as e:
                print(f"[WARN] Failed row: ? | {e}")
                fail += 1

        print(f"[push_to_notion] ✅ Uploaded {success} rows, ❌ Failed {fail}")


def read_csv_fieldnames(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames


# ========== 属性构造 ==========
def make_properties(row, symbol, png_url, csv_path):
    props = {
        "Name": {"title": [{"type": "text", "text": {"content": f"{symbol} 筹码分析"}}]},
        "Symbol": {"rich_text": [{"type": "text", "text": {"content": symbol}}]},
        "Image": {"url": png_url},
        "CSV": {"url": csv_path},
    }

    for k, v in row.items():
        if k not in props:
            props[k] = {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}

    return props


# ========== 主入口 ==========
def main():
    symbol = os.getenv("SYMBOL", "JM2601")
    png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"
    csv_path = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv"

    print(f"[push_to_notion] Starting upload for {symbol}...")
    upsert_rows(symbol, png_url, csv_path)


if __name__ == "__main__":
    main()
