#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_to_notion_v1.8_autocreate.py
-----------------------------------
功能：
✅ 自动检测 Notion 数据库 ID 是否有效
✅ 若无效自动重新创建数据库
✅ 每次执行前清空旧数据
✅ 统一文本字段防报错
✅ 自动过滤 GitHub Pages /docs 路径
✅ 从本地 docs 读取 CSV 上传 Notion（网页URL存储）

依赖：
pip install notion-client
"""

import os
import csv
import re
from notion_client import Client
from notion_client.errors import APIResponseError

# ========== 环境变量 ==========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
NOTION_DB = os.getenv("NOTION_DB", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").strip().rstrip("/")
PAGES_BASE = PAGES_BASE.replace("/docs", "")  # ✅ 自动移除多余 /docs

# 初始化 Notion 客户端
notion = Client(auth=NOTION_TOKEN)


# ========== 工具函数 ==========
def is_valid_uuid(uid: str) -> bool:
    """判断字符串是否是有效 UUID"""
    return bool(re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", uid))


# ========== 创建 / 复用数据库 ==========
def ensure_database(fieldnames):
    """确保数据库存在，不重复创建"""
    global NOTION_DB

    # ✅ 优先使用本地记录文件
    if os.path.exists("notion_db_id.txt"):
        with open("notion_db_id.txt", "r") as f:
            dbid = f.read().strip()
            if is_valid_uuid(dbid):
                print(f"[push_to_notion] ✅ Using existing database (local): {dbid}")
                NOTION_DB = dbid
                return dbid

    # ✅ 若环境变量中有值且合法
    if is_valid_uuid(NOTION_DB):
        print(f"[push_to_notion] ✅ Using NOTION_DB from env: {NOTION_DB}")
        return NOTION_DB
    else:
        print(f"[push_to_notion] ⚠️ Invalid or missing NOTION_DB ({NOTION_DB}), creating new database...")

    # ✅ 创建新数据库
    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 未设置 NOTION_PARENT_PAGE 环境变量")

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
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis (Auto Created)"}}],
        properties=props,
    )

    dbid = db["id"]
    NOTION_DB = dbid
    with open("notion_db_id.txt", "w") as f:
        f.write(dbid)
    print(f"[push_to_notion] ✅ Created new database: {dbid}")
    return dbid


# ========== 清空数据库 ==========
def clear_database(dbid):
    """归档数据库中所有旧页面"""
    try:
        results = notion.databases.query(database_id=dbid).get("results", [])
        for page in results:
            notion.pages.update(page_id=page["id"], archived=True)
        print(f"[push_to_notion] 🧹 Cleared {len(results)} old records")
    except Exception as e:
        print(f"[push_to_notion] ⚠️ Failed to clear old records: {e}")


# ========== 上传数据 ==========
def upsert_rows(symbol, png_url, local_csv, csv_url):
    dbid = ensure_database(read_csv_fieldnames(local_csv))
    clear_database(dbid)

    with open(local_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        success, fail = 0, 0
        for row in reader:
            try:
                props = make_properties(row, symbol, png_url, csv_url)
                notion.pages.create(parent={"database_id": dbid}, properties=props)
                success += 1
            except APIResponseError as e:
                print(f"[WARN] Failed row: ? | {e}")
                fail += 1

        print(f"[push_to_notion] ✅ Uploaded {success} rows, ❌ Failed {fail}")


def read_csv_fieldnames(local_csv):
    with open(local_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames


# ========== 属性构造 ==========
def make_properties(row, symbol, png_url, csv_url):
    props = {
        "Name": {"title": [{"type": "text", "text": {"content": f"{symbol} 筹码分析"}}]},
        "Symbol": {"rich_text": [{"type": "text", "text": {"content": symbol}}]},
        "Image": {"url": png_url},
        "CSV": {"url": csv_url},
    }
    for k, v in row.items():
        if k not in props:
            props[k] = {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}
    return props


# ========== 主入口 ==========
def main():
    symbol = os.getenv("SYMBOL", "JM2601")

    # ✅ 本地文件路径
    local_csv = f"./docs/{symbol}/{symbol}_chipzones_hybrid.csv"
    local_png = f"./docs/{symbol}/{symbol}_chipzones_hybrid.png"

    # ✅ Notion 网页链接
    csv_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv"
    png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"

    print(f"[push_to_notion] Starting upload for {symbol}...")

    if not os.path.exists(local_csv):
        raise FileNotFoundError(f"❌ 本地CSV文件不存在：{local_csv}")

    upsert_rows(symbol, png_url, local_csv, csv_url)


if __name__ == "__main__":
    main()
