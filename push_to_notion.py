#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_to_notion_v2.1_dashboard_embed.py
---------------------------------------
功能：
✅ 自动检测 Notion 数据库是否有效，否则自动创建
✅ 每次运行清空数据库旧记录再上传
✅ 支持多品种自动遍历 ./docs 下所有合约
✅ 每个品种上传图表与数据（全字段文本化）
✅ 自动生成 Notion 浏览页面，直接展示：
    - 图表 (PNG)
    - CSV 前10行表格（非下载链接）
✅ 所有数据写入同一个数据库中
"""

import os
import csv
import re
from itertools import islice
from notion_client import Client
from notion_client.errors import APIResponseError

# ========== 环境变量 ==========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
NOTION_DB = os.getenv("NOTION_DB", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").strip().rstrip("/")
PAGES_BASE = PAGES_BASE.replace("/docs", "")  # ✅ 自动去掉 /docs

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

    # ✅ 优先使用本地缓存
    if os.path.exists("notion_db_id.txt"):
        with open("notion_db_id.txt", "r") as f:
            dbid = f.read().strip()
            if is_valid_uuid(dbid):
                print(f"[push_to_notion] ✅ Using existing database (local): {dbid}")
                NOTION_DB = dbid
                return dbid

    # ✅ 检查环境变量
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
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis (Unified)"}}],
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


# ========== 创建浏览页面 ==========
def create_or_update_index(symbols):
    dashboard_page_id = NOTION_PARENT_PAGE  # 直接放在主 dashboard 下
    print(f"[push_to_notion] 🧭 Generating Notion browsing page under {dashboard_page_id} ...")

    children_blocks = []
    for s in symbols:
        img_url = f"{PAGES_BASE}/{s}/{s}_chipzones_hybrid.png"
        csv_path = f"./docs/{s}/{s}_chipzones_hybrid.csv"

        # 1️⃣ 标题与图片
        children_blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": f"{s} 筹码走势图"}}]}
        })
        children_blocks.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": img_url}}
        })

        # 2️⃣ 表格内容
        if os.path.exists(csv_path):
            rows = list(islice(csv.reader(open(csv_path, "r", encoding="utf-8")), 11))  # 前10行
            header, data = rows[0], rows[1:]
            table_rows = []

            for row in data:
                table_rows.append({
                    "type": "table_row",
                    "table_row": {"cells": [[{"type": "text", "text": {"content": str(cell)}}] for cell in row]}
                })

            children_blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": [{"type": "text", "text": {"content": "📊 筹码密集区数据表（前10行）"}}]}
            })
            children_blocks.append({
                "object": "block",
                "type": "table",
                "table": {
                    "has_column_header": True,
                    "has_row_header": False,
                    "table_width": len(header),
                    "children": [
                        {"type": "table_row",
                         "table_row": {"cells": [[{"type": "text", "text": {"content": h}}] for h in header]}},
                        *table_rows
                    ]
                }
            })
        else:
            children_blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "⚠️ 未找到对应CSV文件"}}]}
            })

    # ✅ 生成目录页
    notion.pages.create(
        parent={"page_id": dashboard_page_id},
        properties={"title": [{"type": "text", "text": {"content": "📘 品种浏览目录"}}]},
        children=children_blocks
    )
    print(f"[push_to_notion] ✅ 品种浏览目录已生成！")


# ========== 主入口 ==========
def main():
    base_dir = "./docs"
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if not symbols:
        print(f"❌ 未找到任何品种目录，请确认 docs/ 下存在合约文件夹")
        return

    for symbol in symbols:
        local_csv = f"{base_dir}/{symbol}/{symbol}_chipzones_hybrid.csv"
        local_png = f"{base_dir}/{symbol}/{symbol}_chipzones_hybrid.png"
        csv_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv"
        png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"

        if not os.path.exists(local_csv):
            print(f"[skip] ❌ 没有找到CSV文件: {local_csv}")
            continue

        print(f"\n[push_to_notion] 🚀 开始上传 {symbol} ...")
        upsert_rows(symbol, png_url, local_csv, csv_url)

    create_or_update_index(symbols)


if __name__ == "__main__":
    main()
