#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_to_notion_v2.3_auto_update.py
---------------------------------------
功能：
✅ 只创建一个统一数据库（不存在则自动创建）
✅ 每次运行先清空数据库旧行，再批量上传 ./docs/<symbol>/ 下所有品种
✅ 生成/更新「📘 品种浏览目录」：不重复创建；先清空旧内容，再写入最新图表 + CSV表格（前10行）
✅ URL 自动过滤 /docs；CSV/字段全部按文本写入，避免类型报错

环境变量（必填）：
- NOTION_TOKEN           Notion 集成 Token
- NOTION_PARENT_PAGE     目录与数据库要放置的页面 page_id（UUID）
- PAGES_BASE             GitHub Pages 根，例如 https://用户名.github.io/仓库名
可选：
- NOTION_DB              已有数据库ID（不填则自动创建并持久化到 notion_db_id.txt）

依赖：pip install notion-client
"""

import os
import csv
import re
from itertools import islice
from notion_client import Client
from notion_client.errors import APIResponseError

# ===== Env =====
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE", "").strip()
NOTION_DB = os.getenv("NOTION_DB", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").strip().rstrip("/")
PAGES_BASE = PAGES_BASE.replace("/docs", "")   # 保险：移除多余 /docs

notion = Client(auth=NOTION_TOKEN)


# ===== Utils =====
def is_valid_uuid(uid: str) -> bool:
    return bool(re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", uid))


def read_csv_fieldnames(local_csv: str):
    with open(local_csv, "r", encoding="utf-8") as f:
        return next(csv.DictReader(f)).keys()


# ===== Database (single, init once) =====
def ensure_database(fieldnames):
    """确保数据库存在且只初始化一次；返回 dbid"""
    global NOTION_DB

    # 1) 本地缓存
    cache_file = "notion_db_id.txt"
    if os.path.exists(cache_file):
        dbid = open(cache_file).read().strip()
        if is_valid_uuid(dbid):
            NOTION_DB = dbid
            print(f"[push_to_notion] ✅ Using existing database (local): {dbid}")
            return dbid

    # 2) 环境变量
    if is_valid_uuid(NOTION_DB):
        print(f"[push_to_notion] ✅ Using NOTION_DB from env: {NOTION_DB}")
        return NOTION_DB

    # 3) 创建新库
    if not is_valid_uuid(NOTION_PARENT_PAGE):
        raise ValueError("❌ NOTION_PARENT_PAGE 未设置或不是有效 UUID")

    print("[push_to_notion] ⚠️ No valid NOTION_DB found, creating a new one...")
    props = {
        "Name": {"title": {}},
        "Symbol": {"rich_text": {}},
        "Image": {"url": {}},
        "CSV": {"url": {}},
    }
    for f in fieldnames:
        if f not in props:
            props[f] = {"rich_text": {}}   # 全部文本，避免类型错误

    db = notion.databases.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis (Unified)"}}],
        properties=props,
    )
    dbid = db["id"]
    open(cache_file, "w").write(dbid)
    NOTION_DB = dbid
    print(f"[push_to_notion] ✅ Created database: {dbid}")
    return dbid


def clear_database(dbid):
    """归档数据库中所有旧页面（保留结构）"""
    try:
        total = 0
        start_cursor = None
        while True:
            resp = notion.databases.query(database_id=dbid, start_cursor=start_cursor) if start_cursor else notion.databases.query(database_id=dbid)
            results = resp.get("results", [])
            for page in results:
                notion.pages.update(page_id=page["id"], archived=True)
            total += len(results)
            if not resp.get("has_more"):
                break
            start_cursor = resp.get("next_cursor")
        print(f"[push_to_notion] 🧹 Cleared {total} old records")
    except Exception as e:
        print(f"[push_to_notion] ⚠️ Failed to clear old records: {e}")


# ===== Upload rows for one symbol =====
def make_properties(row: dict, symbol: str, png_url: str, csv_url: str):
    props = {
        "Name":   {"title":    [{"type": "text", "text": {"content": f"{symbol} 筹码分析"}}]},
        "Symbol": {"rich_text":[{"type": "text", "text": {"content": symbol}}]},
        "Image":  {"url": png_url},
        "CSV":    {"url": csv_url},
    }
    for k, v in row.items():
        if k not in props:
            props[k] = {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}
    return props


def upload_symbol(symbol: str, dbid: str):
    """读取本地CSV并写入Notion数据库"""
    csv_path = f"./docs/{symbol}/{symbol}_chipzones_hybrid.csv"
    png_url  = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"
    csv_url  = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv"

    if not os.path.exists(csv_path):
        print(f"[skip] ❌ CSV 不存在：{csv_path}")
        return

    success, fail = 0, 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                notion.pages.create(parent={"database_id": dbid},
                                   properties=make_properties(row, symbol, png_url, csv_url))
                success += 1
            except APIResponseError as e:
                print(f"[WARN] {symbol} Failed row | {e}")
                fail += 1

    print(f"[push_to_notion] ✅ {symbol}: Uploaded {success}, ❌ Failed {fail}")


# ===== Directory page (create once, then update-in-place) =====
def get_or_create_directory_page(title: str, parent_page_id: str) -> str:
    """返回目录页 page_id；存在则复用，不存在则创建"""
    # 用 search + 过滤父页面匹配
    results = notion.search(query=title, filter={"property": "object", "value": "page"}).get("results", [])
    for p in results:
        if p.get("parent", {}).get("page_id") == parent_page_id:
            # 标题字段在 properties 中
            prop = p.get("properties", {}).get("title", {}).get("title", [])
            if prop and prop[0].get("plain_text") == title:
                return p["id"]

    # 未找到则创建
    page = notion.pages.create(
        parent={"page_id": parent_page_id},
        properties={"title": [{"type": "text", "text": {"content": title}}]},
        children=[]
    )
    return page["id"]


def clear_page_children(page_id: str):
    """把目录页的所有子块标记为 archived=True，相当于清空页面内容"""
    # 遍历所有 child blocks 分页归档
    start_cursor = None
    total = 0
    while True:
        resp = notion.blocks.children.list(block_id=page_id, start_cursor=start_cursor) if start_cursor \
               else notion.blocks.children.list(block_id=page_id)
        for b in resp.get("results", []):
            try:
                notion.blocks.update(block_id=b["id"], archived=True)
                total += 1
            except Exception as e:
                print(f"[push_to_notion] ⚠️ Failed to archive block {b['id']}: {e}")
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    print(f"[push_to_notion] 🧽 Cleared {total} old blocks in directory page")


def build_directory_children(symbols):
    """根据 symbols 生成图表 + CSV表格（前10行）的 blocks 列表"""
    children_blocks = []
    for s in symbols:
        img_url  = f"{PAGES_BASE}/{s}/{s}_chipzones_hybrid.png"
        csv_path = f"./docs/{s}/{s}_chipzones_hybrid.csv"

        # 标题
        children_blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": f"{s} 筹码走势图"}}]}
        })
        # 图片
        children_blocks.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": img_url}}
        })

        # 表格（前10行）
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                rows = list(islice(csv.reader(f), 11))  # 表头+10行
            if rows:
                header, data = rows[0], rows[1:]
                # 小标题
                children_blocks.append({
                    "object": "block",
                    "type": "heading_3",
                    "heading_3": {"rich_text": [{"type": "text", "text": {"content": "📊 筹码密集区数据表（前10行）"}}]}
                })
                # 表格
                table_rows = []
                for row in data:
                    table_rows.append({
                        "type": "table_row",
                        "table_row": {"cells": [[{"type": "text", "text": {"content": str(cell)}}] for cell in row]}
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

    return children_blocks


# ===== Main =====
def main():
    if not NOTION_TOKEN:
        raise ValueError("❌ NOTION_TOKEN 未设置")
    if not is_valid_uuid(NOTION_PARENT_PAGE):
        raise ValueError("❌ NOTION_PARENT_PAGE 未设置或不是有效 UUID")
    if not PAGES_BASE:
        raise ValueError("❌ PAGES_BASE 未设置")

    base_dir = "./docs"
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    if not symbols:
        print("❌ 未在 ./docs 下找到任何品种文件夹")
        return

    # 只用第一份CSV的表头来初始化数据库（只执行一次）
    first_csv = f"{base_dir}/{symbols[0]}/{symbols[0]}_chipzones_hybrid.csv"
    dbid = ensure_database(read_csv_fieldnames(first_csv))
    clear_database(dbid)

    # 批量上传所有品种
    for sym in symbols:
        upload_symbol(sym, dbid)

    # 目录页：只保留一个，且每次运行都覆盖更新内容
    title = "📘 品种浏览目录"
    page_id = get_or_create_directory_page(title, NOTION_PARENT_PAGE)
    clear_page_children(page_id)
    blocks = build_directory_children(symbols)
    if blocks:
        notion.blocks.children.append(block_id=page_id, children=blocks)
    print(f"[push_to_notion] ✅ 目录页已更新完成（{title}）")


if __name__ == "__main__":
    main()
