#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_to_notion_v2.3_auto_update_fixed.py
---------------------------------------
✅ 支持 NOTION_PARENT_PAGE 传入页面URL、32位或36位ID
✅ 单一数据库（仅初始化一次）
✅ 每次运行清空旧记录再上传
✅ 自动更新「📘 品种浏览目录」页面（不重复创建）
✅ 中文兼容、CSV表格嵌入展示
"""

import os, re, csv
from itertools import islice
from notion_client import Client
from notion_client.errors import APIResponseError


# ========== 工具函数 ==========
def normalize_notion_id(val: str) -> str:
    """支持 URL、32位或36位 Notion 页面ID"""
    if not val:
        return ""
    val = val.strip()
    # 从 URL 中提取 32位ID
    m = re.search(r'([0-9a-fA-F]{32})', val)
    if m:
        raw = m.group(1).lower()
        return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"
    # 36位UUID
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", val):
        return val.lower()
    # 32位ID
    if re.fullmatch(r"[0-9a-fA-F]{32}", val):
        raw = val.lower()
        return f"{raw[0:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"
    return val


def is_valid_uuid(uid: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", uid))


# ========== 环境变量 ==========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE = normalize_notion_id(os.getenv("NOTION_PARENT_PAGE", ""))
NOTION_DB = os.getenv("NOTION_DB", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").strip().rstrip("/").replace("/docs", "")

print(f"[push_to_notion] Using parent page: {NOTION_PARENT_PAGE}")

# 初始化 Notion 客户端
notion = Client(auth=NOTION_TOKEN)


# ========== 数据库 ==========
def ensure_database(fieldnames):
    global NOTION_DB
    if os.path.exists("notion_db_id.txt"):
        dbid = open("notion_db_id.txt").read().strip()
        if is_valid_uuid(dbid):
            NOTION_DB = dbid
            print(f"[push_to_notion] ✅ Using existing database (local): {dbid}")
            return dbid
    if is_valid_uuid(NOTION_DB):
        print(f"[push_to_notion] ✅ Using NOTION_DB from env: {NOTION_DB}")
        return NOTION_DB
    if not is_valid_uuid(NOTION_PARENT_PAGE):
        raise ValueError("❌ NOTION_PARENT_PAGE 无效：请提供页面URL、32位或36位UUID")

    print(f"[push_to_notion] ⚠️ No valid NOTION_DB found, creating new database...")
    props = {"Name": {"title": {}}, "Symbol": {"rich_text": {}}, "Image": {"url": {}}, "CSV": {"url": {}}}
    for f in fieldnames:
        if f not in props:
            props[f] = {"rich_text": {}}
    db = notion.databases.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis (Unified)"}}],
        properties=props,
    )
    dbid = db["id"]
    open("notion_db_id.txt", "w").write(dbid)
    NOTION_DB = dbid
    print(f"[push_to_notion] ✅ Created database: {dbid}")
    return dbid


def clear_database(dbid):
    try:
        total = 0
        cursor = None
        while True:
            resp = notion.databases.query(database_id=dbid, start_cursor=cursor) if cursor else notion.databases.query(database_id=dbid)
            for p in resp.get("results", []):
                notion.pages.update(page_id=p["id"], archived=True)
                total += 1
            if not resp.get("has_more"): break
            cursor = resp["next_cursor"]
        print(f"[push_to_notion] 🧹 Cleared {total} old records")
    except Exception as e:
        print(f"[push_to_notion] ⚠️ Failed to clear old records: {e}")


# ========== 上传 ==========
def make_properties(row, sym, png_url, csv_url):
    props = {
        "Name": {"title": [{"type": "text", "text": {"content": f"{sym} 筹码分析"}}]},
        "Symbol": {"rich_text": [{"type": "text", "text": {"content": sym}}]},
        "Image": {"url": png_url},
        "CSV": {"url": csv_url},
    }
    for k, v in row.items():
        if k not in props:
            props[k] = {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}
    return props


def upload_symbol(sym, dbid):
    csv_path = f"./docs/{sym}/{sym}_chipzones_hybrid.csv"
    png_url = f"{PAGES_BASE}/{sym}/{sym}_chipzones_hybrid.png"
    csv_url = f"{PAGES_BASE}/{sym}/{sym}_chipzones_hybrid.csv"
    if not os.path.exists(csv_path):
        print(f"[skip] ❌ CSV 不存在: {csv_path}")
        return
    success = fail = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                notion.pages.create(parent={"database_id": dbid}, properties=make_properties(row, sym, png_url, csv_url))
                success += 1
            except APIResponseError as e:
                print(f"[WARN] {sym} Failed row | {e}")
                fail += 1
    print(f"[push_to_notion] ✅ {sym}: Uploaded {success}, ❌ Failed {fail}")


# ========== 目录页 ==========
def get_or_create_directory_page(title, parent):
    results = notion.search(query=title, filter={"property": "object", "value": "page"}).get("results", [])
    for p in results:
        if p.get("parent", {}).get("page_id") == parent:
            t = p.get("properties", {}).get("title", {}).get("title", [])
            if t and t[0].get("plain_text") == title:
                return p["id"]
    page = notion.pages.create(parent={"page_id": parent},
                               properties={"title": [{"type": "text", "text": {"content": title}}]})
    return page["id"]


def clear_page_children(page_id):
    cursor = None
    total = 0
    while True:
        resp = notion.blocks.children.list(block_id=page_id, start_cursor=cursor) if cursor else notion.blocks.children.list(block_id=page_id)
        for b in resp.get("results", []):
            try:
                notion.blocks.update(block_id=b["id"], archived=True)
                total += 1
            except Exception as e:
                print(f"[WARN] Failed to archive block {b['id']}: {e}")
        if not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    print(f"[push_to_notion] 🧽 Cleared {total} blocks from directory page")


def build_directory_children(symbols):
    children = []
    for s in symbols:
        img_url = f"{PAGES_BASE}/{s}/{s}_chipzones_hybrid.png"
        csv_path = f"./docs/{s}/{s}_chipzones_hybrid.csv"
        children += [
            {"object": "block", "type": "heading_2",
             "heading_2": {"rich_text": [{"type": "text", "text": {"content": f"{s} 筹码走势图"}}]}},
            {"object": "block", "type": "image",
             "image": {"type": "external", "external": {"url": img_url}}}
        ]
        if os.path.exists(csv_path):
            rows = list(islice(csv.reader(open(csv_path, "r", encoding="utf-8")), 11))
            header, data = rows[0], rows[1:]
            children.append({
                "object": "block", "type": "heading_3",
                "heading_3": {"rich_text": [{"type": "text", "text": {"content": "📊 筹码密集区数据表（前10行）"}}]}
            })
            table_rows = [{"type": "table_row",
                           "table_row": {"cells": [[{"type": "text", "text": {"content": str(c)}}] for c in r]}}
                          for r in data]
            children.append({
                "object": "block",
                "type": "table",
                "table": {
                    "has_column_header": True,
                    "has_row_header": False,
                    "table_width": len(header),
                    "children": [{"type": "table_row",
                                  "table_row": {"cells": [[{"type": "text", "text": {"content": h}}] for h in header]}}]
                                + table_rows
                }
            })
        else:
            children.append({
                "object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": "⚠️ 未找到对应CSV文件"}}]}
            })
    return children


# ========== 主流程 ==========
def main():
    if not NOTION_TOKEN:
        raise ValueError("❌ NOTION_TOKEN 未设置")
    if not is_valid_uuid(NOTION_PARENT_PAGE):
        raise ValueError("❌ NOTION_PARENT_PAGE 无效：请提供页面URL、32位或36位UUID")
    if not PAGES_BASE:
        raise ValueError("❌ PAGES_BASE 未设置")

    base = "./docs"
    symbols = [d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))]
    if not symbols:
        print("❌ 未找到任何品种文件夹")
        return

    first_csv = f"{base}/{symbols[0]}/{symbols[0]}_chipzones_hybrid.csv"
    dbid = ensure_database(next(csv.DictReader(open(first_csv, encoding="utf-8"))).keys())
    clear_database(dbid)

    for sym in symbols:
        upload_symbol(sym, dbid)

    page_id = get_or_create_directory_page("📘 品种浏览目录", NOTION_PARENT_PAGE)
    clear_page_children(page_id)
    notion.blocks.children.append(block_id=page_id, children=build_directory_children(symbols))
    print("[push_to_notion] ✅ 品种浏览目录已更新完成")


if __name__ == "__main__":
    main()
