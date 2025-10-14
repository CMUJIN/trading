# -*- coding: utf-8 -*-
"""
push_to_notion_v2.7_rebuild_dir.py
--------------------------------------------------
✅ 版本特性：
1. 每次运行前自动清空目录页所有内容（彻底避免重复）
2. 重新生成所有品种分析区块（图 + CSV）
3. 保留单一数据库结构
4. 所有文字为英文避免乱码
5. 支持大规模品种批量更新
"""

import os
import csv
import datetime
from notion_client import Client

# ==============================
# 环境变量设置
# ==============================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://USERNAME.github.io/REPO/docs")

notion = Client(auth=NOTION_TOKEN)

# ==============================
# 工具函数
# ==============================

def make_property(k, v):
    """根据值类型创建 Notion 属性"""
    try:
        return {"type": "number", "number": float(v)}
    except Exception:
        return {"type": "rich_text", "rich_text": [{"type": "text", "text": {"content": str(v)}}]}


def get_unique_directory_page(title, parent_id):
    """获取或创建唯一 Symbol Directory 页面"""
    results = notion.search(query=title).get("results", [])
    for r in results:
        if r["object"] == "page" and r["properties"].get("title"):
            if any(t["plain_text"] == title for t in r["properties"]["title"]["title"]):
                print(f"[push_to_notion] ✅ Using existing directory page: {r['id']}")
                return r["id"]

    # 未找到则创建新页
    page = notion.pages.create(
        parent={"page_id": parent_id},
        properties={"title": {"title": [{"type": "text", "text": {"content": title}}]}},
    )
    print(f"[push_to_notion] 🆕 Created new directory page: {page['id']}")
    return page["id"]


def clear_all_blocks(page_id):
    """彻底清空目录页下的所有块"""
    cursor = None
    total_deleted = 0
    while True:
        resp = notion.blocks.children.list(block_id=page_id, start_cursor=cursor)
        blocks = resp.get("results", [])
        for block in blocks:
            try:
                notion.blocks.update(block_id=block["id"], archived=True)
                total_deleted += 1
            except Exception as e:
                print(f"[WARN] Failed to delete block: {e}")
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    print(f"[push_to_notion] 🧹 Cleared {total_deleted} old blocks from directory.")


def upsert_rows(symbol, png_url, csv_path):
    """上传 CSV 数据到 Notion 数据库"""
    dbid = NOTION_DB
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        uploaded = 0
        for row in reader:
            props = {k: make_property(k, v) for k, v in row.items()}
            props["Symbol"] = {"title": [{"type": "text", "text": {"content": symbol}}]}
            props["Chart"] = {"url": png_url}
            try:
                notion.pages.create(parent={"database_id": dbid}, properties=props)
                uploaded += 1
            except Exception as e:
                print(f"[WARN] Failed row for {symbol}: {e}")
        print(f"[push_to_notion] ✅ Uploaded {uploaded} rows for {symbol}")


def rebuild_directory(symbols):
    """完全重建目录页面"""
    dir_page = get_unique_directory_page("📘 Symbol Directory", NOTION_PARENT_PAGE)
    clear_all_blocks(dir_page)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    new_blocks = []

    for symbol in symbols:
        img_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png?v={timestamp}"
        csv_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv?v={timestamp}"

        new_blocks.extend([
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"type": "text", "text": {"content": f"{symbol} Analysis"}}]},
            },
            {
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": img_url}},
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [
                    {"type": "text", "text": {"content": "📊 View CSV Data", "link": {"url": csv_url}}}
                ]},
            },
        ])

    notion.blocks.children.append(block_id=dir_page, children=new_blocks)
    print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")


# ==============================
# 主流程
# ==============================

def main():
    symbols = os.getenv("SYMBOLS", "JM2601,M2605").split(",")
    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    for symbol in symbols:
        csv_path = f"docs/{symbol}/{symbol}_chipzones_hybrid.csv"
        png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"
        if not os.path.exists(csv_path):
            print(f"[WARN] CSV not found for {symbol}: {csv_path}")
            continue
        upsert_rows(symbol, png_url, csv_path)

    rebuild_directory(symbols)


if __name__ == "__main__":
    main()
