# -*- coding: utf-8 -*-
"""
push_to_notion_v2.5_auto_clean_dir.py
--------------------------------------------------
✅ 版本改进：
1. 自动清除 Notion 目录中已删除的品种块
2. 单数据库结构（不重复创建）
3. 保留时间戳防缓存
4. 保留 📘 Symbol Directory 页面（不重复创建）
5. 全英文内容避免乱码

环境变量：
NOTION_TOKEN
NOTION_DB
NOTION_PARENT_PAGE
PAGES_BASE
"""

import os
import csv
import datetime
from notion_client import Client

# ------------------------------
# 环境变量初始化
# ------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://USERNAME.github.io/REPO/docs")

notion = Client(auth=NOTION_TOKEN)

# ------------------------------
# 工具函数
# ------------------------------

def read_csv_fieldnames(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames

def make_property(k, v):
    """根据值类型自动生成 Notion 属性"""
    try:
        float(v)
        return {"type": "number", "number": float(v)}
    except Exception:
        return {"type": "rich_text", "rich_text": [{"type": "text", "text": {"content": str(v)}}]}

def get_unique_directory_page(title, parent_id):
    """获取或创建唯一目录页"""
    results = notion.search(query=title).get("results", [])
    for r in results:
        if r["object"] == "page" and r["properties"].get("title"):
            if any(t["plain_text"] == title for t in r["properties"]["title"]["title"]):
                print(f"[push_to_notion] ✅ Using existing directory page: {r['id']}")
                return r["id"]
    # 未找到则创建
    page = notion.pages.create(
        parent={"page_id": parent_id},
        properties={"title": {"title": [{"type": "text", "text": {"content": title}}]}},
    )
    print(f"[push_to_notion] 🆕 Created new directory page: {page['id']}")
    return page["id"]

# ------------------------------
# 上传 CSV 数据到单一数据库
# ------------------------------

def upsert_rows(symbol, png_url, csv_path):
    dbid = NOTION_DB
    print(f"[push_to_notion] 🔁 Syncing {symbol} to Notion DB...")

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
                print(f"[WARN] Failed row: {symbol} | {e}")
        print(f"[push_to_notion] ✅ Uploaded {uploaded} rows for {symbol}")

# ------------------------------
# 刷新目录页（v2.5 核心增强）
# ------------------------------

def refresh_directory_page(symbols):
    """清理旧目录中已删除的品种，并更新图表与CSV"""
    dir_page = get_unique_directory_page("📘 Symbol Directory", NOTION_PARENT_PAGE)

    # ---- ① 获取现有块 ----
    resp = notion.blocks.children.list(block_id=dir_page)
    existing_blocks = resp.get("results", [])
    existing_symbols = []

    for b in existing_blocks:
        if b["type"] == "heading_2":
            title = b["heading_2"]["rich_text"][0]["plain_text"]
            symbol = title.split()[0]
            existing_symbols.append(symbol)

    # ---- ② 删除配置文件中不存在的品种 ----
    removed = [s for s in existing_symbols if s not in symbols]
    for b in existing_blocks:
        if b["type"] == "heading_2":
            title = b["heading_2"]["rich_text"][0]["plain_text"]
            symbol = title.split()[0]
            if symbol in removed:
                print(f"[push_to_notion] 🧹 Removing outdated symbol: {symbol}")
                notion.blocks.update(block_id=b["id"], archived=True)

    # ---- ③ 构建最新内容 ----
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

    # ---- ④ 写入最新块 ----
    notion.blocks.children.append(block_id=dir_page, children=new_blocks)
    print("[push_to_notion] ✅ Directory page refreshed successfully.")

# ------------------------------
# 主函数
# ------------------------------

def main():
    symbols = os.getenv("SYMBOLS", "JM2601,M2601").split(",")
    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    for symbol in symbols:
        csv_path = f"docs/{symbol}/{symbol}_chipzones_hybrid.csv"
        png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"
        if not os.path.exists(csv_path):
            print(f"[WARN] CSV file not found for {symbol}: {csv_path}")
            continue
        upsert_rows(symbol, png_url, csv_path)

    refresh_directory_page(symbols)

if __name__ == "__main__":
    main()
