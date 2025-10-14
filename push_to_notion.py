# -*- coding: utf-8 -*-
"""
push_to_notion_v2.4_singleDB_auto_update_dirfix.py
--------------------------------------------------
版本说明：
✅ 在 v2.4_singleDB_auto_update 基础上增强：
1. 保留单数据库上传逻辑（不重复创建）
2. 每次执行前自动清空目录页旧内容
3. 图片 / CSV 链接添加时间戳防缓存
4. 目录页保持 📘 Symbol Directory，不重复创建
5. 全英文标题与链接文本（避免乱码）

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
        properties={
            "title": {"title": [{"type": "text", "text": {"content": title}}]}
        },
    )
    print(f"[push_to_notion] 🆕 Created new directory page: {page['id']}")
    return page["id"]

# ------------------------------
# Step 1: 上传 CSV 数据到单一数据库
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
# Step 2: 刷新目录页（核心增强部分）
# ------------------------------

def refresh_directory_page(symbols):
    """彻底清空旧目录块并重新添加最新图片和表格链接"""
    dir_page = get_unique_directory_page("📘 Symbol Directory", NOTION_PARENT_PAGE)

    # ---- ① 清空旧块 ----
    print(f"[push_to_notion] 🧹 Clearing old directory blocks...")
    cursor = None
    while True:
        resp = notion.blocks.children.list(block_id=dir_page, start_cursor=cursor) if cursor \
               else notion.blocks.children.list(block_id=dir_page)
        for b in resp.get("results", []):
            try:
                notion.blocks.update(block_id=b["id"], archived=True)
            except Exception as e:
                print(f"[WARN] Failed to archive block {b['id']}: {e}")
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    print("[push_to_notion] ✅ Old blocks cleared")

    # ---- ② 构建新块 ----
    print(f"[push_to_notion] 🧩 Rebuilding directory content...")
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M")
    new_blocks = []

    for symbol in symbols:
        img_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png?v={timestamp}"
        csv_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv?v={timestamp}"

        new_blocks.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": f"{symbol} Analysis"}}]}
        })
        new_blocks.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": img_url}}
        })
        new_blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [
                {"type": "text", "text": {"content": "📊 View CSV Data", "link": {"url": csv_url}}}
            ]}
        })

    # ---- ③ 添加新块 ----
    notion.blocks.children.append(block_id=dir_page, children=new_blocks)
    print("[push_to_notion] ✅ Directory page refreshed successfully.")

# ------------------------------
# Step 3: 主函数入口
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

    # 更新目录页
    refresh_directory_page(symbols)

if __name__ == "__main__":
    main()
