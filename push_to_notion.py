# -*- coding: utf-8 -*-
"""
Notion 同步脚本 v2.5 (sync + dir fix + single DB)
-------------------------------------------------
功能：
✅ 只创建一个数据库 (Unified)
✅ 每次运行自动清空数据库旧数据
✅ 自动删除配置中未包含的品种数据
✅ 自动更新目录页（不会重复创建）
✅ 支持多品种循环上传（PNG + CSV）
✅ CSV 表格直接显示在页面中（非下载链接）
✅ 英文化所有标注，避免乱码
"""

import os
import csv
from notion_client import Client

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")   # 统一数据库ID
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")  # 根目录页ID
PAGES_BASE = os.getenv("PAGES_BASE", "./docs")

notion = Client(auth=NOTION_TOKEN)

# ========== 通用函数 ==========

def make_property(k, v):
    """根据内容类型自动生成 Notion 属性"""
    if isinstance(v, (int, float)):
        return {"number": float(v)}
    else:
        return {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}

def read_csv_fieldnames(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames

def clear_database(dbid):
    """清空当前数据库"""
    print(f"[push_to_notion] 🧹 Clearing database {dbid} ...")
    cursor = None
    total = 0
    while True:
        resp = notion.databases.query(database_id=dbid, start_cursor=cursor) if cursor \
               else notion.databases.query(database_id=dbid)
        for page in resp.get("results", []):
            notion.pages.update(page_id=page["id"], archived=True)
            total += 1
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    print(f"[push_to_notion] ✅ Cleared {total} records.")

def prune_database_to_symbols(dbid, keep_symbols: set):
    """仅保留当前配置文件中的 Symbol"""
    print(f"[push_to_notion] 🔎 Pruning DB to symbols: {sorted(keep_symbols)}")
    cursor = None
    total_pruned = 0
    while True:
        resp = notion.databases.query(database_id=dbid, start_cursor=cursor) if cursor \
               else notion.databases.query(database_id=dbid)
        for page in resp.get("results", []):
            props = page.get("properties", {})
            sym_prop = props.get("Symbol", {})
            texts = sym_prop.get("rich_text", [])
            sym = texts[0]["plain_text"] if texts else ""
            if sym and sym not in keep_symbols:
                notion.pages.update(page_id=page["id"], archived=True)
                total_pruned += 1
        if not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    print(f"[push_to_notion] 🧹 Pruned {total_pruned} obsolete symbols.")

def get_unique_directory_page(title, parent):
    """创建或获取唯一目录页"""
    results = notion.search(query=title, filter={"property": "object", "value": "page"}).get("results", [])
    candidates = [p for p in results if p.get("parent", {}).get("page_id") == parent]
    if not candidates:
        page = notion.pages.create(
            parent={"page_id": parent},
            properties={"title": [{"type": "text", "text": {"content": title}}]},
        )
        print(f"[push_to_notion] ✅ Created directory page: {title}")
        return page["id"]
    keep = candidates[0]["id"]
    for p in candidates[1:]:
        notion.pages.update(page_id=p["id"], archived=True)
    return keep

def clear_page_children(page_id):
    """清空页面内所有子块"""
    children = notion.blocks.children.list(page_id).get("results", [])
    for c in children:
        notion.blocks.delete(block_id=c["id"])
    print(f"[push_to_notion] 🧹 Cleared old directory blocks")

# ========== 上传逻辑 ==========

def upsert_rows(symbol, png_url, csv_path):
    print(f"[push_to_notion] Uploading {symbol} ...")

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            props = {k: make_property(k, v) for k, v in row.items()}
            props["Symbol"] = make_property("Symbol", symbol)
            notion.pages.create(parent={"database_id": NOTION_DB}, properties=props)

    # 每个品种创建一个 Notion 页面
    page = notion.pages.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        properties={
            "title": [{"type": "text", "text": {"content": symbol}}],
        },
    )
    notion.blocks.children.append(
        block_id=page["id"],
        children=[
            {"object": "block", "type": "image",
             "image": {"type": "external", "external": {"url": png_url}}},
            {"object": "block", "type": "table",
             "table": {"has_column_header": True, "has_row_header": False, "children": []}},
        ],
    )
    print(f"[push_to_notion] ✅ Created symbol page: {symbol}")

# ========== 主入口 ==========

def main():
    if not NOTION_TOKEN:
        raise ValueError("❌ 缺少 NOTION_TOKEN")
    if not NOTION_DB:
        raise ValueError("❌ 缺少 NOTION_DB")
    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 缺少 NOTION_PARENT_PAGE")

    print("[push_to_notion] Starting upload ...")

    # Step 1: 获取当前品种目录
    symbols = [d for d in os.listdir(PAGES_BASE) if os.path.isdir(os.path.join(PAGES_BASE, d))]
    print(f"[push_to_notion] Found symbols: {symbols}")

    # Step 2: 清空旧数据
    clear_database(NOTION_DB)

    # Step 3: 删除配置中未包含的品种
    prune_database_to_symbols(NOTION_DB, set(symbols))

    # Step 4: 上传所有品种
    for symbol in symbols:
        png = os.path.join(PAGES_BASE, symbol, f"{symbol}_chipzones_hybrid.png")
        csvf = os.path.join(PAGES_BASE, symbol, f"{symbol}_chipzones_hybrid.csv")
        png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png".replace("docs/", "")
        if os.path.exists(csvf) and os.path.exists(png):
            upsert_rows(symbol, png_url, csvf)
        else:
            print(f"[WARN] Skipping {symbol}, missing PNG or CSV")

    # Step 5: 更新目录页
    dir_page = get_unique_directory_page("📘 Symbol Directory", NOTION_PARENT_PAGE)
    clear_page_children(dir_page)
    children = []
    for symbol in symbols:
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": f"🔗 {symbol} Chart & Table"},
                        "href": f"{PAGES_BASE}/{symbol}/",
                    }
                ]
            },
        })
    notion.blocks.children.append(block_id=dir_page, children=children)
    print("[push_to_notion] ✅ Updated directory page")

if __name__ == "__main__":
    main()
