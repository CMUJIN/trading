# -*- coding: utf-8 -*-
"""
push_to_notion_v3_full_text_safe.py
----------------------------------------
版本特性：
✅ 所有字段统一以 rich_text 上传，彻底解决类型错误问题
✅ 自动检测 & 创建数据库字段（包含 Chart）
✅ 自动清空 Symbol Directory 页面并重新生成目录
✅ 保留单一数据库结构
"""

import os, csv, time
from notion_client import Client
from notion_client.errors import APIResponseError

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")

notion = Client(auth=NOTION_TOKEN)

# ============ 通用安全检查 ============
if not NOTION_TOKEN:
    raise ValueError("❌ NOTION_TOKEN 未设置，请在 GitHub Secrets 中配置。")

if not NOTION_PARENT_PAGE or len(NOTION_PARENT_PAGE) < 20:
    raise ValueError("❌ NOTION_PARENT_PAGE 未设置或不是有效 UUID。")

if not NOTION_DB or NOTION_DB.strip() in ("***", "", None):
    raise ValueError("❌ NOTION_DB 未设置或不是有效 UUID，请在 GitHub Secrets 中配置真实数据库 ID。")

# ============ 通用工具函数 ============

def read_csv_fieldnames(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
    return header

def make_property(k, v):
    """所有字段统一为 rich_text"""
    if v is None:
        v = ""
    return {"rich_text": [{"text": {"content": str(v)}}]}

def ensure_database(fieldnames):
    """检测数据库是否存在，并补齐字段"""
    try:
        db = notion.databases.retrieve(NOTION_DB)
        props = db["properties"]
        updated = False
        for f in fieldnames + ["Symbol", "Chart"]:
            if f not in props:
                props[f] = {"rich_text": {}} if f != "Chart" else {"url": {}}
                updated = True
        if updated:
            notion.databases.update(NOTION_DB, properties=props)
            print("[push_to_notion] ✅ 数据库字段已自动补齐。")
        else:
            print(f"[push_to_notion] ✅ Using existing database: {NOTION_DB}")
        return NOTION_DB
    except APIResponseError as e:
        print(f"[push_to_notion] ❌ 无法访问数据库: {e}")
        raise

def upsert_rows(symbol, chart_url, csv_path):
    """上传 CSV 内容至数据库"""
    dbid = ensure_database(read_csv_fieldnames(csv_path))
    uploaded = 0
    failed = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            props = {k: make_property(k, v) for k, v in row.items()}
            props["Symbol"] = {"rich_text": [{"text": {"content": symbol}}]}
            props["Chart"] = {"url": chart_url}
            try:
                notion.pages.create(parent={"database_id": dbid}, properties=props)
                uploaded += 1
            except Exception as e:
                print(f"[WARN] Failed row for {symbol}: {e}")
                failed += 1
    print(f"[push_to_notion] ✅ Uploaded {uploaded} rows, ❌ Failed {failed}")

def clear_all_blocks(page_id):
    """清空指定 Notion 页面所有子块"""
    blocks = notion.blocks.children.list(page_id)["results"]
    for b in blocks:
        try:
            notion.blocks.delete(b["id"])
        except Exception:
            pass
    print(f"[push_to_notion] 🧹 Cleared {len(blocks)} old blocks from directory.")

def build_symbol_directory(symbols):
    """在 Notion 上重建目录页"""
    dir_title = "📘 Symbol Directory"
    dir_pages = notion.search(query=dir_title)["results"]
    directory = None
    for page in dir_pages:
        if page["object"] == "page" and page["properties"]["title"]["title"][0]["text"]["content"] == dir_title:
            directory = page
            break

    if not directory:
        print("[push_to_notion] Creating new Symbol Directory page...")
        directory = notion.pages.create(
            parent={"page_id": NOTION_PARENT_PAGE},
            properties={"title": {"title": [{"text": {"content": dir_title}}]}},
        )
    else:
        print(f"[push_to_notion] ✅ Using existing directory page: {directory['id']}")

    clear_all_blocks(directory["id"])

    children = []
    for sym in symbols:
        chart_path = f"docs/{sym}/{sym}_chipzones_hybrid.png"
        csv_path = f"docs/{sym}/{sym}_chipzones_hybrid.csv"
        ts = int(time.time())
        chart_url = f"https://你的用户名.github.io/仓库名/{chart_path}?t={ts}"

        children.append({
            "object": "block",
            "type": "heading_2",
            "heading_2": {"text": [{"type": "text", "text": {"content": sym}}]},
        })
        children.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": chart_url}},
        })
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                csv_text = f.read()
            children.append({
                "object": "block",
                "type": "code",
                "code": {
                    "language": "csv",
                    "rich_text": [{"type": "text", "text": {"content": csv_text[:1800]}}],
                },
            })
        else:
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {"type": "text", "text": {"content": f"⚠️ CSV not found for {sym}"}}
                    ]
                }
            })


    notion.blocks.children.append(directory["id"], children=children)
    print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")

# ============ 主执行逻辑 ============

def main():
    symbols = [s.strip() for s in os.getenv("SYMBOLS", "JM2601").split(",")]
    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    for sym in symbols:
        csv_path = f"docs/{sym}/{sym}_chipzones_hybrid.csv"
        if not os.path.exists(csv_path):
            print(f"[WARN] CSV not found for {sym}: {csv_path}")
            continue
        chart_path = f"docs/{sym}/{sym}_chipzones_hybrid.png"
        chart_url = f"https://你的用户名.github.io/仓库名/{chart_path}?t={int(time.time())}"
        upsert_rows(sym, chart_url, csv_path)

    build_symbol_directory(symbols)

if __name__ == "__main__":
    main()
