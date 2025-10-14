# -*- coding: utf-8 -*-
"""
push_to_notion_v2.6_auto_config.py
----------------------------------
✅ 自动读取 config.yaml 中的 symbols 列表
✅ 自动清空并重建目录页（防止重复）
✅ 保留 Notion 数据库，不重复创建字段
✅ 自动忽略缺失文件
✅ 不再写死任何 symbol
"""

import os
import csv
import yaml
from notion_client import Client
from notion_client.errors import APIResponseError

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://cmujin.github.io/trading")

if not NOTION_TOKEN or not NOTION_DB:
    raise ValueError("❌ 缺少 NOTION_TOKEN 或 NOTION_DB 环境变量")

notion = Client(auth=NOTION_TOKEN)


def safe_text_block(content, block_type="heading_2"):
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": content}}]},
    }


def read_csv(csv_path):
    with open(csv_path, "r", encoding="utf-8") as f:
        return f.read()


def clear_directory(directory_id):
    try:
        children = notion.blocks.children.list(directory_id)["results"]
        for child in children:
            notion.blocks.delete(child["id"])
        print(f"[push_to_notion] 🧹 Cleared {len(children)} old blocks from directory.")
    except Exception as e:
        print(f"[WARN] Failed to clear directory: {e}")


def build_symbol_directory(symbols):
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")
    directory_id = NOTION_PARENT_PAGE
    clear_directory(directory_id)

    children = []
    for sym in symbols:
        csv_path = f"docs/{sym}/{sym}_chipzones_hybrid.csv"
        img_path = f"docs/{sym}/{sym}_chipzones_hybrid.png"
        csv_url = f"{PAGES_BASE}/docs/{sym}/{sym}_chipzones_hybrid.csv"
        img_url = f"{PAGES_BASE}/docs/{sym}/{sym}_chipzones_hybrid.png"

        children.append(safe_text_block(f"{sym} Analysis"))
        children.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": img_url}},
        })

        if os.path.exists(csv_path):
            csv_text = read_csv(csv_path)
            children.append({
                "object": "block",
                "type": "code",
                "code": {
                    "language": "markdown",
                    "rich_text": [
                        {"type": "text", "text": {"content": csv_text[:1800]}}
                    ],
                },
            })
        else:
            children.append(safe_text_block(f"⚠️ CSV not found for {sym}", "paragraph"))

    notion.blocks.children.append(directory_id, children=children)
    print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")


def upsert_rows(symbol, csv_path):
    csv_url = f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.csv"
    img_url = f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.png"

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            props = {
                "Name": {"title": [{"text": {"content": f"{symbol} 筹码分析"}}]},
                "CSV": {"url": csv_url},
                "Image": {"url": img_url},
            }
            for k, v in row.items():
                props[k] = {"rich_text": [{"text": {"content": str(v)}}]}
            try:
                notion.pages.create(parent={"database_id": NOTION_DB}, properties=props)
            except APIResponseError as e:
                print(f"[WARN] Failed row for {symbol}: {e}")


def main():
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    symbols = config.get("symbols", [])

    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    for sym in symbols:
        csv_path = f"docs/{sym}/{sym}_chipzones_hybrid.csv"
        if os.path.exists(csv_path):
            upsert_rows(sym, csv_path)
        else:
            print(f"[WARN] CSV not found for {sym}: {csv_path}")

    build_symbol_directory(symbols)


if __name__ == "__main__":
    main()
