# -*- coding: utf-8 -*-
"""
push_to_notion_v2.8_auto_config_fix_symbols.py
✅ 修复 symbols 为 dict 导致的路径错误
✅ 修复 Invalid image url
✅ 自动清空目录
✅ 自动读取 config.yaml 并兼容两种结构
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

notion = Client(auth=NOTION_TOKEN)


def safe_text_block(content, block_type="heading_2"):
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": str(content)}}]},
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

    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        img_path = f"docs/{code}/{code}_chipzones_hybrid.png"
        csv_url = f"{PAGES_BASE}/docs/{code}/{code}_chipzones_hybrid.csv"
        img_url = f"{PAGES_BASE}/docs/{code}/{code}_chipzones_hybrid.png"

        children.append(safe_text_block(f"{code} Analysis"))

        if os.path.exists(img_path):
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": img_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Image not found for {code}", "paragraph"))

        if os.path.exists(csv_path):
            csv_text = read_csv(csv_path)
            children.append({
                "object": "block",
                "type": "code",
                "code": {
                    "language": "markdown",
                    "rich_text": [{"type": "text", "text": {"content": csv_text[:1800]}}],
                },
            })
        else:
            children.append(safe_text_block(f"⚠️ CSV not found for {code}", "paragraph"))

    notion.blocks.children.append(directory_id, children=children)
    print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")


def upsert_rows(code, csv_path):
    csv_url = f"{PAGES_BASE}/docs/{code}/{code}_chipzones_hybrid.csv"
    img_url = f"{PAGES_BASE}/docs/{code}/{code}_chipzones_hybrid.png"

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            props = {
                "Name": {"title": [{"text": {"content": f"{code} Analysis"}}]},
                "CSV": {"url": csv_url},
                "Image": {"url": img_url},
            }
            for k, v in row.items():
                props[k] = {"rich_text": [{"text": {"content": str(v)}}]}
            try:
                notion.pages.create(parent={"database_id": NOTION_DB}, properties=props)
            except APIResponseError as e:
                print(f"[WARN] Failed row for {code}: {e}")


def main():
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    raw_symbols = config.get("symbols", [])

    # ✅ 统一转换为纯字符串列表
    symbols = []
    for s in raw_symbols:
        if isinstance(s, dict) and "code" in s:
            symbols.append(s["code"])
        elif isinstance(s, str):
            symbols.append(s)

    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        if os.path.exists(csv_path):
            upsert_rows(code, csv_path)
        else:
            print(f"[WARN] CSV not found for {code}: {csv_path}")

    build_symbol_directory(symbols)


if __name__ == "__main__":
    main()
