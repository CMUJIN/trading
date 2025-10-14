# -*- coding: utf-8 -*-
"""
push_to_notion_v3.2_clear_db.py
✅ 每次运行自动清空数据库旧数据
✅ 自动重建目录
✅ 修复 /docs/ 链接过滤
✅ 兼容 symbols 为 dict 或 str
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

# -----------------------------
# 安全文本块包装
# -----------------------------
def safe_text_block(content, block_type="heading_2"):
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": str(content)}}]},
    }

# -----------------------------
# 清空数据库（归档旧页面）
# -----------------------------
def clear_database(database_id):
    try:
        response = notion.databases.query(database_id=database_id)
        total_deleted = 0
        while True:
            for page in response["results"]:
                notion.pages.update(page["id"], archived=True)
                total_deleted += 1
            if not response.get("has_more"):
                break
            response = notion.databases.query(
                database_id=database_id,
                start_cursor=response["next_cursor"]
            )
        print(f"[push_to_notion] 🧹 Cleared {total_deleted} old entries from database.")
    except Exception as e:
        print(f"[WARN] Failed to clear database: {e}")

# -----------------------------
# 清空目录内容（保留目录本身）
# -----------------------------
def clear_directory(directory_id):
    try:
        children = notion.blocks.children.list(directory_id)["results"]
        cleared = 0
        for child in children:
            if child["type"] in ("child_page", "child_database"):
                print(f"[SAFE MODE] ⚠️ Skipped deleting {child['type']} block ({child['id']})")
                continue
            notion.blocks.delete(child["id"])
            cleared += 1
        print(f"[push_to_notion] 🧹 Cleared {cleared} blocks (skipped database/page blocks).")
    except Exception as e:
        print(f"[WARN] Failed to clear directory: {e}")

# -----------------------------
# 构建符号目录页
# -----------------------------
def build_symbol_directory(symbols):
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")
    directory_id = NOTION_PARENT_PAGE
    clear_directory(directory_id)
    children = []

    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        img_path = f"docs/{code}/{code}_chipzones_hybrid.png"

        csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
        img_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png"

        children.append(safe_text_block(f"{code} Analysis"))

        # 图片
        if os.path.exists(img_path):
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": img_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Image not found for {code}", "paragraph"))

        # CSV 表格文本
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8") as f:
                csv_text = f.read()
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

# -----------------------------
# 上传 CSV 数据到数据库
# -----------------------------
def upsert_rows(code, csv_path):
    csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
    img_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png"

    try:
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
                notion.pages.create(parent={"database_id": NOTION_DB}, properties=props)
        print(f"[push_to_notion] ✅ Uploaded rows for {code}")
    except APIResponseError as e:
        print(f"[WARN] Failed row for {code}: {e}")

# -----------------------------
# 主入口
# -----------------------------
def main():
    print("[push_to_notion] Starting upload process...")

    # 清空数据库
    if NOTION_DB:
        clear_database(NOTION_DB)
    else:
        print("[WARN] NOTION_DB not set, skipping clear.")

    # 加载配置文件
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    raw_symbols = config.get("symbols", [])
    symbols = [s["code"] if isinstance(s, dict) and "code" in s else s for s in raw_symbols]
    print(f"[push_to_notion] Symbols to upload: {symbols}")

    # 上传数据
    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        if os.path.exists(csv_path):
            upsert_rows(code, csv_path)
        else:
            print(f"[WARN] CSV not found for {code}: {csv_path}")

    # 构建目录
    build_symbol_directory(symbols)

    print("[push_to_notion] ✅ All tasks completed.")


if __name__ == "__main__":
    main()
