# -*- coding: utf-8 -*-
"""
push_to_notion_v2.9_safe_directory_protect.py
✅ 修复 clear_directory 误删数据库问题
✅ 自动检测 child_database / child_page 并跳过
✅ 加入安全模式提示与日志输出
✅ 兼容自动符号读取与上传逻辑
"""

import os
import csv
import yaml
from notion_client import Client
from notion_client.errors import APIResponseError

# 环境变量
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://cmujin.github.io/trading")

notion = Client(auth=NOTION_TOKEN)


def safe_text_block(content, block_type="heading_2"):
    """生成安全的文本块"""
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": str(content)}}]},
    }


def read_csv(csv_path):
    """读取 CSV 文件"""
    with open(csv_path, "r", encoding="utf-8") as f:
        return f.read()


def clear_directory_safe(directory_id):
    """安全清空目录（不会删除数据库 / 页面类型块）"""
    try:
        children = notion.blocks.children.list(directory_id)["results"]
        deleted = 0
        skipped = 0

        for child in children:
            block_type = child.get("type", "")
            block_id = child["id"]

            # 🚫 跳过数据库与子页面
            if block_type in ["child_database", "child_page"]:
                print(f"[SAFE MODE] ⚠️ Skipped deleting {block_type} block ({block_id})")
                skipped += 1
                continue

            notion.blocks.delete(block_id)
            deleted += 1

        print(f"[push_to_notion] 🧹 Cleared {deleted} blocks (skipped {skipped} database/page blocks).")

    except Exception as e:
        print(f"[WARN] Failed to clear directory safely: {e}")


def build_symbol_directory(symbols):
    """重建 Symbol Directory 页面"""
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")
    directory_id = NOTION_PARENT_PAGE
    clear_directory_safe(directory_id)

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
    """上传行数据至数据库"""
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
    """主入口"""
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    raw_symbols = config.get("symbols", [])

    # ✅ 支持字符串和字典两种结构
    symbols = []
    for s in raw_symbols:
        if isinstance(s, dict) and "code" in s:
            symbols.append(s["code"])
        elif isinstance(s, str):
            symbols.append(s)

    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    # 上传 CSV 内容
    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        if os.path.exists(csv_path):
            upsert_rows(code, csv_path)
        else:
            print(f"[WARN] CSV not found for {code}: {csv_path}")

    # 重建目录（安全模式）
    build_symbol_directory(symbols)


if __name__ == "__main__":
    main()
