# -*- coding: utf-8 -*-
"""
push_to_notion_v3.0_final.py
✅ 全字段文本模式稳定版
✅ 自动移除 BOM 和空格
✅ SAFE MODE 防止误删数据库
✅ 自动同步目录到 Notion 页面
"""

import os
import csv
import yaml
from notion_client import Client
from notion_client.errors import APIResponseError

# =============================
# 环境变量
# =============================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://cmujin.github.io/trading")

notion = Client(auth=NOTION_TOKEN)


# =============================
# 工具函数
# =============================
def safe_text_block(content, block_type="heading_2"):
    """安全生成文字块"""
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": str(content)}}]},
    }


def read_csv(csv_path):
    """读取 CSV 内容"""
    with open(csv_path, "r", encoding="utf-8") as f:
        return f.read()


def clear_directory(directory_id):
    """清理目录内容（保留数据库块）"""
    try:
        children = notion.blocks.children.list(directory_id)["results"]
        cleared = 0
        skipped = 0
        for child in children:
            if child["type"] in ["child_database", "child_page"]:
                print(f"[SAFE MODE] ⚠️ Skipped deleting {child['type']} block ({child['id']})")
                skipped += 1
                continue
            notion.blocks.delete(child["id"])
            cleared += 1
        print(f"[push_to_notion] 🧹 Cleared {cleared} blocks (skipped {skipped} database/page blocks).")
    except Exception as e:
        print(f"[WARN] Failed to clear directory: {e}")


def build_symbol_directory(symbols):
    """在 Notion 页面中创建每个品种的图表 + CSV"""
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")
    directory_id = NOTION_PARENT_PAGE
    clear_directory(directory_id)
    children = []

    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        img_path = f"docs/{code}/{code}_chipzones_hybrid.png"
        csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
        img_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png"

        # 添加标题
        children.append(safe_text_block(f"{code} Analysis"))

        # 图片块
        if os.path.exists(img_path):
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": img_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Image not found for {code}", "paragraph"))

        # CSV 表格块（以代码块形式展示前 1800 字）
        if os.path.exists(csv_path):
            csv_text = read_csv(csv_path)
            children.append({
                "object": "block",
                "type": "code",
                "code": {
                    "language": "plain text",
                    "rich_text": [{"type": "text", "text": {"content": csv_text[:1800]}}],
                },
            })
        else:
            children.append(safe_text_block(f"⚠️ CSV not found for {code}", "paragraph"))

    # 上传目录内容
    notion.blocks.children.append(directory_id, children=children)
    print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")


def upsert_rows(code, csv_path):
    """上传 CSV 行数据到 Notion 数据库"""
    csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
    img_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png"

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # 移除 BOM 并清理列名空格
        reader.fieldnames = [h.replace('\ufeff', '').strip() for h in reader.fieldnames]
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
    """主流程"""
    print("[push_to_notion] Starting push_to_notion_v3.0_final...")

    # 读取 config.yaml
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    raw_symbols = config.get("symbols", [])

    # 支持字符串或 dict 格式
    symbols = []
    for s in raw_symbols:
        if isinstance(s, dict) and "code" in s:
            symbols.append(s["code"])
        elif isinstance(s, str):
            symbols.append(s)

    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    # 上传数据到 Notion DB
    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        if os.path.exists(csv_path):
            upsert_rows(code, csv_path)
        else:
            print(f"[WARN] CSV not found for {code}: {csv_path}")

    # 构建目录页
    build_symbol_directory(symbols)


if __name__ == "__main__":
    main()
