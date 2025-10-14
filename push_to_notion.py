# -*- coding: utf-8 -*-
"""
push_to_notion_v2.5_final_stable.py
----------------------------------
✅ 单数据库模式（不重复创建）
✅ 每次自动清空并重建目录页（彻底解决残留 Symbol 问题）
✅ CSV 表格以 Markdown 形式显示
✅ 复用现有 Image 列，不重复生成 Chart 列
✅ GitHub Pages 链接修正（不再乱码）
"""

import os
import csv
from notion_client import Client
from notion_client.errors import APIResponseError

# ======================
# 环境变量
# ======================
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://cmujin.github.io/trading")

if not NOTION_TOKEN or not NOTION_DB:
    raise ValueError("❌ 缺少 NOTION_TOKEN 或 NOTION_DB 环境变量")

notion = Client(auth=NOTION_TOKEN)

# ======================
# 辅助函数
# ======================
def safe_text_block(content, block_type="heading_2"):
    """生成兼容的 Notion 文本块"""
    return {
        "object": "block",
        "type": block_type,
        block_type: {
            "rich_text": [{"type": "text", "text": {"content": content}}],
        },
    }


def read_csv(csv_path):
    """读取 CSV 内容"""
    with open(csv_path, "r", encoding="utf-8") as f:
        return f.read()


def clear_directory(directory_id):
    """清空目录页内容"""
    try:
        children = notion.blocks.children.list(directory_id)["results"]
        for child in children:
            notion.blocks.delete(child["id"])
        print(f"[push_to_notion] 🧹 Cleared {len(children)} old blocks from directory.")
    except Exception as e:
        print(f"[WARN] Failed to clear directory: {e}")


def build_symbol_directory(symbols):
    """重建 Symbol Directory 页面"""
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")
    directory_id = NOTION_PARENT_PAGE
    if not directory_id:
        raise ValueError("❌ NOTION_PARENT_PAGE 未设置")

    # 清空目录
    clear_directory(directory_id)

    children = []

    for sym in symbols:
        csv_path = f"docs/{sym}/{sym}_chipzones_hybrid.csv"
        png_path = f"docs/{sym}/{sym}_chipzones_hybrid.png"
        csv_url = f"{PAGES_BASE}/docs/{sym}/{sym}_chipzones_hybrid.csv"
        img_url = f"{PAGES_BASE}/docs/{sym}/{sym}_chipzones_hybrid.png"

        # 标题块
        children.append(safe_text_block(f"{sym} Analysis"))

        # 图片块
        children.append({
            "object": "block",
            "type": "image",
            "image": {"type": "external", "external": {"url": img_url}},
        })

        # 表格块（用 Markdown 渲染）
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

    # 一次性写入
    try:
        notion.blocks.children.append(directory_id, children=children)
        print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")
    except APIResponseError as e:
        print(f"[ERROR] Directory rebuild failed: {e}")
        raise


def upsert_rows(symbol, csv_path, png_path):
    """上传数据到数据库"""
    print(f"[push_to_notion] ⬆️ Uploading {symbol} to Notion...")

    csv_url = f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.csv"
    img_url = f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.png"

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            props = {
                "Name": {"title": [{"text": {"content": f"{symbol} 筹码分析"}}]},
                "CSV": {"url": csv_url},
                "Image": {"url": img_url},  # ✅ 复用 Image 字段
            }
            # 其他字段统一转文本
            for k, v in row.items():
                props[k] = {"rich_text": [{"text": {"content": str(v)}}]}

            try:
                notion.pages.create(parent={"database_id": NOTION_DB}, properties=props)
            except APIResponseError as e:
                print(f"[WARN] Failed row for {symbol}: {e}")


# ======================
# 主执行逻辑
# ======================
def main():
    symbols = ["JM2601", "M2605"]  # 这里可以改成从 config.yaml 动态读取

    print(f"[push_to_notion] Starting upload for symbols: {symbols}")

    for sym in symbols:
        csv_path = f"docs/{sym}/{sym}_chipzones_hybrid.csv"
        png_path = f"docs/{sym}/{sym}_chipzones_hybrid.png"
        if os.path.exists(csv_path):
            upsert_rows(sym, csv_path, png_path)
        else:
            print(f"[WARN] CSV not found for {sym}: {csv_path}")

    build_symbol_directory(symbols)


if __name__ == "__main__":
    main()
