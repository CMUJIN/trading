#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
push_to_notion.py (CDN + Forced Refresh Version)
--------------------------------------------------------
🚀 彻底解决 Notion 图片不刷新的缓存问题：
    - 每张图片 URL 自动附加更新时间戳 ?t=xxxxxx
    - Notion 认为是全新 URL，从而强制重新加载最新图片

📌 保持你原有逻辑完全不变，只修复图片更新问题
"""

import os
import csv
import yaml
import time
from notion_client import Client
from notion_client.errors import APIResponseError
import glob
from datetime import datetime

# -------------------------------------------
# 固定使用 jsDelivr CDN
# -------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")

PAGES_BASE = "https://cdn.jsdelivr.net/gh/CMUJIN/trading@main/docs"

notion = Client(auth=NOTION_TOKEN)


# -----------------------------
# 公共函数
# -----------------------------
def safe_text_block(content, block_type="heading_2"):
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": str(content)}}]},
    }


# -----------------------------
# 清空目录页
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
# 获取文件更新时间
# -----------------------------
def get_file_update_time(path):
    if not os.path.exists(path):
        return "❌ 文件不存在"
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


# ===================================================
# 生成带时间戳强制刷新的 CDN 图片 URL
# ===================================================
def build_cdn_url_with_ts(filepath, url_base):
    if not os.path.exists(filepath):
        return None
    ts = int(os.path.getmtime(filepath))  # 时间戳作为 Notion 缓存刷新参数
    return f"{url_base}?t={ts}"


# -----------------------------
# 构建目录页
# -----------------------------
def build_symbol_directory(symbols):
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")
    directory_id = NOTION_PARENT_PAGE
    clear_directory(directory_id)
    children = []

    for code in symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        img_path = f"docs/{code}/{code}_chipzones_hybrid.png"
        trend_path = f"docs/{code}/{code}_trend_v6.png"

        # 标准 CDN 路径（不带参数）
        csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
        img_base_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png"
        trend_base_url = f"{PAGES_BASE}/{code}/{code}_trend_v6.png"

        # ============================
        # 🔥 强制刷新的 CDN 外链（带时间戳）
        # ============================
        img_url = build_cdn_url_with_ts(img_path, img_base_url)
        trend_url = build_cdn_url_with_ts(trend_path, trend_base_url)

        # 更新时间
        csv_time = get_file_update_time(csv_path)
        img_time = get_file_update_time(img_path)
        last_update = f"📅 Last Updated: CSV={csv_time} | IMG={img_time}"

        # 标题
        children.append(safe_text_block(f"📊 {code} Analysis"))
        children.append(safe_text_block(last_update, "paragraph"))

        # -------- trend_v6 图 (强制刷新) -----------
        if trend_url:
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": trend_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Trend_v6 image not found for {code}", "paragraph"))

        # -------- chipzones 图 (强制刷新) -----------
        if img_url:
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": img_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Chipzones image not found for {code}", "paragraph"))

        # -------- CSV 内容展示 -----------
        if os.path.exists(csv_path):
            with open(csv_path, "r", encoding="utf-8-sig") as f:
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
# 主入口
# -----------------------------
def main():
    print("[push_to_notion] Starting upload process (Skip Database Upload Mode)...")

    # 自动读取所有 config 文件
    config_files = glob.glob("config*.yaml")
    print(f"[INFO] Found config files: {config_files}")

    all_symbols = []

    for config_file in config_files:
        print(f"[INFO] Using config file: {config_file}")
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            raw_symbols = config.get("symbols", [])
            symbols = [s["code"] if isinstance(s, dict) and "code" in s else s for s in raw_symbols]
            all_symbols.extend(symbols)
            print(f"[INFO] Symbols in {config_file}: {symbols}")
        except Exception as e:
            print(f"[ERROR] Failed to read {config_file}: {e}")

    print(f"[INFO] All symbols to include in directory: {all_symbols}")

    build_symbol_directory(all_symbols)

    print("[push_to_notion] ✅ All tasks completed (Database upload skipped).")


if __name__ == "__main__":
    main()
