#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
push_to_notion.py (Notion Compatible Version)
--------------------------------------------------------
✅ 使用 raw.githubusercontent.com → Notion 稳定显示图片
✅ 每张图片自动附加更新时间戳 ?t=xxxx → 强制刷新
✅ 完整保留你原有所有逻辑结构
"""

import os
import csv
import yaml
from notion_client import Client
import glob
from datetime import datetime

# -------------------------------------------
# 🚀 使用 raw.githubusercontent（Notion 100% 支持）
# -------------------------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")

# 旧版（不兼容 Notion）：
# PAGES_BASE = "https://cdn.jsdelivr.net/gh/CMUJIN/trading@main/docs"

# 新版（100% Notion 兼容）
PAGES_BASE = "https://raw.githubusercontent.com/CMUJIN/trading/main/docs"

notion = Client(auth=NOTION_TOKEN)


# -----------------------------
# 工具函数
# -----------------------------
def safe_text_block(content, block_type="heading_2"):
    return {
        "object": "block",
        "type": block_type,
        block_type: {
            "rich_text": [
                {"type": "text", "text": {"content": str(content)}}
            ]
        },
    }


def clear_directory(directory_id):
    """清空父页面内容，但保留子页面/数据库"""
    try:
        children = notion.blocks.children.list(directory_id)["results"]
        cleared = 0
        for child in children:
            if child["type"] in ("child_page", "child_database"):
                print(f"[SAFE MODE] Skip {child['type']} {child['id']}")
                continue
            notion.blocks.delete(child["id"])
            cleared += 1
        print(f"[push_to_notion] 🧹 Cleared {cleared} blocks.")
    except Exception as e:
        print(f"[WARN] Failed to clear directory: {e}")


def get_file_update_time(path):
    if not os.path.exists(path):
        return "❌ File Not Found"
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def build_url_with_ts(filepath, base_url):
    """生成带时间戳的 URL 强制刷新"""
    if not os.path.exists(filepath):
        return None
    ts = int(os.path.getmtime(filepath))
    return f"{base_url}?t={ts}"


# -----------------------------
# 构建目录页
# -----------------------------
def build_symbol_directory(symbols):
    print("[push_to_notion] Building Symbol Directory...")
    directory_id = NOTION_PARENT_PAGE

    clear_directory(directory_id)
    children = []

    for code in symbols:
        # ================= PATHS =================
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        chip_path = f"docs/{code}/{code}_chipzones_hybrid.png"
        trend_path = f"docs/{code}/{code}_trend_v6.png"

        # ================= RAW BASE URLs =================
        csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
        chip_base_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png"
        trend_base_url = f"{PAGES_BASE}/{code}/{code}_trend_v6.png"

        # ================= 强制刷新 URL =================
        chip_url = build_url_with_ts(chip_path, chip_base_url)
        trend_url = build_url_with_ts(trend_path, trend_base_url)

        # 更新时间显示
        csv_time = get_file_update_time(csv_path)
        chip_time = get_file_update_time(chip_path)

        children.append(safe_text_block(f"📈 {code} Analysis"))
        children.append(
            safe_text_block(
                f"📅 Last Updated: CSV={csv_time} | IMG={chip_time}", "paragraph"
            )
        )

        # ------------ Trend_v6 图 ------------
        if trend_url:
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": trend_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ No Trend_v6 for {code}", "paragraph"))

        # ------------ Chipzones 图 ------------
        if chip_url:
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": chip_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Chipzones Image Missing ({code})", "paragraph"))

        # ------------ CSV 表格展示 ------------
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
            children.append(safe_text_block(f"⚠️ CSV Missing: {code}", "paragraph"))

    # 全部追加到 Notion 页面
    notion.blocks.children.append(directory_id, children=children)
    print(f"[push_to_notion] ✅ Directory updated with {len(symbols)} symbols.")


# -----------------------------
# 主程序
# -----------------------------
def main():
    print("[push_to_notion] Starting...")

    config_files = glob.glob("config*.yaml")
    print(f"[INFO] Found config: {config_files}")

    all_symbols = []

    for config_file in config_files:
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
            raw_symbols = config.get("symbols", [])
            symbols = [
                s["code"] if isinstance(s, dict) and "code" in s else s
                for s in raw_symbols
            ]
            all_symbols.extend(symbols)
            print(f"[INFO] {config_file}: {symbols}")
        except Exception as e:
            print(f"[ERROR] Failed to read {config_file}: {e}")

    build_symbol_directory(all_symbols)
    print("[push_to_notion] 🎉 All Done!")


if __name__ == "__main__":
    main()
