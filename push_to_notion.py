import os
import csv
import yaml
from notion_client import Client
import glob
from datetime import datetime

# -------------------------------------------
# 🔥 固定使用 jsDelivr CDN
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


def get_latest_image(pattern):
    """自动匹配: *_YYYYMMDD_HH.png"""
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


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
        print(f"[push_to_notion] 🧹 Cleared {cleared} blocks.")
    except Exception as e:
        print(f"[WARN] Failed to clear directory: {e}")


# -----------------------------
# 获取文件更新时间
# -----------------------------
def get_file_update_time(path):
    if not path or not os.path.exists(path):
        return "❌ 文件不存在"
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


# -----------------------------
# 构建目录页（自动找最新 *_YYYYMMDD_HH.png）
# -----------------------------
def build_symbol_directory(symbols):
    print("[push_to_notion] 🔁 Rebuilding Symbol Directory page...")

    directory_id = NOTION_PARENT_PAGE
    clear_directory(directory_id)

    children = []

    for code in symbols:
        # ===== CSV 文件（不变）=====
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"

        # ===== 寻找最新 chipzones 图 =====
        chip_pattern = f"docs/{code}/{code}_chipzones_hybrid_*.png"
        chip_path = get_latest_image(chip_pattern)

        if chip_path:
            chip_filename = os.path.basename(chip_path)
            img_url = f"{PAGES_BASE}/{code}/{chip_filename}"
        else:
            img_url = None

        # ===== 寻找最新 trend_v6 图 =====
        trend_pattern = f"docs/{code}/{code}_trend_v6_*.png"
        trend_path = get_latest_image(trend_pattern)

        if trend_path:
            trend_filename = os.path.basename(trend_path)
            trend_url = f"{PAGES_BASE}/{code}/{trend_filename}"
        else:
            trend_url = None

        # 更新时间
        csv_time = get_file_update_time(csv_path)
        img_time = get_file_update_time(chip_path)

        # ===== 写入内容 =====
        children.append(safe_text_block(f"📊 {code} Analysis"))
        children.append(safe_text_block(f"📅 Last Updated: CSV={csv_time} | IMG={img_time}", "paragraph"))

        # ===== trend_v6 图 =====
        if trend_url:
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": trend_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Trend_v6 image not found for {code}", "paragraph"))

        # ===== chipzones 图 =====
        if img_url:
            children.append({
                "object": "block",
                "type": "image",
                "image": {"type": "external", "external": {"url": img_url}},
            })
        else:
            children.append(safe_text_block(f"⚠️ Chipzones image not found for {code}", "paragraph"))

        # ===== CSV 展示（不变）=====
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


    # → 一次性推送
    notion.blocks.children.append(directory_id, children=children)

    print(f"[push_to_notion] ✅ Directory rebuilt with {len(symbols)} symbols.")


# -----------------------------
# 主入口
# -----------------------------
def main():
    print("[push_to_notion] Starting upload process...")

    config_files = glob.glob("config*.yaml")
    print(f"[INFO] Found config files: {config_files}")

    all_symbols = []

    for config_file in config_files:
        print(f"[INFO] Using config file: {config_file}")
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        raw_symbols = config.get("symbols", [])
        symbols = [s["code"] if isinstance(s, dict) and "code" in s else s for s in raw_symbols]
        all_symbols.extend(symbols)

    print(f"[INFO] All symbols to include: {all_symbols}")

    build_symbol_directory(all_symbols)

    print("[push_to_notion] 🎉 All tasks completed.")


if __name__ == "__main__":
    main()
