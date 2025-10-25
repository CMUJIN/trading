import os
import csv
import yaml
import time
import glob
import requests
from notion_client import Client
from notion_client.errors import APIResponseError

# -----------------------------
# 基本环境变量
# -----------------------------
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
PAGES_BASE = os.getenv("PAGES_BASE", "https://cmujin.github.io/trading")

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

notion = Client(auth=NOTION_TOKEN)

# -----------------------------
# 通用函数
# -----------------------------
def safe_text_block(content, block_type="heading_2"):
    return {
        "object": "block",
        "type": block_type,
        block_type: {"rich_text": [{"type": "text", "text": {"content": str(content)}}]},
    }

# -----------------------------
# 原生 REST 查询接口（用于兼容性兜底）
# -----------------------------
def _query_database_pages_raw(database_id, start_cursor=None):
    url = f"{NOTION_API_BASE}/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    payload = {}
    if start_cursor:
        payload["start_cursor"] = start_cursor
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

# -----------------------------
# ✅ 清空数据库（SDK + REST 双保险）
# -----------------------------
def clear_database(database_id):
    try:
        print("[push_to_notion] 🧹 Starting full database cleanup...")
        total_deleted = 0
        next_cursor = None
        has_more = True

        while has_more:
            # 优先尝试 SDK，如果失败则走 REST
            try:
                if hasattr(notion.databases, "query"):
                    resp = notion.databases.query(database_id=database_id, start_cursor=next_cursor)
                elif hasattr(notion.databases, "query_database"):
                    resp = notion.databases.query_database(database_id=database_id, start_cursor=next_cursor)
                else:
                    resp = _query_database_pages_raw(database_id, next_cursor)
            except Exception:
                resp = _query_database_pages_raw(database_id, next_cursor)

            results = resp.get("results", [])
            if not results:
                break

            for page in results:
                page_id = page["id"]
                try:
                    notion.pages.update(page_id, archived=True)
                    total_deleted += 1
                    if total_deleted % 50 == 0:
                        print(f"[INFO] Archived {total_deleted} pages so far...")
                except Exception as e:
                    print(f"[WARN] Failed to archive page {page_id}: {e}")
                time.sleep(0.2)

            has_more = resp.get("has_more", False)
            next_cursor = resp.get("next_cursor")

        print(f"[push_to_notion] ✅ Cleared total {total_deleted} entries from database.")
    except Exception as e:
        print(f"[ERROR] Failed to clear database: {e}")

# -----------------------------
# ✅ 自动补齐数据库字段（软失败模式）
# -----------------------------
def ensure_properties_exist(database_id, fieldnames, soft_fail=True):
    try:
        db = notion.databases.retrieve(database_id)
        props = db.get("properties", None)
        if props is None and "results" in db and db["results"]:
            props = db["results"][0].get("properties", {})

        if not props:
            print(f"[WARN] Unable to retrieve database properties for {database_id}")
            return

        existing_props = props.keys()
        for name in fieldnames:
            clean_name = name.strip().replace("﻿", "")
            if clean_name not in existing_props:
                try:
                    notion.databases.update(
                        database_id=database_id,
                        properties={clean_name: {"rich_text": {}}}
                    )
                    print(f"[push_to_notion] ➕ Added missing property: {clean_name}")
                except Exception as e:
                    if soft_fail:
                        print(f"[SOFT-WARN] Could not add property '{clean_name}': {e}")
                    else:
                        raise

        print("[push_to_notion] ✅ 数据库字段已自动补齐（软失败模式启用）。")
    except Exception as e:
        if soft_fail:
            print(f"[SOFT-WARN] Failed to update database properties: {e}")
        else:
            raise

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
# 构建目录页（加时间戳刷新图片）
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
        img_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png?ver={int(time.time())}"

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
# 上传 CSV 数据到数据库
# -----------------------------
def upsert_rows(code, csv_path):
    csv_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.csv"
    img_url = f"{PAGES_BASE}/{code}/{code}_chipzones_hybrid.png?ver={int(time.time())}"

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        ensure_properties_exist(NOTION_DB, reader.fieldnames, soft_fail=True)
        for row in reader:
            props = {
                "Name": {"title": [{"text": {"content": f"{code} Analysis"}}]},
                "CSV": {"url": csv_url},
                "Image": {"url": img_url},
            }
            for k, v in row.items():
                clean_key = k.strip().replace("﻿", "")
                props[clean_key] = {"rich_text": [{"text": {"content": str(v)}}]}
            try:
                notion.pages.create(parent={"database_id": NOTION_DB}, properties=props)
            except APIResponseError as e:
                print(f"[WARN] Failed row for {code}: {e}")
    print(f"[push_to_notion] ✅ Uploaded rows for {code}")

# -----------------------------
# 主入口
# -----------------------------
def main():
    print("[push_to_notion] Starting upload process...")

    if NOTION_DB:
        clear_database(NOTION_DB)
    else:
        print("[WARN] NOTION_DB not set, skipping clear.")

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
            print(f"[ERROR] Error reading {config_file}: {e}")

    print(f"[INFO] All symbols to upload: {all_symbols}")

    for code in all_symbols:
        csv_path = f"docs/{code}/{code}_chipzones_hybrid.csv"
        if os.path.exists(csv_path):
            upsert_rows(code, csv_path)
        else:
            print(f"[WARN] CSV not found for {code}: {csv_path}")

    build_symbol_directory(all_symbols)
    print("[push_to_notion] ✅ All tasks completed.")

if __name__ == "__main__":
    main()
