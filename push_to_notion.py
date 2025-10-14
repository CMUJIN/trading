# -*- coding: utf-8 -*-
"""
push_to_notion.py
----------------------------------------
自动上传分析结果 (CSV + PNG 链接) 到 Notion 数据库
支持：
- 自动创建数据库（字段类型自动匹配）
- 自动类型识别（数字/文本）
- 兼容 GitHub Actions 环境
----------------------------------------
依赖: pip install notion-client pyyaml
"""

import os
import csv
import traceback
from notion_client import Client

# 从环境变量读取配置
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB = os.getenv("NOTION_DB", "").strip()
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").rstrip("/")

# 初始化 Notion 客户端
notion = Client(auth=NOTION_TOKEN)


# =============================
# 创建数据库
# =============================
def ensure_database(fieldnames):
    """若数据库不存在则自动创建"""
    if NOTION_DB and not NOTION_DB.lower().startswith("placeholder"):
        print(f"[push_to_notion] Using existing DB: {NOTION_DB}")
        return NOTION_DB

    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 未设置 NOTION_PARENT_PAGE 环境变量")

    print("[push_to_notion] Creating new Notion database...")

    # 自动识别字段类型（数字/文本）
    props = {
        "Name": {"title": {}},
        "Symbol": {"rich_text": {}},
        "Image": {"url": {}},
        "CSV": {"url": {}}
    }

    for f in fieldnames:
        # 主字段避免重复
        if f in props:
            continue
        # 默认先设为数字型，后续写入时会自动修正
        props[f] = {"number": {}}

    db = notion.databases.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis"}}],
        properties=props,
    )
    dbid = db["id"]
    print(f"[push_to_notion] Created database: {dbid}")
    return dbid


# =============================
# 类型安全属性生成
# =============================
def to_property(value):
    """自动识别 Notion 属性类型"""
    if value is None:
        return {"rich_text": [{"text": {"content": ""}}]}
    try:
        # 尝试转数字
        if isinstance(value, (int, float)):
            return {"number": float(value)}
        v_str = str(value).strip()
        if v_str.replace(".", "", 1).isdigit():
            return {"number": float(v_str)}
        # 默认文本
        return {"rich_text": [{"text": {"content": v_str}}]}
    except Exception:
        return {"rich_text": [{"text": {"content": str(value)}}]}


# =============================
# 写入一条记录
# =============================
def upsert_rows(symbol, png_url, csv_file):
    with open(csv_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        dbid = ensure_database(reader.fieldnames)
        count = 0
        for row in reader:
            try:
                props = {}
                for k, v in row.items():
                    props[k] = to_property(v)
                # 附加公共字段
                props["Name"] = {"title": [{"text": {"content": f"{symbol}"} }]}
                props["Symbol"] = {"rich_text": [{"text": {"content": symbol}}]}
                props["Image"] = {"url": f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.png"}
                props["CSV"] = {"url": f"{PAGES_BASE}/docs/{symbol}/{symbol}_chipzones_hybrid.csv"}

                notion.pages.create(parent={"database_id": dbid}, properties=props)
                count += 1
            except Exception as e:
                print(f"[WARN] Failed row: {row.get('date', '?')} | {e}")
                traceback.print_exc()

        print(f"[push_to_notion] ✅ Uploaded {count} rows to Notion.")


# =============================
# 主入口
# =============================
def main():
    if not NOTION_TOKEN:
        raise ValueError("❌ 未设置 NOTION_TOKEN 环境变量")

    # 寻找 CSV 和 PNG
    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]
    if not csv_files:
        print("⚠️ No CSV file found.")
        return

    for csvf in csv_files:
        symbol = csvf.split("_")[0].upper()
        png_path = f"{symbol}_chipzones_hybrid.png"
        if not os.path.exists(png_path):
            print(f"⚠️ PNG not found for {symbol}, skipping image link.")
        upsert_rows(symbol, png_path, csvf)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Fatal Error: {e}")
        traceback.print_exc()
        exit(1)
