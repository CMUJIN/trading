#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_to_notion_v2.0_single_db.py
---------------------------------------
功能：
✅ 程序启动时仅创建一个数据库
✅ 所有品种写入同一个数据库
✅ 每次执行清空旧记录再上传
✅ 全字段文本兼容
✅ 自动过滤 /docs 路径
✅ 支持批量扫描 docs 下所有品种
"""

import os
import csv
import re
from notion_client import Client
from notion_client.errors import APIResponseError

# ========== 环境变量 ==========
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE = os.getenv("NOTION_PARENT_PAGE")
NOTION_DB = os.getenv("NOTION_DB", "").strip()
PAGES_BASE = os.getenv("PAGES_BASE", "").strip().rstrip("/")
PAGES_BASE = PAGES_BASE.replace("/docs", "")  # ✅ 自动去掉 /docs

# 初始化 Notion 客户端
notion = Client(auth=NOTION_TOKEN)


# ========== 工具函数 ==========
def is_valid_uuid(uid: str) -> bool:
    """判断字符串是否是有效 UUID"""
    return bool(re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", uid))


# ========== 创建 / 复用数据库 ==========
def ensure_database(fieldnames):
    """确保数据库存在，不重复创建"""
    global NOTION_DB

    # ✅ 优先使用本地缓存
    if os.path.exists("notion_db_id.txt"):
        with open("notion_db_id.txt", "r") as f:
            dbid = f.read().strip()
            if is_valid_uuid(dbid):
                print(f"[push_to_notion] ✅ Using existing database (local): {dbid}")
                NOTION_DB = dbid
                return dbid

    # ✅ 检查环境变量
    if is_valid_uuid(NOTION_DB):
        print(f"[push_to_notion] ✅ Using NOTION_DB from env: {NOTION_DB}")
        return NOTION_DB
    else:
        print(f"[push_to_notion] ⚠️ Invalid or missing NOTION_DB ({NOTION_DB}), creating new database...")

    # ✅ 创建新数据库
    if not NOTION_PARENT_PAGE:
        raise ValueError("❌ 未设置 NOTION_PARENT_PAGE 环境变量")

    props = {
        "Name": {"title": {}},
        "Symbol": {"rich_text": {}},
        "Image": {"url": {}},
        "CSV": {"url": {}},
    }
    for f in fieldnames:
        if f not in props:
            props[f] = {"rich_text": {}}

    db = notion.databases.create(
        parent={"page_id": NOTION_PARENT_PAGE},
        title=[{"type": "text", "text": {"content": "Futures Chip Analysis (Unified)"}}],
        properties=props,
    )

    dbid = db["id"]
    NOTION_DB = dbid
    with open("notion_db_id.txt", "w") as f:
        f.write(dbid)
    print(f"[push_to_notion] ✅ Created new unified database: {dbid}")
    return dbid


# ========== 清空数据库 ==========
def clear_database(dbid):
    """归档数据库中所有旧页面"""
    try:
        results = notion.databases.query(database_id=dbid).get("results", [])
        for page in results:
            notion.pages.update(page_id=page["id"], archived=True)
        print(f"[push_to_notion] 🧹 Cleared {len(results)} old records")
    except Exception as e:
        print(f"[push_to_notion] ⚠️ Failed to clear old records: {e}")


# ========== 上传数据 ==========
def upload_symbol(symbol, dbid):
    """上传单个品种数据"""
    local_csv = f"./docs/{symbol}/{symbol}_chipzones_hybrid.csv"
    local_png = f"./docs/{symbol}/{symbol}_chipzones_hybrid.png"

    if not os.path.exists(local_csv):
        print(f"[skip] ❌ 没有找到CSV文件: {local_csv}")
        return 0, 0

    csv_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.csv"
    png_url = f"{PAGES_BASE}/{symbol}/{symbol}_chipzones_hybrid.png"

    with open(local_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        success, fail = 0, 0
        for row in reader:
            try:
                props = make_properties(row, symbol, png_url, csv_url)
                notion.pages.create(parent={"database_id": dbid}, properties=props)
                success += 1
            except APIResponseError as e:
                print(f"[WARN] Failed row in {symbol}: {e}")
                fail += 1

        print(f"[push_to_notion] ✅ {symbol}: Uploaded {success} rows, ❌ Failed {fail}")
        return success, fail


def make_properties(row, symbol, png_url, csv_url):
    """构造 Notion 属性"""
    props = {
        "Name": {"title": [{"type": "text", "text": {"content": f"{symbol} 筹码分析"}}]},
        "Symbol": {"rich_text": [{"type": "text", "text": {"content": symbol}}]},
        "Image": {"url": png_url},
        "CSV": {"url": csv_url},
    }
    for k, v in row.items():
        if k not in props:
            props[k] = {"rich_text": [{"type": "text", "text": {"content": str(v)}}]}
    return props


# ========== 主入口 ==========
def main():
    base_dir = "./docs"
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]

    if not symbols:
        print("❌ 未找到任何品种目录，请确认 docs/ 下存在合约文件夹")
        return

    # 用第一个 CSV 的字段结构创建一次数据库
    first_csv = f"{base_dir}/{symbols[0]}/{symbols[0]}_chipzones_hybrid.csv"
    if not os.path.exists(first_csv):
        print(f"❌ 找不到初始 CSV 文件: {first_csv}")
        return
    with open(first_csv, "r", encoding="utf-8") as f:
        fieldnames = csv.DictReader(f).fieldnames

    dbid = ensure_database(fieldnames)
    clear_database(dbid)

    total_success, total_fail = 0, 0
    for symbol in symbols:
        print(f"\n[push_to_notion] 🚀 开始上传 {symbol} ...")
        s, f_ = upload_symbol(symbol, dbid)
        total_success += s
        total_fail += f_

    print(f"\n✅ 全部完成，共上传 {total_success} 条，失败 {total_fail} 条。数据库ID: {dbid}")


if __name__ == "__main__":
    main()
