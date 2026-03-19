"""
每日A股价格同步脚本
运行时机：每个交易日 15:35（北京时间）收盘后
将全量A股数据写入 Cloudflare KV，供 Worker 直接命中，省去东方财富实时查询
"""

import akshare as ak
import json
import requests
import os
import sys
from datetime import datetime


CF_ACCOUNT_ID    = os.environ["CF_ACCOUNT_ID"]
CF_NAMESPACE_ID  = os.environ["CF_KV_NAMESPACE_ID"]
CF_API_TOKEN     = os.environ["CF_API_TOKEN"]

KV_BULK_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
    f"/storage/kv/namespaces/{CF_NAMESPACE_ID}/bulk"
)
HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json",
}

TTL = 72000  # 20小时，确保下个交易日开盘前过期


def fetch() -> list[dict]:
    print(f"[{datetime.now():%H:%M:%S}] 拉取新浪A股数据...")
    df = ak.stock_zh_a_spot()
    print(f"[{datetime.now():%H:%M:%S}] 获取到 {len(df)} 条")
    return df.to_dict("records")


def build_entries(rows: list[dict]) -> list[dict]:
    entries = []
    skipped = 0

    for row in rows:
        code_full = str(row.get("代码", "")).lower()   # e.g. sh600519
        name      = str(row.get("名称", "")).strip()
        price_raw = row.get("最新价", 0)
        chg_raw   = row.get("涨跌幅", 0)

        # 跳过停牌或无价格
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 0:
            skipped += 1
            continue

        try:
            chg = float(chg_raw)
        except (TypeError, ValueError):
            chg = 0.0

        # 去掉市场前缀 sh/sz/bj → 纯代码
        if code_full[:2] in ("sh", "sz", "bj"):
            pure_code = code_full[2:]
        else:
            pure_code = code_full

        sign = "+" if chg >= 0 else ""
        value = json.dumps({
            "price":      price,
            "unit":       "元/股",
            "note":       f"{name} 收盘价，较昨收{sign}{chg:.2f}% · 日更",
            "confidence": "high",
            "category":   "stock_cn",
        }, ensure_ascii=False)

        # 存两种 key：纯代码 + 带市场前缀
        for key in (f"stock_cn:{pure_code}", f"stock_cn:{code_full}"):
            entries.append({"key": key, "value": value, "expiration_ttl": TTL})

    print(f"生成 {len(entries)} 条 KV 记录（跳过停牌 {skipped} 条）")
    return entries


def write_kv(entries: list[dict]) -> None:
    chunk_size = 10_000  # CF KV bulk 上限
    total = len(entries)
    for i in range(0, total, chunk_size):
        chunk = entries[i : i + chunk_size]
        resp = requests.put(KV_BULK_URL, headers=HEADERS, data=json.dumps(chunk))
        resp.raise_for_status()
        result = resp.json()
        if not result.get("success"):
            raise RuntimeError(f"KV 写入失败: {result}")
        end = min(i + chunk_size, total)
        print(f"[{datetime.now():%H:%M:%S}] 写入 {i+1}–{end} / {total} 条 ✓")


def main():
    rows    = fetch()
    entries = build_entries(rows)
    write_kv(entries)
    print(f"[{datetime.now():%H:%M:%S}] 同步完成 ✓")


if __name__ == "__main__":
    main()
