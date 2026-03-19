"""
每日港股价格同步脚本
运行时机：每个交易日 23:00（北京时间）
将全量港股数据写入 Cloudflare KV
"""

import akshare as ak
import json
import requests
import os
import time
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

TTL = 72000  # 20小时


def fetch() -> list[dict]:
    print(f"[{datetime.now():%H:%M:%S}] 拉取新浪港股数据...")
    df = ak.stock_hk_spot()
    print(f"[{datetime.now():%H:%M:%S}] 获取到 {len(df)} 条")
    return df.to_dict("records")


def build_entries(rows: list[dict]) -> list[dict]:
    entries = []
    skipped = 0

    for row in rows:
        code  = str(row.get("代码", "")).strip()   # e.g. 00700
        name  = str(row.get("中文名称", "")).strip()
        price_raw = row.get("最新价", 0)
        chg_raw   = row.get("涨跌幅", 0)

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

        sign = "+" if chg >= 0 else ""
        value = json.dumps({
            "price":      price,
            "unit":       "港元/股",
            "note":       f"{name} 收盘价，较昨收{sign}{chg:.2f}% · 日更",
            "confidence": "high",
            "category":   "stock_hk",
        }, ensure_ascii=False)

        entries.append({"key": f"stock_hk:{code}", "value": value, "expiration_ttl": TTL})

    print(f"生成 {len(entries)} 条 KV 记录（跳过停牌 {skipped} 条）")
    return entries


def write_kv_chunk(chunk: list[dict], retries: int = 5) -> None:
    for attempt in range(retries):
        resp = requests.put(KV_BULK_URL, headers=HEADERS, data=json.dumps(chunk))
        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"限速 429，等待 {wait}s 后重试...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        result = resp.json()
        if not result.get("success"):
            raise RuntimeError(f"KV 写入失败: {result}")
        return
    raise RuntimeError("KV 写入失败：超过最大重试次数")


def write_kv(entries: list[dict]) -> None:
    chunk_size = 10_000
    total = len(entries)
    for i in range(0, total, chunk_size):
        chunk = entries[i : i + chunk_size]
        write_kv_chunk(chunk)
        end = min(i + chunk_size, total)
        print(f"[{datetime.now():%H:%M:%S}] 写入 {i+1}–{end} / {total} 条 ✓")


def main():
    rows    = fetch()
    entries = build_entries(rows)
    write_kv(entries)
    print(f"[{datetime.now():%H:%M:%S}] 同步完成 ✓")


if __name__ == "__main__":
    main()
