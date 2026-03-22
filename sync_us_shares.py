"""
美股价格同步脚本（新浪）
运行时机：每个工作日 02:00（北京时间）
将全量美股数据写入 Cloudflare KV 单条记录（stock_us:ALL）
价格以美元原币存储，汇率换算由 APP 显示层负责
"""

import akshare as ak
import json
import requests
import os
import time
from datetime import datetime


CF_ACCOUNT_ID   = os.environ["CF_ACCOUNT_ID"]
CF_NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
CF_API_TOKEN    = os.environ["CF_API_TOKEN"]

KV_BULK_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
    f"/storage/kv/namespaces/{CF_NAMESPACE_ID}/bulk"
)
HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json",
}

TTL = 604800  # 7天；同步成功则覆盖，失败则旧数据托底一周


def fetch() -> list[dict]:
    print(f"[{datetime.now():%H:%M:%S}] 拉取新浪美股数据...")
    df = ak.stock_us_spot()
    print(f"[{datetime.now():%H:%M:%S}] 获取到 {len(df)} 条")
    return df.to_dict("records")


def build_blob(rows: list[dict]) -> dict:
    blob = {}
    skipped = 0

    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            skipped += 1
            continue

        cname = str(row.get("cname", "")).strip()
        name  = str(row.get("name", "")).strip()
        display_name = cname if cname else name

        try:
            usd_price = float(row.get("price", 0) or 0)
        except (TypeError, ValueError):
            usd_price = 0.0
        if usd_price <= 0:
            skipped += 1
            continue

        try:
            chg = float(row.get("chg", 0) or 0)
        except (TypeError, ValueError):
            chg = 0.0

        sign = "+" if chg >= 0 else ""
        blob[symbol] = {
            "price":      usd_price,
            "unit":       "美元/股",
            "note":       f"{display_name} 较昨收{sign}{chg:.2f}% · 日更",
            "confidence": "high",
            "name":       display_name,
        }

    print(f"生成 {len(blob)} 条数据（跳过 {skipped} 条）")
    return blob


def write_kv(blob: dict) -> None:
    value = json.dumps(blob, ensure_ascii=False)
    size_kb = len(value.encode()) / 1024
    print(f"[{datetime.now():%H:%M:%S}] 写入 stock_us:ALL，{len(blob)} 条，{size_kb:.1f} KB")

    entry = [{"key": "stock_us:ALL", "value": value, "expiration_ttl": TTL}]
    for attempt in range(5):
        resp = requests.put(KV_BULK_URL, headers=HEADERS, data=json.dumps(entry))
        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"限速 429，等待 {wait}s 后重试...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        result = resp.json()
        if not result.get("success"):
            raise RuntimeError(f"KV 写入失败: {result}")
        print(f"[{datetime.now():%H:%M:%S}] 写入完成 ✓")
        return
    raise RuntimeError("KV 写入失败：超过最大重试次数")


def main():
    rows = fetch()
    blob = build_blob(rows)
    write_kv(blob)
    print(f"[{datetime.now():%H:%M:%S}] 同步完成 ✓")


if __name__ == "__main__":
    main()
