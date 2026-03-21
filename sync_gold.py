"""
黄金价格同步脚本（上海黄金交易所）
运行时机：每个工作日 08:00（北京时间），早盘前同步前一日收盘价
将 Au99.99 最新价写入 Cloudflare KV 单条记录（gold:ALL）
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
SYMBOL = "Au99.99"


def fetch() -> float:
    print(f"[{datetime.now():%H:%M:%S}] 拉取上金所 {SYMBOL} 实时行情...")
    df = ak.spot_quotations_sge(symbol=SYMBOL)
    # 取最后一条（最新分钟价）
    latest = df.iloc[-1]
    price = float(latest["现价"])
    update_time = latest["更新时间"]
    print(f"[{datetime.now():%H:%M:%S}] {SYMBOL} 最新价 {price} 元/克（{update_time}）")
    return price


def build_blob(price: float) -> dict:
    return {
        SYMBOL: {
            "price":      price,
            "unit":       "元/克",
            "note":       f"上金所 Au99.99 现货价 · 日更",
            "confidence": "high",
            "name":       "黄金 Au99.99",
        }
    }


def write_kv(blob: dict) -> None:
    value = json.dumps(blob, ensure_ascii=False)
    print(f"[{datetime.now():%H:%M:%S}] 写入 gold:ALL...")

    entry = [{"key": "gold:ALL", "value": value, "expiration_ttl": TTL}]
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
    price = fetch()
    blob = build_blob(price)
    write_kv(blob)
    print(f"[{datetime.now():%H:%M:%S}] 同步完成 ✓")


if __name__ == "__main__":
    main()
