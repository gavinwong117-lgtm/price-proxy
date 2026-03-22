"""
外汇汇率同步脚本（frankfurter.app）
运行时机：每个工作日 09:00（北京时间），开市前同步
将 USD/CNY、HKD/CNY 写入 Cloudflare KV fx:ALL
"""

import json
import os
import requests
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

TTL = 604800  # 7天托底


def fetch_rates() -> dict:
    """一次请求获取 USD→CNY 和 USD→HKD，推算 HKD→CNY"""
    r = requests.get(
        "https://api.frankfurter.app/latest?from=USD&to=CNY,HKD",
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    date      = data["date"]
    usd_cny   = round(data["rates"]["CNY"], 4)
    usd_hkd   = data["rates"]["HKD"]
    hkd_cny   = round(usd_cny / usd_hkd, 4)
    print(f"[汇率] USD/CNY={usd_cny}  HKD/CNY={hkd_cny}  日期={date}")
    return {
        "USD": {"rate": usd_cny, "date": date},
        "HKD": {"rate": hkd_cny, "date": date},
    }


def write_kv(blob: dict) -> None:
    entry = [{"key": "fx:ALL", "value": json.dumps(blob, ensure_ascii=False), "expiration_ttl": TTL}]
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
        print(f"[{datetime.now():%H:%M:%S}] fx:ALL 写入完成 ✓")
        return
    raise RuntimeError("KV 写入失败：超过最大重试次数")


def main():
    blob = fetch_rates()
    write_kv(blob)
    print(f"[{datetime.now():%H:%M:%S}] 汇率同步完成 ✓")


if __name__ == "__main__":
    main()
