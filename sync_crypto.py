"""
主流加密货币价格同步脚本（CoinGecko）
运行时机：每天 09:00（北京时间）
将主流币 CNY 价格写入 Cloudflare KV crypto:ALL
"""

import json
import os
import requests
import time
from datetime import datetime, timezone, timedelta


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

TTL = 86400  # 24小时

# CoinGecko ID → APP 侧 ticker（用户输入的代码）
COINS = {
    "bitcoin":        "BTC",
    "ethereum":       "ETH",
    "tether":         "USDT",
    "binancecoin":    "BNB",
    "solana":         "SOL",
    "ripple":         "XRP",
    "dogecoin":       "DOGE",
    "usd-coin":       "USDC",
    "cardano":        "ADA",
    "tron":           "TRX",
}

# ticker → 中文名
COIN_NAMES = {
    "BTC":  "比特币",
    "ETH":  "以太坊",
    "USDT": "泰达币",
    "BNB":  "币安币",
    "SOL":  "索拉纳",
    "XRP":  "瑞波币",
    "DOGE": "狗狗币",
    "USDC": "USD Coin",
    "ADA":  "卡尔达诺",
    "TRX":  "波场",
}


def fetch_prices() -> dict:
    ids = ",".join(COINS.keys())
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=cny&include_24hr_change=true"
    )
    r = requests.get(url, timeout=15, headers={"Accept": "application/json"})
    r.raise_for_status()
    data = r.json()

    today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    blob = {}
    for coin_id, ticker in COINS.items():
        entry = data.get(coin_id)
        if not entry or not entry.get("cny"):
            print(f"[跳过] {ticker} 无数据")
            continue
        price = entry["cny"]
        change = round(entry.get("cny_24h_change", 0), 2)
        name = COIN_NAMES.get(ticker, ticker)
        blob[ticker] = {
            "price": price,
            "unit": "元/枚",
            "note": f"{name} 较昨日{'+' if change >= 0 else ''}{change}%",
            "confidence": "high",
            "name": name,
            "date": today,
        }
        print(f"[{ticker}] {name} ¥{price:,.2f}  {change:+.2f}%")

    return blob


def write_kv(blob: dict) -> None:
    entry = [{"key": "crypto:ALL", "value": json.dumps(blob, ensure_ascii=False), "expiration_ttl": TTL}]
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
        print(f"[{datetime.now():%H:%M:%S}] crypto:ALL 写入完成 ({len(blob)} 条) ✓")
        return
    raise RuntimeError("KV 写入失败：超过最大重试次数")


def main():
    print(f"[{datetime.now():%H:%M:%S}] 开始同步加密货币价格...")
    blob = fetch_prices()
    if not blob:
        raise RuntimeError("未获取到任何价格数据")
    write_kv(blob)
    print(f"[{datetime.now():%H:%M:%S}] 加密货币同步完成 ✓")


if __name__ == "__main__":
    main()
