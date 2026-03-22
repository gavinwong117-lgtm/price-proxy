"""
A股实时价格推送脚本（掘金量化 → Cloudflare KV）
配合掘金量化主脚本使用，每5分钟读取 current_prices.csv 更新 stock_cn:ALL
用法：在掘金量化脚本运行的同一台电脑上单独启动本脚本

需提前在系统环境变量中设置：
  CF_ACCOUNT_ID
  CF_KV_NAMESPACE_ID
  CF_API_TOKEN
"""

import requests
import json
import os
import time
import pandas as pd
from datetime import datetime

CF_ACCOUNT_ID   = os.environ["CF_ACCOUNT_ID"]
CF_NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
CF_API_TOKEN    = os.environ["CF_API_TOKEN"]

KV_BASE_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
    f"/storage/kv/namespaces/{CF_NAMESPACE_ID}"
)
BULK_URL = f"{KV_BASE_URL}/bulk"
HEADERS  = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}

CSV_PATH      = r"D:\syncthing\newprice\current_prices.csv"
KV_KEY        = "stock_cn:ALL"
PUSH_INTERVAL = 5 * 60   # 5分钟推一次
TTL           = 604800    # 7天


def load_blob_from_kv() -> dict:
    """从KV读取现有blob，保留name/note/confidence等元数据"""
    try:
        r = requests.get(f"{KV_BASE_URL}/values/{KV_KEY}", headers=HEADERS, timeout=15)
        if r.status_code == 200:
            blob = json.loads(r.text)
            print(f"[{ts()}] 从KV加载 {len(blob)} 条基础数据")
            return blob
        print(f"[{ts()}] KV读取返回 {r.status_code}，从空白开始")
    except Exception as e:
        print(f"[{ts()}] KV读取失败: {e}")
    return {}


def update_prices(blob: dict, csv_path: str) -> int:
    """用CSV里的最新价更新blob中的price字段，返回更新条数"""
    df = pd.read_csv(csv_path, dtype={"代码": str})
    updated = 0
    for _, row in df.iterrows():
        code = str(row["代码"]).strip().zfill(6)
        try:
            price = float(row["最新价"])
        except (ValueError, TypeError):
            continue
        if price > 0 and code in blob:
            blob[code]["price"] = round(price, 4)
            updated += 1
    return updated


def push_blob(blob: dict) -> bool:
    """将blob写入KV"""
    value = json.dumps(blob, ensure_ascii=False)
    size_kb = len(value.encode()) / 1024
    entry = [{"key": KV_KEY, "value": value, "expiration_ttl": TTL}]
    try:
        r = requests.put(BULK_URL, headers=HEADERS, data=json.dumps(entry), timeout=30)
        result = r.json()
        if result.get("success"):
            print(f"[{ts()}] KV写入完成 ✓ ({size_kb:.0f} KB)")
            return True
        print(f"[{ts()}] KV写入失败: {result}")
    except Exception as e:
        print(f"[{ts()}] KV写入异常: {e}")
    return False


def is_trading_time() -> bool:
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    if h == 9 and m >= 30: return True
    if 9 < h < 11: return True
    if h == 11 and m <= 30: return True
    if h == 13: return True
    if 13 < h < 15: return True
    if h == 15 and m == 0: return True
    return False


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def main():
    print(f"[{ts()}] 启动 A股实时推送脚本...")
    blob = load_blob_from_kv()

    last_push = 0.0
    last_csv_mtime = 0.0

    while True:
        try:
            #if not is_trading_time():
            #    time.sleep(30)
            #    continue

            if not os.path.exists(CSV_PATH):
                print(f"[{ts()}] CSV不存在，等待掘金量化写入...")
                time.sleep(30)
                continue

            # 只有CSV有更新才重新读取
            csv_mtime = os.path.getmtime(CSV_PATH)
            if csv_mtime != last_csv_mtime:
                updated = update_prices(blob, CSV_PATH)
                last_csv_mtime = csv_mtime
                print(f"[{ts()}] CSV更新，同步 {updated} 条价格")

            # 每5分钟推一次
            now = time.time()
            if now - last_push >= PUSH_INTERVAL:
                push_blob(blob)
                last_push = now

        except Exception as e:
            print(f"[{ts()}] 错误: {e}")

        time.sleep(10)


if __name__ == "__main__":
    main()
