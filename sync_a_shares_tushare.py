"""
A股日线托底脚本（Tushare）
运行时机：每个交易日 19:00（北京时间）
使用 Tushare 官方收盘价覆盖 stock_cn:ALL，作为当日最终数据
"""

import tushare as ts
import json
import requests
import os
import time
from datetime import datetime


CF_ACCOUNT_ID   = os.environ["CF_ACCOUNT_ID"]
CF_NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
CF_API_TOKEN    = os.environ["CF_API_TOKEN"]
TUSHARE_TOKEN   = os.environ["TUSHARE_TOKEN"]

KV_BULK_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
    f"/storage/kv/namespaces/{CF_NAMESPACE_ID}/bulk"
)
HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json",
}

TTL = 604800  # 7天；同步成功则覆盖，失败则旧数据托底一周


def fetch():
    print(f"[{datetime.now():%H:%M:%S}] 连接 Tushare...")
    pro = ts.pro_api(TUSHARE_TOKEN)

    today = datetime.now().strftime('%Y%m%d')
    print(f"[{datetime.now():%H:%M:%S}] 拉取 {today} 日线数据...")
    df_daily = pro.daily(trade_date=today)
    if df_daily is None or df_daily.empty:
        raise RuntimeError(f"今日({today})无交易数据，可能是非交易日")
    print(f"[{datetime.now():%H:%M:%S}] 获取到 {len(df_daily)} 条日线数据")

    print(f"[{datetime.now():%H:%M:%S}] 拉取股票名称...")
    df_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
    name_map = dict(zip(df_basic['ts_code'], df_basic['name']))
    print(f"[{datetime.now():%H:%M:%S}] 获取到 {len(name_map)} 条名称映射")

    return df_daily, name_map


def build_blob(df_daily, name_map):
    blob = {}
    skipped = 0

    for _, row in df_daily.iterrows():
        ts_code   = row['ts_code']          # e.g. 000001.SZ
        pure_code = ts_code.split('.')[0]   # → 000001
        name      = name_map.get(ts_code, pure_code)
        price     = float(row.get('close', 0) or 0)
        pct_chg   = float(row.get('pct_chg', 0) or 0)

        if price <= 0:
            skipped += 1
            continue

        sign = '+' if pct_chg >= 0 else ''
        blob[pure_code] = {
            'price':      price,
            'unit':       '元/股',
            'note':       f'{name} 收盘价，较昨收{sign}{pct_chg:.2f}% · 日更',
            'confidence': 'high',
            'name':       name,
        }

    print(f"生成 {len(blob)} 条数据（跳过 {skipped} 条）")
    return blob


def write_kv(blob):
    value = json.dumps(blob, ensure_ascii=False)
    size_kb = len(value.encode()) / 1024
    print(f"[{datetime.now():%H:%M:%S}] 写入 stock_cn:ALL，{len(blob)} 条，{size_kb:.1f} KB")

    entry = [{"key": "stock_cn:ALL", "value": value, "expiration_ttl": TTL}]
    for attempt in range(5):
        resp = requests.put(KV_BULK_URL, headers=HEADERS, data=json.dumps(entry))
        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"限速 429，等待 {wait}s 后重试...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        result = resp.json()
        if not result.get('success'):
            raise RuntimeError(f"KV 写入失败: {result}")
        print(f"[{datetime.now():%H:%M:%S}] 写入完成 ✓")
        return
    raise RuntimeError("KV 写入失败：超过最大重试次数")


def main():
    df_daily, name_map = fetch()
    blob = build_blob(df_daily, name_map)
    write_kv(blob)
    print(f"[{datetime.now():%H:%M:%S}] 同步完成 ✓")


if __name__ == '__main__':
    main()
