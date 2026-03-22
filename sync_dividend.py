"""
分红/送股/转增同步脚本（百度财经）
运行时机：每个工作日 16:30（北京时间），收盘后同步当日除权数据
向后回溯3天，防止周末或漏跑导致数据缺失
将数据写入 Cloudflare KV dividend:ALL，保留最近30天
"""

import akshare as ak
import json
import os
import re
import requests
import time
from datetime import datetime, timedelta


CF_ACCOUNT_ID   = os.environ["CF_ACCOUNT_ID"]
CF_NAMESPACE_ID = os.environ["CF_KV_NAMESPACE_ID"]
CF_API_TOKEN    = os.environ["CF_API_TOKEN"]

KV_KEY    = "dividend:ALL"
KEEP_DAYS = 90
TTL       = 60 * 60 * 24 * 95  # 95天托底

HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json",
}
KV_BASE = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/storage/kv/namespaces/{CF_NAMESPACE_ID}"


def parse_cash(val) -> tuple:
    """'2.50元' → (2.50, 'CNY')，'0.21港元' → (0.21, 'HKD')，'-'/NaN → (None, '')"""
    if not val or str(val).strip() in ("-", "nan", "NaN", ""):
        return None, ""
    s = str(val).strip()
    if "港元" in s:
        num = re.sub(r"[^\d.]", "", s.replace("港元", ""))
        return (float(num), "HKD") if num else (None, "")
    elif "元" in s:
        num = re.sub(r"[^\d.]", "", s.replace("元", ""))
        return (float(num), "CNY") if num else (None, "")
    return None, ""


def parse_shares(val) -> float | None:
    """'10股转增3.00股' 或 '10股送3.00股' → 3.0，'-'/NaN → None"""
    if not val:
        return None
    s = str(val).strip()
    if s in ("-", "nan", "NaN", ""):
        return None
    m = re.search(r"([\d.]+)股$", s)
    return float(m.group(1)) if m else None


def fetch_day(date_str: str) -> list:
    """拉取指定日期（YYYYMMDD）的分红数据，返回标准化 list"""
    try:
        df = ak.news_trade_notify_dividend_baidu(date=date_str)
        if df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            code     = str(row.get("股票代码", "")).strip()
            exchange = str(row.get("交易所", "")).strip()
            name     = str(row.get("股票简称", "")).strip()
            date     = str(row.get("除权日", "")).strip()

            cash, cash_unit = parse_cash(row.get("分红"))
            bonus           = parse_shares(row.get("送股"))
            transfer        = parse_shares(row.get("转增"))

            if cash is None and bonus is None and transfer is None:
                continue

            rec = {"code": code, "exchange": exchange, "name": name, "date": date}
            if cash is not None:
                rec["cash"]     = cash
                rec["cashUnit"] = cash_unit
            if bonus is not None:
                rec["bonus"] = bonus
            if transfer is not None:
                rec["transfer"] = transfer
            records.append(rec)
        return records
    except Exception as e:
        print(f"[fetch] {date_str} 出错: {e}")
        return []


def kv_get(key: str) -> dict:
    url = f"{KV_BASE}/values/{key}"
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code == 200:
        try:
            return r.json()
        except Exception:
            return {}
    return {}


def kv_put(key: str, blob: dict) -> None:
    url = f"{KV_BASE}/bulk"
    entry = [{"key": key, "value": json.dumps(blob, ensure_ascii=False), "expiration_ttl": TTL}]
    for attempt in range(5):
        r = requests.put(url, headers=HEADERS, data=json.dumps(entry), timeout=15)
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"限速 429，等待 {wait}s 后重试...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        result = r.json()
        if not result.get("success"):
            raise RuntimeError(f"KV 写入失败: {result}")
        return
    raise RuntimeError("KV 写入失败：超过最大重试次数")


def main():
    today = datetime.now()

    # 回溯3天（覆盖周末 + 漏跑情况）
    dates = [(today - timedelta(days=i)).strftime("%Y%m%d") for i in range(3)]

    # 读取现有 blob
    blob = kv_get(KV_KEY)
    print(f"[init] 已有 {len(blob)} 天数据")

    new_total = 0
    for date_str in dates:
        date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        if date_fmt in blob:
            print(f"[skip] {date_fmt} 已存在（{len(blob[date_fmt])} 条）")
            continue
        records = fetch_day(date_str)
        if records:
            blob[date_fmt] = records
            new_total += len(records)
            print(f"[fetch] {date_fmt}: {len(records)} 条")
        else:
            print(f"[fetch] {date_fmt}: 无数据（非交易日或无除权）")

    # 裁剪：只保留最近 KEEP_DAYS 天
    cutoff = (today - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    before = len(blob)
    blob = {k: v for k, v in blob.items() if k >= cutoff}
    pruned = before - len(blob)
    if pruned:
        print(f"[prune] 删除 {pruned} 天过期数据")

    kv_put(KV_KEY, blob)
    print(f"[done] dividend:ALL 写入完成，共 {len(blob)} 天，新增 {new_total} 条 ✓")


if __name__ == "__main__":
    main()
