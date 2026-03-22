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

WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=c7129f5b-6124-42f2-a38e-c9bd1c9d045a"
BARK_KEY       = "9vAhoWi6DJZCGcEzYtAmsS"


# ── 通知 ────────────────────────────────────────────

def send_wechat(content: str):
    try:
        resp = requests.post(WECHAT_WEBHOOK, json={"msgtype": "text", "text": {"content": content}}, timeout=5).json()
        if resp.get("errcode") != 0:
            print(f"[{ts()}] 微信推送失败: {resp}")
    except Exception as e:
        print(f"[{ts()}] 微信推送异常: {e}")


def send_bark(title: str, content: str):
    try:
        requests.get(f"https://api.day.app/{BARK_KEY}/{title}/{content}", timeout=5)
    except Exception as e:
        print(f"[{ts()}] Bark推送异常: {e}")


def notify(title: str, content: str):
    """同时推送微信和 Bark"""
    send_wechat(f"{title}\n{content}")
    send_bark(title, content)


# ── 核心逻辑 ────────────────────────────────────────

def load_blob_from_kv() -> dict:
    """从KV读取现有blob，保留name/note/confidence等元数据"""
    print(f"[{ts()}] 读取 KV blob...")
    t0 = time.time()
    try:
        r = requests.get(f"{KV_BASE_URL}/values/{KV_KEY}", headers=HEADERS, timeout=30)
        elapsed = time.time() - t0
        if r.status_code == 200:
            blob = json.loads(r.text)
            size_kb = len(r.content) / 1024
            print(f"[{ts()}] 从KV加载 {len(blob)} 条基础数据 ({size_kb:.0f} KB，{elapsed:.1f}s)")
            return blob
        print(f"[{ts()}] KV读取返回 {r.status_code}（{elapsed:.1f}s），从空白开始")
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[{ts()}] KV读取失败（{elapsed:.1f}s 后）: {e}")
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
    print(f"[{ts()}] 开始写入 KV，blob 大小 {size_kb:.0f} KB，共 {len(blob)} 条...")
    t0 = time.time()
    try:
        r = requests.put(BULK_URL, headers=HEADERS, data=json.dumps(entry), timeout=(10, 120))
        elapsed = time.time() - t0
        print(f"[{ts()}] HTTP 响应 {r.status_code}，耗时 {elapsed:.1f}s")
        result = r.json()
        if result.get("success"):
            print(f"[{ts()}] KV写入完成 ✓ ({size_kb:.0f} KB，{elapsed:.1f}s)")
            return True
        msg = f"KV写入失败: {result}"
        print(f"[{ts()}] {msg}")
        notify("❌【KV推送失败】", msg)
    except Exception as e:
        elapsed = time.time() - t0
        msg = f"写入异常（{elapsed:.1f}s 后）: {e}"
        print(f"[{ts()}] KV{msg}")
        notify("❌【KV推送异常】", msg)
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
    notify("🚀【KV推送启动】", f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} A股实时推送脚本已启动")
    blob = load_blob_from_kv()

    last_push      = 0.0
    last_csv_mtime = 0.0
    fail_count     = 0       # 连续写入失败次数
    push_count     = 0       # 成功写入总次数
    last_trading   = None

    while True:
        try:
            trading = is_trading_time()
            if not trading:
                if last_trading is not False:
                    print(f"[{ts()}] 已闭市，暂停推送，等待下次交易时间...")
                    last_push_ok = push_blob(blob) if blob else False
                    result_str = f"最后一次写入{'成功 ✓' if last_push_ok else '失败 ✗'}，本次共推送 {push_count} 次"
                    notify("🔴【已闭市】", f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 已闭市，推送暂停\n{result_str}")
                    push_count = 0
                last_trading = False
                time.sleep(30)
                continue
            if not last_trading:
                print(f"[{ts()}] 交易时间开始，启动推送...")
                notify("🟢【开市】", f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 交易时间开始，KV推送启动")
            last_trading = True

            if not os.path.exists(CSV_PATH):
                print(f"[{ts()}] CSV不存在，等待掘金量化写入...")
                time.sleep(30)
                continue

            # 只有CSV有更新才重新读取
            csv_mtime = os.path.getmtime(CSV_PATH)
            if csv_mtime != last_csv_mtime:
                print(f"[{ts()}] 检测到 CSV 变更，开始读取...")
                t0 = time.time()
                updated = update_prices(blob, CSV_PATH)
                last_csv_mtime = csv_mtime
                print(f"[{ts()}] CSV同步完成，更新 {updated}/{len(blob)} 条，耗时 {time.time()-t0:.1f}s")

            # 每5分钟推一次
            now = time.time()
            if now - last_push >= PUSH_INTERVAL:
                ok = push_blob(blob)
                last_push = now
                if ok:
                    fail_count = 0
                    push_count += 1
                    if push_count == 1:
                        notify("✅【首次写入成功】", f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} 首次KV写入成功，推送正常运行")
                else:
                    fail_count += 1
                    if fail_count >= 3:
                        notify("🆘【KV连续失败】", f"已连续 {fail_count} 次写入失败，请检查网络或 CF 配置")

        except Exception as e:
            print(f"[{ts()}] 错误: {e}")
            notify("⚠️【推送脚本异常】", str(e))

        time.sleep(10)


if __name__ == "__main__":
    main()
