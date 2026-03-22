"""
生成 AssetTracker 专业版激活码，并批量写入 Cloudflare KV。

用法：
  python generate_keys.py --count 100 --account-id YOUR_CF_ACCOUNT_ID --namespace-id YOUR_KV_NS_ID --api-token YOUR_CF_API_TOKEN

生成的 key 格式：AT-XXXX-XXXX-XXXX（去除易混淆字符 0/O/1/I）
"""
import argparse
import random
import string
import json
import sys
import urllib.request

# 去除易混淆字符
CHARS = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'

def gen_key() -> str:
    def seg(n):
        return ''.join(random.choices(CHARS, k=n))
    return f"AT-{seg(4)}-{seg(4)}-{seg(4)}"

def write_to_kv(account_id: str, namespace_id: str, api_token: str, keys: list[str]):
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/storage/kv/namespaces/{namespace_id}/bulk"
    payload = [
        {"key": f"license:{k}", "value": json.dumps({"activated": False})}
        for k in keys
    ]
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method='PUT')
    req.add_header('Authorization', f'Bearer {api_token}')
    req.add_header('Content-Type', 'application/json')
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    return result

def main():
    parser = argparse.ArgumentParser(description='生成激活码并写入 Cloudflare KV')
    parser.add_argument('--count', type=int, default=50, help='生成数量（默认 50）')
    parser.add_argument('--account-id', help='Cloudflare Account ID')
    parser.add_argument('--namespace-id', help='KV Namespace ID')
    parser.add_argument('--api-token', help='Cloudflare API Token')
    parser.add_argument('--dry-run', action='store_true', help='只打印，不写 KV')
    args = parser.parse_args()

    keys = [gen_key() for _ in range(args.count)]

    print(f"\n生成了 {len(keys)} 个激活码：")
    for k in keys:
        print(f"  {k}")

    if args.dry_run or not all([args.account_id, args.namespace_id, args.api_token]):
        print("\n（dry-run 模式，未写入 KV）")
        print("提示：加上 --account-id / --namespace-id / --api-token 参数以写入 KV")
        return

    print(f"\n正在写入 Cloudflare KV...")
    result = write_to_kv(args.account_id, args.namespace_id, args.api_token, keys)
    if result.get('success'):
        print(f"✅ 成功写入 {len(keys)} 条记录")
    else:
        print(f"❌ 写入失败：{result}")
        sys.exit(1)

if __name__ == '__main__':
    main()
