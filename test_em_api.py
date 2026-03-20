"""
测试东方财富 stock_sh_a_spot_em 接口是否被封
"""
import akshare as ak
from datetime import datetime

print(f"[{datetime.now():%H:%M:%S}] 开始请求东财沪A股数据...")
try:
    df = ak.stock_sh_a_spot_em()
    print(f"[{datetime.now():%H:%M:%S}] 成功，共 {len(df)} 条")
    print(df.head(3).to_string())
except Exception as e:
    print(f"[{datetime.now():%H:%M:%S}] 失败: {e}")
