import requests
import asyncio
import aiohttp
import pandas as pd
from aiohttp_socks import ProxyConnector
from common.proxy_utils import get_shared_valid_proxies

FAILED_FILE = 'failed_domains.txt'
RETRY_RESULT_FILE = 'retry_failed_results.csv'
PROXY_LIST_URL = 'https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt'

# 1. 读取失败域名
with open(FAILED_FILE, 'r') as f:
    failed_domains = [line.strip() for line in f if line.strip()]

# 2. 获取最新socks5代理列表
proxy_list = get_shared_valid_proxies(max_count=100)
proxy_cycle = iter([p.replace('socks5://', '') for p in proxy_list])

async def fetch_with_proxy(domain, proxy):
    url = f'https://{domain}'
    proxy_url = f'socks5://{proxy}'
    try:
        connector = ProxyConnector.from_url(proxy_url)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, timeout=15) as resp:
                html = await resp.text()
                # 这里可调用你的内容提取函数
                return domain, 'success'
    except Exception as e:
        return domain, f'error: {e}'

async def main():
    results = []
    for domain in failed_domains:
        proxy = next(proxy_cycle, None)
        if not proxy:
            print('No more proxies available!')
            break
        result = await fetch_with_proxy(domain, proxy)
        results.append(result)
    # 保存结果
    pd.DataFrame(results, columns=['domain', 'result']).to_csv(RETRY_RESULT_FILE, index=False)

if __name__ == '__main__':
    asyncio.run(main()) 