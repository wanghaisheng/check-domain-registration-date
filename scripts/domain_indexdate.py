import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from common.data_utils import cleandomain, filter_done_domains
from common.file_utils import ensure_dir_exists, write_lines
from common.html_utils import extract_indexdate_from_google_html
from common.log_utils import setup_logging
import asyncio
import aiohttp
import logging
from aiohttp_socks import ProxyConnector
import itertools
import requests
import re
from common.proxy_utils import get_shared_valid_proxies

BATCH_SIZE = 10000
PROGRESS_FILE = 'indexdate_progress.txt'
RESULT_DIR = './results_indexdate'
LOG_FILE = 'indexdate.log'
ensure_dir_exists(RESULT_DIR)
setup_logging(LOG_FILE)

INPUT_CSV = os.getenv('input_csv') or 'domains.csv'
DOMAIN_COL = os.getenv('domain_col') or 'domain'
logging.info(f'INPUT_CSV: {INPUT_CSV}, DOMAIN_COL: {DOMAIN_COL}')

RETRY = 3
GOOGLE_PROXY = os.getenv('GOOGLE_PROXY', 'socks5://127.0.0.1:1080')

# 读取进度
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        last_id = int(f.read().strip())
else:
    last_id = 0

# 读取任务
try:
    df = pd.read_csv(INPUT_CSV)
    logging.info(f'Successfully loaded {INPUT_CSV}, total rows: {len(df)}')
except Exception as e:
    logging.error(f'Failed to load {INPUT_CSV}: {e}')
    raise

domains = df[DOMAIN_COL].tolist()

total = len(domains)

def is_valid_proxy(proxy):
    m = re.match(r'^([0-9]{1,3}\.){3}[0-9]{1,3}:(\d{1,5})$', proxy)
    if not m:
        return False
    port = int(proxy.split(':')[1])
    return 1 <= port <= 65535

logging.info('Fetching and validating proxies from shared pool...')
proxy_list = get_shared_valid_proxies(max_count=100)
if not proxy_list:
    logging.error('No valid proxies could access Google! Falling back to 127.0.0.1:1080')
    proxy_list = ['socks5://127.0.0.1:1080']
proxy_cycle = itertools.cycle([p.replace('socks5://', '') for p in proxy_list])

async def fetch_indexdate(domain, max_retries=3):
    for attempt in range(1, max_retries+1):
        proxy = next(proxy_cycle)
        proxy_url = f'socks5://{proxy}'
        connector = ProxyConnector.from_url(proxy_url)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                url = f'https://www.google.com/search?q=About+{domain}&tbm=ilp'
                async with session.get(url, timeout=10) as resp:
                    html = await resp.text()
                    indexdate = extract_indexdate_from_google_html(html)
                    logging.info(f"{domain} | indexdate: {indexdate} | proxy: {proxy}")
                    return domain, indexdate
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {domain} with proxy {proxy}: {e}")
            await asyncio.sleep(1)
    return domain, f'error: all proxies failed'

async def process_batch(start_id, batch_domains):
    results = []
    tasks = [fetch_indexdate(d) for d in batch_domains]
    for r in await asyncio.gather(*tasks):
        results.append(r)
    return results

for batch_start in range(last_id, total, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, total)
    batch_domains = domains[batch_start:batch_end]
    logging.info(f'Processing {batch_start} - {batch_end}')
    batch_results = asyncio.run(process_batch(batch_start, batch_domains))
    # 保存结果
    result_file = os.path.join(RESULT_DIR, f'indexdate_{batch_start}_{batch_end}.csv')
    pd.DataFrame(batch_results, columns=['domain', 'indexdate']).to_csv(result_file, index=False)
    # 更新进度
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(batch_end))
    print(f'Saved {result_file}, progress updated to {batch_end}') 