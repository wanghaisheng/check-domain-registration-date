import os
import pandas as pd
from common.data_utils import cleandomain
from common.file_utils import ensure_dir_exists
from common.html_utils import extract_price_plans_from_html, extract_markdown_from_html, extract_price_from_markdown_with_api
from common.log_utils import setup_logging
import asyncio
import aiohttp
import logging

BATCH_SIZE = 10000
PROGRESS_FILE = 'price_progress.txt'
RESULT_DIR = './results_price'
LOG_FILE = 'price.log'
ensure_dir_exists(RESULT_DIR)
setup_logging(LOG_FILE)

INPUT_CSV = os.getenv('input_csv', 'domains.csv')
DOMAIN_COL = os.getenv('domain_col', 'domain')
API_URL = os.getenv('price_api_url', 'http://localhost:8000/price-extract')  # 你的API地址
API_KEY = os.getenv('price_api_key', None)
RETRY = 3

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        last_id = int(f.read().strip())
else:
    last_id = 0

df = pd.read_csv(INPUT_CSV)
domains = df[DOMAIN_COL].tolist()
total = len(domains)

async def fetch_price(session, domain):
    url = f'https://{domain}'
    for attempt in range(1, RETRY+1):
        try:
            async with session.get(url, timeout=15) as resp:
                html = await resp.text()
                md = extract_markdown_from_html(html)
                price_info = extract_price_from_markdown_with_api(md, API_URL, api_key=API_KEY)
                logging.info(f"{domain} | price_info: {price_info}")
                return domain, price_info
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {domain}: {e}")
            if attempt == RETRY:
                return domain, f'error: {e}'
            await asyncio.sleep(1)

async def process_batch(batch_domains):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_price(session, d) for d in batch_domains]
        for r in await asyncio.gather(*tasks):
            results.append(r)
    return results

for batch_start in range(last_id, total, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, total)
    batch_domains = domains[batch_start:batch_end]
    logging.info(f'Processing {batch_start} - {batch_end}')
    batch_results = asyncio.run(process_batch(batch_domains))
    result_file = os.path.join(RESULT_DIR, f'price_{batch_start}_{batch_end}.csv')
    pd.DataFrame(batch_results, columns=['domain', 'price_info']).to_csv(result_file, index=False)
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(batch_end))
    logging.info(f'Saved {result_file}, progress updated to {batch_end}') 