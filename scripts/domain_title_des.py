import os
import pandas as pd
from common.data_utils import cleandomain
from common.file_utils import ensure_dir_exists
from common.html_utils import get_title_from_html, get_advanced_description
from common.log_utils import setup_logging
import asyncio
import aiohttp
import logging

BATCH_SIZE = 10000
PROGRESS_FILE = 'title_des_progress.txt'
RESULT_DIR = './results_title_des'
LOG_FILE = 'title_des.log'
ensure_dir_exists(RESULT_DIR)
setup_logging(LOG_FILE)

INPUT_CSV = os.getenv('input_csv', 'domains.csv')
DOMAIN_COL = os.getenv('domain_col', 'domain')
RETRY = 3

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        last_id = int(f.read().strip())
else:
    last_id = 0

df = pd.read_csv(INPUT_CSV)
domains = df[DOMAIN_COL].tolist()
total = len(domains)

async def fetch_title_des(session, domain):
    url = f'https://{domain}'
    for attempt in range(1, RETRY+1):
        try:
            async with session.get(url, timeout=10) as resp:
                html = await resp.text()
                title = get_title_from_html(html)
                des = get_advanced_description(html)
                logging.info(f"{domain} | title: {title}")
                return domain, title, des
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {domain}: {e}")
            if attempt == RETRY:
                return domain, f'error: {e}', ''
            await asyncio.sleep(1)

async def process_batch(batch_domains):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_title_des(session, d) for d in batch_domains]
        for r in await asyncio.gather(*tasks):
            results.append(r)
    return results

for batch_start in range(last_id, total, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, total)
    batch_domains = domains[batch_start:batch_end]
    logging.info(f'Processing {batch_start} - {batch_end}')
    batch_results = asyncio.run(process_batch(batch_domains))
    result_file = os.path.join(RESULT_DIR, f'title_des_{batch_start}_{batch_end}.csv')
    pd.DataFrame(batch_results, columns=['domain', 'title', 'description']).to_csv(result_file, index=False)
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(batch_end))
    logging.info(f'Saved {result_file}, progress updated to {batch_end}') 