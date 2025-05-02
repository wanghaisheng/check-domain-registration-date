import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from common.data_utils import cleandomain
from common.file_utils import ensure_dir_exists
from common.domain_borndate_utils import lookup_domain_borndate, rdap_query_url, rdap_parse_borndate
from common.log_utils import setup_logging
import asyncio
import aiohttp
import logging

BATCH_SIZE = 10000
PROGRESS_FILE = 'borndate_rdap_progress.txt'
RESULT_DIR = './results_borndate_rdap'
LOG_FILE = 'borndate_rdap.log'
ensure_dir_exists(RESULT_DIR)
setup_logging(LOG_FILE)

INPUT_CSV = os.getenv('input_csv', 'domains.csv')
DOMAIN_COL = os.getenv('domain_col', 'domain')
RDAP_URL = os.getenv('rdap_url', 'https://rdap.whois.ai/')  # 可根据TLD动态调整
RETRY = 3

def make_rdap_query_url(domain):
    return rdap_query_url(domain, RDAP_URL)

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        last_id = int(f.read().strip())
else:
    last_id = 0

df = pd.read_csv(INPUT_CSV)
domains = df[DOMAIN_COL].tolist()
total = len(domains)

async def fetch_borndate(domain):
    for attempt in range(1, RETRY+1):
        try:
            borndate = await lookup_domain_borndate(domain, make_rdap_query_url, rdap_parse_borndate)
            logging.info(f"{domain} | borndate: {borndate}")
            return domain, borndate
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {domain}: {e}")
            if attempt == RETRY:
                return domain, f'error: {e}'
            await asyncio.sleep(1)

async def process_batch(batch_domains):
    results = []
    tasks = [fetch_borndate(d) for d in batch_domains]
    for r in await asyncio.gather(*tasks):
        results.append(r)
    return results

for batch_start in range(last_id, total, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, total)
    batch_domains = domains[batch_start:batch_end]
    logging.info(f'Processing {batch_start} - {batch_end}')
    batch_results = asyncio.run(process_batch(batch_domains))
    result_file = os.path.join(RESULT_DIR, f'borndate_rdap_{batch_start}_{batch_end}.csv')
    pd.DataFrame(batch_results, columns=['domain', 'borndate']).to_csv(result_file, index=False)
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(batch_end))
    logging.info(f'Saved {result_file}, progress updated to {batch_end}') 