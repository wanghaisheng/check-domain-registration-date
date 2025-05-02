import os
import pandas as pd
import asyncio
from datetime import datetime
from common.file_utils import ensure_dir_exists, write_lines, read_lines
from common.data_utils import cleandomain
from common.html_utils import extract_indexdate_from_google_html
from aiohttp import ClientSession

BATCH_SIZE = 10000
PROGRESS_FILE = 'indexdate_progress.txt'
RESULT_DIR = './results/indexdate/'
ensure_dir_exists(RESULT_DIR)

INPUT_CSV = os.getenv('filename', 'domains.csv')
COLNAME = os.getenv('colname', 'domain')

async def fetch_indexdate(session, domain):
    url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp"
    try:
        async with session.get(url, timeout=15) as resp:
            html = await resp.text()
            return extract_indexdate_from_google_html(html)
    except Exception as e:
        return ''

def get_last_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            return int(open(PROGRESS_FILE).read().strip())
        except:
            return 0
    return 0

def save_progress(idx):
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(idx))

async def main():
    df = pd.read_csv(INPUT_CSV)
    domains = df[COLNAME].tolist()
    start_idx = get_last_progress()
    total = len(domains)
    print(f"Total domains: {total}, start from: {start_idx}")
    batch = []
    results = []
    async with ClientSession() as session:
        for idx in range(start_idx, total):
            domain = cleandomain(domains[idx])
            indexdate = await fetch_indexdate(session, domain)
            results.append({'id': idx, 'domain': domain, 'indexdate': indexdate})
            if (idx + 1) % BATCH_SIZE == 0 or idx == total - 1:
                batch_no = (idx + 1) // BATCH_SIZE
                out_csv = os.path.join(RESULT_DIR, f'batch_{batch_no}.csv')
                pd.DataFrame(results).to_csv(out_csv, index=False)
                save_progress(idx + 1)
                print(f"Saved batch {batch_no} at idx {idx + 1}")
                results = []

if __name__ == "__main__":
    asyncio.run(main()) 