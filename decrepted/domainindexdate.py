#!/usr/bin/env python
# MassRDAP - developed by acidvegas (https://git.acid.vegas/massrdap)

import asyncio
import logging
import json
import re
import os, random
from datetime import datetime

import pandas as pd
from DataRecorder import Recorder
from dbhelper import *

# try:
#     import aiofiles
# except ImportError:
#     raise ImportError('missing required aiofiles library (pip install aiofiles)')

try:
    import aiohttp
except ImportError:
    raise ImportError("missing required aiohttp library (pip install aiohttp)")
import aiohttp
import asyncio
from contextlib import asynccontextmanager
import aiohttp_socks

from loguru import logger

# Replace this with your actual test URL
test_url = "http://example.com"

# Replace this with your actual outfile object and method for adding data
# outfile = YourOutfileClass()
# Color codes
BLUE = "\033[1;34m"
CYAN = "\033[1;36m"
GREEN = "\033[1;32m"
GREY = "\033[1;90m"
PINK = "\033[1;95m"
PURPLE = "\033[0;35m"
RED = "\033[1;31m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"

MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global variable to store RDAP servers
RDAP_SERVERS = {}



from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time

# Semaphore to control concurrency
semaphore = asyncio.Semaphore(50)  # Allow up to 50 concurrent tasks
# db_manager = DatabaseManager()
inputfilepath=r'D:\Download\audio-visual\a_ideas\majestic_million.csv'
inputfilepath=r'D:\Download\audio-visual\a_ideas\toolify.ai-organic-competitors--.csv'

filename='majestic_million'
filename='toolify.ai-organic-competitors--'
folder_path='.'
inputfilepath=filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)
outfilepath=inputfilepath.replace('.csv','-in.csv')
outfile = Recorder(folder_path+'/'+outfilepath, cache_size=50)

def get_title_from_html(html):
    title = "not content!"
    try:
        title_patten = r"<title>(\s*?.*?\s*?)</title>"
        result = re.findall(title_patten, html)
        if len(result) >= 1:
            title = result[0]
            title = title.strip()
    except:
        logger.error("cannot find title")
    return title


async def fetch_rdap_servers():
    """Fetches RDAP servers from IANA's RDAP Bootstrap file."""

    async with aiohttp.ClientSession() as session:
        async with session.get("https://data.iana.org/rdap/dns.json") as response:
            data = await response.json()
            for entry in data["services"]:
                tlds = entry[0]
                rdap_url = entry[1][0]
                for tld in tlds:
                    RDAP_SERVERS[tld] = rdap_url


async def get_proxy():
    proxy=None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('http://demo.spiderpy.cn/get') as response:
                data = await response.json()
                proxy=data['proxy']
                return proxy
        except:
            pass
async def get_proxy_proxypool():
    async with aiohttp.ClientSession() as session:

        try:
            async with session.get('https://proxypool.scrape.center/random') as response:
                proxy = await response.text()
                return proxy
        except:
            return None

def get_des_from_html(html):
    description = 'not content!'
    try:

        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')


        # Extract the meta description
        description_tag = soup.find('meta', attrs={'name': 'description'})
        description = description_tag['content'] if description_tag else 'No description found'
        description=description.strip()
        logger.ino(f'find description:{description}')

    except:
        logger.error('cannot find description')
    return description

def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]

# Function to extract data from HTTP response
async def extract_data(response):
    try:
        data = await response.json()
        return data
    except aiohttp.ContentTypeError:
        return None
async def extract_indedate(response,domain):
    data = await response.text()

    soup = BeautifulSoup(data, "html.parser")

    # Find all elements that contain the text 'aaa'

    elements_with_aaa = soup.find_all(
        lambda tag: "Site first indexed by Google" in tag.get_text()
    )

    # # Output the text content of each element that contains 'aaa'
    # for element in elements_with_aaa:
    # print(element.get_text().strip())
    if len(elements_with_aaa) > 0:
        r = elements_with_aaa[0].get_text()
        if "Site first indexed by Google" in r:
            r = r.split("Site first indexed by Google")
            # print("get data", r[0])
            print("get data", r[-1])
            date=r[-1]
            if date and not date.endswith('ago'):
                date=date.split('ago')[0]+'ago'
            data = {
                "domain": domain,
                "indexdate": date,
                'indexdata':r
            }
            outfile.add_data(data)



            # Domain=
            # new_domain = db_manager.Domain(
            #     url=domain,tld=get_tld(domain),
            # title=None,
            # indexat=r[-1] or None,
            # des=None,
            # bornat=None)
            # db_manager.add_domain(new_domain)

def savedb(outfile,domain,domaindata):

    # Domain=
    new_domain = Domain(
        url=domain,tld=get_tld(domain),
    title=None,
    indexat=r[-1] or None,
    des=None,
    bornat=None)
    add_domain(new_domain)
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector

async def getSession(proxy_url):
    logger.info('get response',proxy_url)
    response=None

    if proxy_url is None or  'http' in proxy_url:
        logger.info('not socks prroxy')
        async with aiohttp.ClientSession() as session:

          
            return session       
    
    else:

        # initialize a SOCKS proxy connector
        connector = ProxyConnector.from_url(proxy_url)

        # initialize an AIOHTTP client with the SOCKS proxy connector
        async with aiohttp.ClientSession(connector=connector) as session:

                    


            return session       
    

# Function to simulate a task asynchronously
async def get_index_date(session,domains):
    async with semaphore:
        
        retries = 3
        attempt=1
        for domain in domains:
            url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj3jraUsoGGAxUvSGwGHUbfAEwQv5AHegQIABAE"

            try:
                async with session.get(url) as response:
                        if response.status == 200:
                            data = await extract_indedate(response,domain)
                            if data:
                                print(f"Task {url} completed on attempt {attempt}. Data: {data}")
                                return
                        else:
                            print(f"Task {url} failed on attempt {attempt}. Status code: {response.status}")
            except aiohttp.ClientConnectionError:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")

            except Exception:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")


# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
def cleandomain(domain):
    domain=domain.strip()
    if "https://" in domain:
        domain = domain.replace("https://", "")
    if "http://" in domain:
        domain = domain.replace("http://", "")
    if "www." in domain:
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

# Function to run tasks asynchronously with specific concurrency
async def process_domains_indexdate(domains,outfile,counts,db_manager):
    tasks = []

    df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")
    domains=df['domain'].to_list()
    print('load domains')
    donedomains=[]

    try:
        # dbdata=db_manager.read_domain_all()

        # for i in dbdata:
        #     if i.indexat is not None:
        #         donedomains.append(i.url)    
        pass    
    except Exception as e:
        print(f'query error: {e}')
        if os.path.exists(outfilepath):
            df=pd.read_csv(outfilepath)
            donedomains=df['domain'].to_list()

    print(f'load donedomains:{donedomains}')

    domains=[cleandomain(i) for i in domains if i not in donedomains]    
    print(f'to be  donedomains:{len(donedomains)}')
    time.sleep(60)


    # Process domains in batches
    batch_size = 50  # Define the size of each batch
    for i in range(0, len(domains), batch_size):
        batch = domains[i:i + batch_size]
        proxy_url='socks5://127.0.0.1:1080'
        # proxy_url = "socks5://127.0.0.1:9050"  # Example SOCKS5 proxy URL
        connector = aiohttp_socks.ProxyConnector.from_url(proxy_url) if proxy_url and proxy_url.startswith("socks") else None
        proxy=proxy_url if proxy_url and 'http' in proxy_url else None
        print('===proxy',proxy)
        session= aiohttp.ClientSession(connector=connector)

        task = asyncio.create_task(get_index_date(session,batch))
        tasks.append(task)
    await asyncio.gather(*tasks)

# # Example usage: Main coroutine
# async def main():
#     start_time = time.time()
#     await run_async_tasks()
#     print(f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds")

# # Manually manage the event loop in Jupyter Notebook or other environments
# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     try:
#         loop.run_until_complete(main())
#     finally:
#         loop.close()

