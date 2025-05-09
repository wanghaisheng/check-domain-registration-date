#!/usr/bin/env python
# MassRDAP - developed by acidvegas (https://git.acid.vegas/massrdap)

import asyncio
import logging
import json
import re
import os,random
from datetime import datetime

import pandas as pd
from DataRecorder import Recorder

# try:
#     import aiofiles
# except ImportError:
#     raise ImportError('missing required aiofiles library (pip install aiofiles)')

try:
    import aiohttp
except ImportError:
    raise ImportError('missing required aiohttp library (pip install aiohttp)')
import aiohttp
import asyncio
from contextlib import asynccontextmanager

from loguru import logger
from dbhelper import DatabaseManager

# Replace this with your actual test URL
test_url = 'http://example.com'

# Replace this with your actual outfile object and method for adding data
# outfile = YourOutfileClass()
# Color codes
BLUE   = '\033[1;34m'
CYAN   = '\033[1;36m'
GREEN  = '\033[1;32m'
GREY   = '\033[1;90m'
PINK   = '\033[1;95m'
PURPLE = '\033[0;35m'
RED    = '\033[1;31m'
YELLOW = '\033[1;33m'
RESET  = '\033[0m'

MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

# Setup basic logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable to store RDAP servers
RDAP_SERVERS = {}
def get_title_from_html_1(html):
    title = 'not content!'
    try:
        title_patten = r'<title>(\s*?.*?\s*?)</title>'
        result = re.findall(title_patten, html)
        if len(result) >= 1:
            title = result[0]
            title = title.strip()
    except:
        logger.error('cannot find title')
    return title
def get_title_from_html(html):
    title = 'not found'
    try:
        title_pattern = r'<title>(.*?)</title>'
        result = re.search(title_pattern, html, re.IGNORECASE | re.DOTALL)
        if result:
            title = result.group(1).strip()
    except Exception as e:
        logger.error(f'Error finding title: {e}')
    return title

def get_des_from_html1(html):
    title = 'not content!'
    try:
        title_patten = r'<meta name="description" content="(\s*?.*?\s*?)/>'
        result = re.findall(title_patten, html)
        if len(result) >= 1:
            title = result[0]
            title = title.strip()
    except:
        logger.error('cannot find description')
    return title
def get_des_from_html(html):
    description = 'not found'
    try:
        des_pattern = r'<meta\s+name="description"\s+content="(.*?)"\s*/?>'
        result = re.search(des_pattern, html, re.IGNORECASE | re.DOTALL)
        if result:
            description = result.group(1).strip()
    except Exception as e:
        logger.error(f'Error finding description: {e}')
    return description


async def fetch_rdap_servers():
    '''Fetches RDAP servers from IANA's RDAP Bootstrap file.'''

    async with aiohttp.ClientSession() as session:
        async with session.get('https://data.iana.org/rdap/dns.json') as response:
            data = await response.json()
            for entry in data['services']:
                tlds = entry[0]
                rdap_url = entry[1][0]
                for tld in tlds:
                    RDAP_SERVERS[tld] = rdap_url
async def get_proxy():
    proxy=None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('http://demo.spiderpy.cn/get?https') as response:
                data = await response.json()
                proxy=data['proxy']
                if proxy and 'http' not in proxy:
                    proxy=f'https://{proxy}'

                return proxy
        except:
            return None
async def get_proxy_proxypool():
    proxy=None
   
    async with aiohttp.ClientSession() as session:

        try:
            async with session.get('https://proxypool.scrape.center/random?https') as response:
                proxy = await response.text()
                if proxy and 'http' not in proxy:
                    proxy=f'https://{proxy}'
                    return proxy
        except:
            return None

from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector

async def getSession(proxy_url):
    if proxy_url:
        if 'socks' in proxy_url:
            # initialize a SOCKS proxy connector
            connector = ProxyConnector.from_url(proxy_url)

            # initialize an AIOHTTP client with the SOCKS proxy connector
            session=  aiohttp.ClientSession(connector=connector)
            return session
        else:
            session= aiohttp.ClientSession() 
            return session        

    else:
        session= aiohttp.ClientSession() 
        return session        
async def getResponse(proxy_url,query_url):
    logger.info('get response',proxy_url)

    if proxy_url is None or  'http' in proxy_url:
        logger.info('not socks prroxy')
        async with aiohttp.ClientSession() as session:

            response=await session.get(query_url, proxy=proxy_url if proxy_url else None,
                # auth=auth.prepare_request, 
                timeout=30)               
            return response       
    
    else:

        # initialize a SOCKS proxy connector
        connector = ProxyConnector.from_url(proxy_url)

        # initialize an AIOHTTP client with the SOCKS proxy connector
        async with aiohttp.ClientSession(connector=connector) as session:

            response=await session.get(query_url,timeout=30)
                    


            return response
def get_tld(domain: str):
    '''Extracts the top-level domain from a domain name.'''
    parts = domain.split('.')
    return '.'.join(parts[-1:]) if len(parts) > 1 else parts[0]
async def lookup_domain_with_retry(domain: str, valid_proxies:list,proxy_url: str, semaphore: asyncio.Semaphore, outfile:Recorder,db_manager):
    retry_count = 0
    while retry_count < MAX_RETRIES:
        if retry_count>0:
            pro_str=None
            proxy_url=None
            if valid_proxies:
                proxy_url=random.choice(valid_proxies)
            else:
                try:
                    proxy_url=await get_proxy()

                    if proxy_url is None:
                    
                        proxy_url=await get_proxy_proxypool()


                except Exception as e:
                    logger.error('get proxy error:{} use backup',e)
                    return 
        logger.info(f"{retry_count} retry current proxy {proxy_url}")

        try:
            async with semaphore:
                result = await asyncio.wait_for(lookup_domain(domain, proxy_url, semaphore, outfile,db_manager), timeout=10)
                if result:
                    if proxy_url and proxy_url not in valid_proxies:
                        valid_proxies.append(proxy_url)
                    return result
        except asyncio.TimeoutError:
            logger.error(f"Timeout occurred for domain: {domain} with proxy: {proxy_url}")
            if proxy_url and proxy_url  in valid_proxies:
                valid_proxies.remove(proxy_url)        
        except Exception as e:
            logger.error(f"Error occurred: {e}")
            if proxy_url and proxy_url  in valid_proxies:
                valid_proxies.remove(proxy_url)        
        retry_count += 1
        # if retry_count < MAX_RETRIES:
        #     delay = min(INITIAL_DELAY * (2 ** retry_count), MAX_DELAY)
        #     logger.info(f"Retrying in {delay} seconds with proxy {proxy_url}...")
        #     await asyncio.sleep(delay)
    
    logger.error(f"Max retries reached for domain: {domain}")
    return None

async def lookup_domain(domain: str,proxy_url: str, semaphore: asyncio.Semaphore, outfile:Recorder,db_manager):
    '''
    Looks up a domain using the RDAP protocol.
    
    :param domain: The domain to look up.
    :param proxy_url: The proxy URL to use for the request.
    :param semaphore: The semaphore to use for concurrency limiting.
    '''


    async with semaphore:


        logger.info('use proxy_url:{}',proxy_url)


        query_url=domain
        if 'http' not in domain:
            query_url=f'https://{domain}'

        session = None  # Initialize session at the beginning of the function

        logger.info('querying:{}',query_url)


        try:
            response=await getResponse(proxy_url,query_url)

            creation_date_str=''
            rawdata=''
            if response is None:
                logger.error(f"Received None as response for {query_url}")
                return False

            # Parse JSON and check if data is None
            data = await response.text()
            if data is None:
                logger.error(f"Received None as data for {query_url}")
                return False

            if response.status == 200:
                data = await response.text()
                title = get_title_from_html(data)
                des=get_des_from_html(data)
                if title:
                    data = {
                        'domain': domain,
                        "title": title,
                        'des':des
                    }
                    outfile.add_data(data)

                    new_domain = db_manager.Domain(
                        url=domain,
                    title=title,
                    des=des,
                    )
                    db_manager.add_domain(new_domain)
                    logger.info(f'add data for {domain}')


                    logger.info(f'{GREEN}SUCCESS {GREY}| {BLUE}{response.status} {GREY}| {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain}{GREEN}')
                return True
            else:
                logger.warning(f"Non-200 status code: {response.status} for {domain}")
                return False

        except asyncio.TimeoutError as e:
            logger.info(f'{RED} TimeoutError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}')
            raise

        except aiohttp.ClientError as e:
            logger.info(f'{RED} ClientError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}')
            raise
        
        except Exception as e:
            logger.info(f'{RED}Exception  {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}')
            raise
        
        finally:
            if session:
                await session.close()


@asynccontextmanager
async def aiohttp_session(url):
    async with aiohttp.ClientSession() as session:
        yield session

async def test_proxy(test_url,proxy_url):
    try:
        async with aiohttp_session(test_url) as session:
            # Determine the type of proxy and prepare the appropriate proxy header

            # Make the request
            async with session.get(test_url, timeout=10, proxy=proxy_url) as response:
                if response.status == 200:

                    # outfile.add_data(proxy_url)  # Uncomment and replace with your actual implementation
                    return True
                else:

                    return False
    except asyncio.TimeoutError:
        # print(f"{Style.BRIGHT}{Color.red}Invalid Proxy (Timeout) | {proxy_url}{Style.RESET_ALL}")
        return False
    except aiohttp.ClientError:

        return False

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
async def process_domains_title(domains, outfile, counts, db_manager):
    semaphore = asyncio.Semaphore(25)  # Set the concurrency limit

    # Initialize a counter
    domain_counter = 0
    if counts>0:
        domains=domains[:counts]
    # This will be an asynchronous generator
    async def domain_generator(domains, batch_size=5):
        nonlocal domain_counter
        for domain in domains:
            domain = cleandomain(domain)
            if domain and isinstance(domain, str) and "." in domain and len(domain.split(".")) > 1:
                yield domain
                domain_counter += 1
                if domain_counter % batch_size == 0:
                    # Yield after every 5 domains
                    await asyncio.sleep(0)  # Yield control back to the event loop

    # Use the domain_generator to get batches of domains
    domain_gen = domain_generator(domains)

    async def process_batch(valid_proxies):
        tasks = []
        for _ in range(5):  # Process 5 domains in a batch
            domain = await domain_gen.__anext__()  # Get the next domain from the generator
            if domain:
                task = asyncio.create_task(
                    lookup_domain_with_retry(domain, valid_proxies, None,semaphore, outfile, db_manager)
                )
                tasks.append(task)

        # Wait for all tasks in the current batch to complete
        await asyncio.gather(*tasks)

    # Main processing loop
    while True:
        try:
            valid_proxies = []  # Initialize valid proxies list for each batch
            await process_batch(valid_proxies)
        except StopAsyncIteration:
            # No more domains to process
            break

    # Rest of your code...