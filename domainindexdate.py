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
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable to store RDAP servers
RDAP_SERVERS = {}
def get_title_from_html(html):
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

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('http://demo.spiderpy.cn/get') as response:
                data = await response.json()
                proxy=data.get('proxy')
                return proxy
        except:
            try:
                async with session.get('https://proxypool.scrape.center/random') as response:
                    proxy = await response.text()
                    return proxy
            except:
                return None
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector

async def getSession(proxy_url):
    if 'socks' in proxy_url:
        # initialize a SOCKS proxy connector
        connector = ProxyConnector.from_url(proxy_url)

        # initialize an AIOHTTP client with the SOCKS proxy connector
        session=  aiohttp.ClientSession(connector=connector)
        return session
    else:
        session= aiohttp.ClientSession() 
        return session        


def get_tld(domain: str):
    '''Extracts the top-level domain from a domain name.'''
    parts = domain.split('.')
    return '.'.join(parts[1:]) if len(parts) > 1 else parts[0]
async def lookup_domain_with_retry(domain: str, valid_proxies:list,proxy_url: str, semaphore: asyncio.Semaphore, outfile:Recorder):
    retry_count = 0
    while retry_count < MAX_RETRIES:
        logger.info('current retry：{}',retry_count)
        if retry_count>0 and proxy_url==None:
            if valid_proxies:
                proxy_url=random.choice(valid_proxies)
            else:
                try:
                    pro_str=await get_proxy()

                    proxy_url = "http://{}".format(pro_str)        
                    if pro_str is None:
                        proxy_url='socks5://127.0.0.1:1080'

                except Exception as e:
                    logger.error('get proxy error:{} use backup',e)
                    proxy_url='socks5://127.0.0.1:1080'
        logger.info('current proxy{}',proxy_url)

        try:
            async with semaphore:
                result = await asyncio.wait_for(lookup_domain(domain, proxy_url, semaphore, outfile), timeout=30)
            if result:
                if proxy_url and proxy_url not in valid_proxies:
                    valid_proxies.append(proxy_url)
                return result
        except asyncio.TimeoutError:
            logger.error(f"Timeout occurred for domain: {domain} with proxy: {proxy_url}")
        except Exception as e:
            logger.error(f"Error occurred: {e}")
        retry_count += 1
        # if retry_count < MAX_RETRIES:
        #     delay = min(INITIAL_DELAY * (2 ** retry_count), MAX_DELAY)
        #     logger.info(f"Retrying in {delay} seconds with proxy {proxy_url}...")
        #     await asyncio.sleep(delay)
    
    logger.error(f"Max retries reached for domain: {domain}")
    return None
from bs4 import BeautifulSoup

async def lookup_domain(domain: str,proxy_url: str, semaphore: asyncio.Semaphore, outfile:Recorder):
    '''
    Looks up a domain using the RDAP protocol.
    
    :param domain: The domain to look up.
    :param proxy_url: The proxy URL to use for the request.
    :param semaphore: The semaphore to use for concurrency limiting.
    '''


    async with semaphore:


        logger.info('use proxy_url:{}',proxy_url)


        query_url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj3jraUsoGGAxUvSGwGHUbfAEwQv5AHegQIABAE"

        session = None  # Initialize session at the beginning of the function

        logger.info('querying:{}',query_url)


        try:
            session=await getSession(proxy_url)
            response=None
            if 'socks' in proxy_url:
                response=await session.get(query_url,timeout=20)
                    
            else:
                response=await session.get(query_url, proxy=proxy_url if proxy_url else None,
                # auth=auth.prepare_request, 
                timeout=20)   
                                

            if response.status == 200:
                data = await response.text()
                title = get_title_from_html(data)
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


                    if r:
                        data = {
                            'domain': domain,
                            "indexdate": r,
                        }
                        outfile.add_data(data)
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

async def process_domains(inputfilepath,domainkey,outfilepath,outfile):

    
    semaphore = asyncio.Semaphore(500)
    df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")

    # df = df.head(1)
    # domains = df[df["type"] == "aitools"]["domains"].tolist()
    # domains=df['Keyword'].tolist()
    domains=df[domainkey].tolist()


    # outfilefailedpath=inputfilepath.replace('.csv','-result-failed.csv')
    
    # outfilefailed = Recorder(outfilefailedpath, cache_size=500)

    donedomains=[]
    if os.path.exists(outfilepath):
        df = pd.read_csv(outfilepath)
        # print(df.head(1))
        donedomains=df['domain'].tolist()
    # domains=[d for d in domains if d not in donedomains]
    # print(len(domains))


    tasks = []
    domains=list(set(domains))
    for domain in domains:
        
        

        if domain and domain not in  donedomains and type(domain)==str and "." in domain and len(domain.split('.'))>1:
            print(domain)

            domain = domain.strip()
            domain=domain.split('//')[-1]
            proxy=None

            # if len(valid_proxies)>1:
            #     proxy=random.choice(valid_proxies)
            #     print('pick proxy',proxy)

            proxy="socks5h://127.0.0.1:1080"
            tld = get_tld(domain)

            if tld:

                try:
                    task = asyncio.create_task(lookup_domain_with_retry(domain, [],proxy, semaphore, outfile))
                    # Ensure the semaphore is released even if the task fails
                    task.add_done_callback(lambda t: semaphore.release())
                    # print('done', url)
                    tasks.append(task)

                except Exception as e:
                    print(f"{RED}An error occurred while processing {domain}: {e}")



    # Log when the task starts and finishes
    for task in tasks:
        await task


