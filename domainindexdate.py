import asyncio
import logging
import re
import random
from bs4 import BeautifulSoup
import aiohttp
from aiohttp_socks import ProxyConnector
from loguru import logger
from DataRecorder import Recorder  # Assuming this is a custom class/module you've defined

# Global constants
MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10
RDAP_SERVERS = {}

# Initialize logging
logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to get the top-level domain from a domain name
def get_tld(domain: str):
    parts = domain.split(".")
    return ".".join(parts[-1:]) if len(parts) > 1 else parts[0]

# Function to clean the domain name
def cleandomain(domain):
    domain = domain.strip()
    if "https://" in domain:
        domain = domain.replace("https://", "")
    if "http://" in domain:
        domain = domain.replace("http://", "")
    if "www." in domain:
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

async def get_proxy():
    proxy=None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('http://demo.spiderpy.cn/get?https') as response:
                data = await response.json()
                proxy=data['proxy']
                proxy=f'https://{proxy}'
                return proxy
        except:
            return None
async def get_proxy_proxypool():
    async with aiohttp.ClientSession() as session:

        try:
            async with session.get('https://proxypool.scrape.center/random?https') as response:
                proxy = await response.text()
                proxy=f'https://{proxy}'

                return proxy
        except:
            return None
# Asynchronous context manager for aiohttp session
async def aiohttp_session(url):
    async with aiohttp.ClientSession() as session:
        yield session

# Asynchronous function to fetch RDAP servers
async def fetch_rdap_servers():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://data.iana.org/rdap/dns.json") as response:
            data = await response.json()
            for entry in data["services"]:
                tlds = entry[0]
                rdap_url = entry[1][0]
                for tld in tlds:
                    RDAP_SERVERS[tld] = rdap_url

# Asynchronous function to get a session with optional proxy
async def getSession(proxy_url):
    if proxy_url and 'socks' in proxy_url:
        connector = ProxyConnector.from_url(proxy_url)
        session = aiohttp.ClientSession(connector=connector)
    else:
        session = aiohttp.ClientSession()
    return session

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
                        # proxy_url='http://127.0.0.1:1080'

                except Exception as e:
                    logger.error('get proxy error:{} use backup',e)
        logger.info(f"{retry_count} retry current proxy {proxy_url}")
        # proxy_url=None

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
# Asynchronous function to lookup a domain using RDAP protocol
async def lookup_domain(
    domain: str, proxy_url: str, semaphore: asyncio.Semaphore, outfile: Recorder, db_manager
):
    async with semaphore:
        query_url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj3jraUsoGGAxUvSGwGHUbfAEwQv5AHegQIABAE"

        session = None
        try:
            session = await getSession(proxy_url)
            if not session:
                return False

            response = await session.get(query_url, timeout=20, proxy=proxy_url if proxy_url else None)

            if response is None:
                logger.error(f"Received None as response for {query_url}")
                return False

            data = await response.text()
            if data is None:
                logger.error(f"Received None as data for {query_url}")
                return False

            if response.status == 200:
                soup = BeautifulSoup(data, "html.parser")
                elements_with_aaa = soup.find_all(lambda tag: "Site first indexed by Google" in tag.get_text())

                if len(elements_with_aaa) > 0:
                    r = elements_with_aaa[0].get_text().split("Site first indexed by Google")

                    if r:
                        data = {
                            "domain": domain,
                            "indexdate": r[-1].strip() if r else None,
                            'indexdata': r
                        }
                        outfile.add_data(data)

                        new_domain = db_manager.Domain(
                            url=domain,
                            tld=get_tld(domain),
                            indexat=r[-1].strip() if r else None
                        )
                        db_manager.add_domain(new_domain)

                        logger.info(f'Added data for domain: {domain}')
                        logger.info(
                            f"SUCCESS | {response.status} | {query_url.ljust(50)} | {domain}"
                        )
                        return True
            else:
                logger.warning(f"Non-200 status code: {response.status} for {domain}")
                return False

        except asyncio.TimeoutError as e:
            logger.error(
                f"TimeoutError | {query_url.ljust(50)} | {domain} | {e}"
            )
            raise
        except aiohttp.ClientError as e:
            logger.error(
                f"ClientError | {query_url.ljust(50)} | {domain} | {e}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Exception | {query_url.ljust(50)} | {domain} | {e}"
            )
            raise
        finally:
            if session:
                await session.close()

def getlocalproxies():
    raw_proxies = []
    import os
    for p in ['http','socks4','socks5']:
        proxyfile = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out\proxies\{p}.txt'


        proxy_dir = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out\proxies'
        proxyfile = os.path.join(proxy_dir, f'{p}.txt')
        if os.path.exists(proxyfile):

            tmp = open(proxyfile, "r", encoding="utf8").readlines()
            tmp = list(set(tmp))
            print('p',p,len(tmp))
            raw_proxies+= [f'{p}://'+v.replace("\n", "") for v in tmp if "\n" in v]

    raw_proxies=list(set(raw_proxies))
    return raw_proxies
    # print('raw count',len(raw_proxies))
    # valid_proxies=[]
    # checktasks=[]
    # for proxy_url in raw_proxies:
    #     task = asyncio.create_task(test_proxy('https://revved.com',proxy_url))
    #     checktasks.append(task)

    # for task in checktasks:
    #     good = await task
    #     if good:
    #         valid_proxies.append(proxy_url)
    # valid_proxies=raw_proxies
    # print('clean count',len(valid_proxies))

# Asynchronous function to process domains and fetch index dates
async def process_domains_indexdate(domains, outfile, counts, db_manager, semaphore):
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