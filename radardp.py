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
import time




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
from dbhelper import DatabaseManager
# Usage
# Now you can use db_manager.add_screenshot(), db_manager.read_screenshot_by_url(), etc.
from loguru import logger
import threading


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
semaphore = threading.Semaphore(5)  # Allow up to 5 concurrent tasks


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

def get_proxy():
    proxy=None
    with aiohttp.ClientSession() as session:
        try:
            with session.get('http://demo.spiderpy.cn/get') as response:
                data =  response.json()
                proxy=data['proxy']
                return proxy
        except:
            pass
def get_proxy_proxypool():
    with aiohttp.ClientSession() as session:

        if proxy is None:
            try:
                with session.get('https://proxypool.scrape.center/random') as response:
                    proxy =  response.text()
                    return proxy
            except:
                return None

def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]


def submit_radar_with_retry(
        browser,
    domain: str,
    url:str,
    valid_proxies: list,
    proxy_url: str,
    outfile: Recorder,
):
    retry_count = 0
    while retry_count < MAX_RETRIES:
        if retry_count>0:
            pro_str=None
            proxy_url = "http://127.0.0.1:1080"  # Example SOCKS5 proxy URL
            if retry_count>1:
                if valid_proxies:
                    proxy_url=random.choice(valid_proxies)
                else:
                    try:
                        pro_str= get_proxy()

                        if pro_str is None:
                    
                            pro_str= get_proxy_proxypool()


                    except Exception as e:
                        logger.error('get proxy error:{} use backup',e)
                if pro_str:
                    proxy_url = "http://{}".format(pro_str)            
    
        logger.info("current proxy{}", proxy_url)

        try:
            with semaphore:

                result =      submit_radar(browser,domain,url, proxy_url, outfile)
                
                if result:
                    if proxy_url and proxy_url not in valid_proxies:
                        valid_proxies.append(proxy_url)
                    return result
        except asyncio.TimeoutError:
            logger.error(
                f"Timeout occurred for domain: {domain} with proxy: {proxy_url}"
            )
        except Exception as e:
            logger.error(f"Error occurred: {e}")
        retry_count += 1
        # if retry_count < MAX_RETRIES:
        #     delay = min(INITIAL_DELAY * (2 ** retry_count), MAX_DELAY)
        #     logger.info(f"Retrying in {delay} seconds with proxy {proxy_url}...")
        #     await asyncio.sleep(delay)

    logger.error(f"Max retries reached for domain: {domain}")
    return None

import uuid

def is_valid_uuid(uuid_to_test, version=4):
    try:
        # This will check if the UUID is valid and raise a ValueError if not
        val = uuid.UUID(uuid_to_test, version=version)
        return str(val) == uuid_to_test
    except ValueError:
        # The UUID is not valid
        return False



def submit_radar(browser,
    domain: str, url:str,proxy_url: str, outfile: Recorder
):
    """
    Looks up a domain using the RDAP protocol.

    :param domain: The domain to look up.
    :param proxy_url: The proxy URL to use for the request.
    :param semaphore: The semaphore to use for concurrency limiting.
    """

    with semaphore:



        query_url=url
        

        logger.info("use proxy_url:{}", proxy_url)

        logger.info("querying:{}", query_url)
        page=None
        try:
            headless
        except:
            headless=True
            
        try:
            page=browser.driver.new_tab()

            # page = browser.driver.get_tab(tab)
            page.get(query_url)    
            page.wait.load_start()
            if page.json:
                data = {
                    "domain": domain,
                    'url':url.split('?')[0],
                    'result':page.json

                }
                outfile.add_data(data)
                # Domain=
                # new_domain = db_manager.Screenshot(
                    # url=domain,
                # uuid=uuid)
                # db_manager.add_screenshot(new_domain)


                logger.info(
                    f"{GREEN}SUCCESS {GREY}| {BLUE}{uuid} {GREY}| {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain}{GREEN}"
                )
                return True
            else:
                logger.warning(f"Non-valid uuid: {uuid} for {domain}")
                return False

        except asyncio.TimeoutError as e:
            logger.info(
                f"{RED} TimeoutError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
            )
            raise

        except aiohttp.ClientError as e:
            logger.info(
                f"{RED} ClientError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
            )
            raise

        except Exception as e:
            # page.close()
            # 需要注意的是，程序结束时浏览器不会自动关闭，下次运行会继续接管该浏览器。

    # 无头浏览器因为看不见很容易被忽视。可在程序结尾用page.quit()将其关闭 不 quit 还是会无头模式
            # headless=False
            print('start a new browser to get fresh cookie')
            # newbrowser=None
            # if proxy_url:
            #     newbrowser=DPHelper(browser_path=None,HEADLESS=False,proxy_server=f'http://{proxy_url}')
                    
            # else:
            #     newbrowser=DPHelper(browser_path=None,HEADLESS=False)
            # cookie=newbrowser.bypass(query_url)
            # page=newbrowser.driver                                    
            # page.get(query_url)                
            # newbrowser.saveCookie(cookiepath,cookie)
            # with open('error.html','w',encoding='utf8') as f:
            #     f.write(page.html)

            logger.info(
                f"{RED}Exception  {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
            )
            return

        finally:
            if page:
                page.close()
            print('finally')

@asynccontextmanager
async def aiohttp_session(url):
    async with aiohttp.ClientSession() as session:
        yield session


async def test_proxy(test_url, proxy_url):
    try:
        async with aiohttp_session(test_url) as session:
            # Determine the type of proxy and prepare the appropriate proxy header

            # Make the request
            async with session.get(test_url, timeout=10, proxy=proxy_url) as response:
                if uuid == 200:

                    # outfile.add_data(proxy_url)  # Uncomment and replace with your actual implementation
                    return True
                else:

                    return False
    except asyncio.TimeoutError:
        # print(f"{Style.BRIGHT}{Color.red}Invalid Proxy (Timeout) | {proxy_url}{Style.RESET_ALL}")
        return False
    except aiohttp.ClientError:

        return False


def getlocalproxies():

    raw_proxies = []

    for p in ['http','socks4','socks5']:
        proxyfile = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-cf\proxies\{p}.txt'


        proxy_dir = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-cf\proxies'
        proxyfile = os.path.join(proxy_dir, f'{p}.txt')
        if os.path.exists(proxyfile):

            tmp = open(proxyfile, "r", encoding="utf8").readlines()
            tmp = list(set(tmp))
            print('p',p,len(tmp))
            raw_proxies+= [f'{p}://'+v.replace("\n", "") for v in tmp if "\n" in v]

    raw_proxies=list(set(raw_proxies))
    print('raw count',len(raw_proxies))
    valid_proxies=[]
    # checktasks=[]
    # for proxy_url in raw_proxies:
    #     task = asyncio.create_task(test_proxy('https://revved.com',proxy_url))
    #     checktasks.append(task)

    # for task in checktasks:
    #     good = await task
    #     if good:
    #         valid_proxies.append(proxy_url)
    valid_proxies=raw_proxies
    print('clean count',len(valid_proxies))
    return valid_proxies    

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

def process_domains_screensht(domains, outfile,counts):
    from DPhelper import DPHelper
    browser=DPHelper(browser_path=None,HEADLESS=True)
    


    tasks = []
    domains = list(set(domains))
    if counts!=0:
        domains=domains[:counts]

    valid_proxies=getlocalproxies()
       
    for domain in domains:
        domain=cleandomain(domain)
        # print('===',domain)

        if (
            domain
        ):
            print(domain)

            proxy = None

            # if len(valid_proxies)>1:
            #     proxy=random.choice(valid_proxies)
            #     print('pick proxy',proxy)

            # proxy = "socks5h://127.0.0.1:1080"
            tld = get_tld(domain)
            urls=[
    f'https://radar.cloudflare.com/api/search?query={domain}',# uuid for screenshot

    # f'https://radar.cloudflare.com/charts/WhoisPanel/fetch?domain={domain}',
    f'https://radar.cloudflare.com/charts/DomainCategoriesPanel/fetch?domain={domain}',
    f'https://radar.cloudflare.com/charts/DomainInfoPanel/fetch?domain={domain}',
    f'https://radar.cloudflare.com/charts/VisitorLocationCombined/fetch?domain={domain}',
    f'https://radar.cloudflare.com/charts/ScansTable/fetch?domain={domain}&type=url&value={domain}',
    # f'https://radar.cloudflare.com/charts/TLSCertificatesTable/fetch?domain={domain}'
    
    ]
            for url in urls:

                try:

                    task = threading.Thread(target=submit_radar_with_retry, args=(browser, domain,url,valid_proxies, proxy,outfile))
                    tasks.append(task)
                    task.start()
                except Exception as e:
                    print(f"{RED}An error occurred while processing {domain}: {e}")
                if len(tasks) >= 5:
            
                    [task.join() for task in tasks]
                    tasks=[]
    for task in tasks:
        task.join()


# 拿到uuid以后可以构造
# https://radar.cloudflare.com/api/url-scanner/b184ae3b-7c75-4438-9c03-4712189c3f81/screenshot?resolution=desktop
# 出站链接
#https://radar.cloudflare.com/scan/b184ae3b-7c75-4438-9c03-4712189c3f81/dom?_data=routes%2Fscan%2F%24uuid%2Fdom


        # page.close()
filename='top1000ai'
filename='toolify.ai-organic-competitors--'
filename='toolify.ai-organic-competitors--'
# filename='toolify-top500'
filename='cftopai'
filename='ahref-top'
filename='builtwith-top'
folder_path='./output'
colname='domain'
counts=0
filename = os.getenv("filename")
colname = os.getenv("colname")
counts = os.getenv("counts")
try:
    counts=int(counts)
except:
    counts=0

headless=True
cookiepath='cookie.txt'
start=datetime.now()
inputfilepath=filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)
outfilepath=inputfilepath.replace('.csv','-cftools.csv')
outfile = Recorder(folder_path+'/'+outfilepath, cache_size=50)
failedfilepath=inputfilepath.replace('.csv','-cftools-error.csv')
failedfile = Recorder(folder_path+'/'+failedfilepath, cache_size=50)

df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")
domains=df[colname].tolist()
print('domain total',len(domains))
time.sleep(30)
# db_manager = DatabaseManager()
# dbdata=db_manager.read_screenshot_all()
donedomains=[]
# for i in dbdata:
    # if i.uuid is not None:
        # donedomains.append(i.url)
if os.path.exists(outfilepath):
    df=pd.read_csv(outfilepath)
    donedomains=df['domain'].to_list()

domains=list(set([cleandomain(i) for i in domains])-set(donedomains))
print('domain to done',len(domains))
process_domains_screensht(domains,outfile,counts)
end=datetime.now()
print('costing',end-start)
outfile.record()
