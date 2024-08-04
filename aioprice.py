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
import aiohttp_socks

from loguru import logger
# from magic_html import GeneralExtractor

# 初始化提取器
# extractor = GeneralExtractor()
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
semaphore = asyncio.Semaphore(25)  # Allow up to 50 concurrent tasks
# db_manager = DatabaseManager()
filename='majestic_million'
# filename='toolify.ai-organic-competitors--'
filename='cftopai'
filename='toolify-top500'
# filename='character.ai-organic-competitors--'
# filename='efficient.app-organic-competitors--'
# filename='top-domains-1m'
# filename='artifacts'
# filename='ahref-top'
# filename='builtwith-top'
filename = "./tranco_Z377G"
filename="domain-1year"
folder_path='.'
inputfilepath=filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)
outfilepath=inputfilepath.replace('.csv','-prices.csv')
# outfilepath = "top-domains-1m-price.csv"

outfile = Recorder(folder_path+'/'+outfilepath, cache_size=10)
outcffilepath=inputfilepath.replace('.csv','-prices-cfblock.csv')

outcffile = Recorder(folder_path+'/'+outcffilepath, cache_size=10)


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




def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]
def get_price_from_html(html):
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
def get_des_from_html(html):
    description = 'not content!'
    try:

        # Parse the HTML content
        soup = BeautifulSoup(html, 'html.parser')


        # Extract the meta description
        description_tag = soup.find('meta', attrs={'name': 'description'})
        description = description_tag['content'] if description_tag else 'No description found'
        description=description.replace('\n','').replace('\r','')

        description=description.strip()
        logger.ino(f'find description:{description}')

    except:
        logger.error('cannot find description')
    return description
# More comprehensive dictionary mapping ISO 639-1 language codes to English names
language_codes = {
    'aa': 'Afar',
    'ab': 'Abkhazian',
    'af': 'Afrikaans',
    'am': 'Amharic',
    'ar': 'Arabic',
    'as': 'Assamese',
    'ay': 'Aymara',
    'az': 'Azerbaijani',
    'ba': 'Bashkir',
    'be': 'Belarusian',
    'bg': 'Bulgarian',
    'bh': 'Bihari',
    'bi': 'Bislama',
    'bn': 'Bengali',
    'bo': 'Tibetan',
    'br': 'Breton',
    'ca': 'Catalan',
    'cs': 'Czech',
    'cy': 'Welsh',
    'da': 'Danish',
    'de': 'German',
    'dz': 'Dzongkha',
    'el': 'Greek',
    'en': 'English',
    'eo': 'Esperanto',
    'es': 'Spanish',
    'et': 'Estonian',
    'eu': 'Basque',
    'fa': 'Persian',
    'fi': 'Finnish',
    'fj': 'Fijian',
    'fo': 'Faroese',
    'fr': 'French',
    'fy': 'Western Frisian',
    'ga': 'Irish',
    'gd': 'Scottish Gaelic',
    'gl': 'Galician',
    'gn': 'Guarani',
    'gu': 'Gujarati',
    'ha': 'Hausa',
    'he': 'Hebrew',
    'hi': 'Hindi',
    'hr': 'Croatian',
    'hu': 'Hungarian',
    'hy': 'Armenian',
    'ia': 'Interlingua',
    'id': 'Indonesian',
    'ie': 'Interlingue',
    'is': 'Icelandic',
    'it': 'Italian',
    'ja': 'Japanese',
    'jv': 'Javanese',
    'ka': 'Georgian',
    'kk': 'Kazakh',
    'kl': 'Kalaallisut',
    'km': 'Khmer',
    'kn': 'Kannada',
    'ko': 'Korean',
    'ks': 'Kashmiri',
    'ku': 'Kurdish',
    'ky': 'Kirundi',
    'la': 'Latin',
    'ln': 'Lingala',
    'lo': 'Lao',
    'lt': 'Lithuanian',
    'lv': 'Latvian',
    'mg': 'Malagasy',
    'mi': 'Maori',
    'mk': 'Macedonian',
    'ml': 'Malayalam',
    'mn': 'Mongolian',
    'mo': 'Moldavian',
    'mr': 'Marathi',
    'ms': 'Malay',
    'mt': 'Maltese',
    'my': 'Burmese',
    'na': 'Nauru',
    'ne': 'Nepali',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'oc': 'Occitan',
    'om': 'Oromo',
    'or': 'Oriya',
    'pa': 'Punjabi',
    'pl': 'Polish',
    'ps': 'Pashto',
    'pt': 'Portuguese',
    'qu': 'Quechua',
    'rm': 'Romansh',
    'rn': 'Rundi',
    'ro': 'Romanian',
    'ru': 'Russian',
    'rw': 'Kinyarwanda',
    'sa': 'Sanskrit',
    'sc': 'Sardinian',
    'sd': 'Sindhi',
    'se': 'Northern Sami',
    'sg': 'Sango',
    'sh': 'Serbo-Croatian',
    'si': 'Sinhalese',
    'sk': 'Slovak',
    'sl': 'Slovene',
    'sm': 'Samoan',
    'sn': 'Shona',
    'so': 'Somali',
    'sq': 'Albanian',
    'sr': 'Serbian',
    'ss': 'Swati',
    'st': 'Sotho',
    'su': 'Sudanese',
    'sv': 'Swedish',
    'sw': 'Swahili',
    'ta': 'Tamil',
    'te': 'Telugu',
    'tg': 'Tajik',
    'th': 'Thai',
    'ti': 'Tigrinya',
    'tk': 'Turkmen',
    'tl': 'Tagalog',
    'tn': 'Tswana',
    'to': 'Tonga',
    'tr': 'Turkish',
    'ts': 'Tsonga',
    'tt': 'Tatar',
    'tw': 'Twi',
    'ug': 'Uighur',
    'uk': 'Ukrainian',
    'ur': 'Urdu',
    'uz': 'Uzbek',
    'vi': 'Vietnamese',
    'vo': 'Volapük',
    'wo': 'Wolof',
    'xh': 'Xhosa',
    'xx-bork': 'Bork, bork, bork!',  # A fun, fictional language code
    'xx-hylian': 'Hylian',  # A fictional language from the Legend of Zelda
    'yi': 'Yiddish',
    'yo': 'Yoruba',
    'za': 'Zhuang',
    'zh': 'Chinese',
    'zu': 'Zulu'
}

# Function to get the English name of a language code
def get_language_name(code):
    return language_codes.get(code, "Unknown language code")

# Example usage
# language_code = 'fr'
# print(f"The language code '{language_code}' is for {get_language_name(language_code)}.")s
def getrawtext(html):
        import py3langid as langid
    

        import justext
        lang= langid.classify(rawtx)
        lagname= language_codes.get(lang, "English")

        paragraphs = justext.justext(html, justext.get_stoplist(lagname))

        rawtx=''
        for paragraph in paragraphs:
            if not paragraph.is_boilerplate:
                rawtx=rawtx+paragraph.text+'\n'
def gettext(html):
# https://github.com/wanghaisheng/htmltotext-benchmark/blob/master/algorithms.py
    import trafilatura
    return trafilatura.extract(html,output_format='txt')
def gettextmagic_html (html,domain):
    data = extractor.extract(html, base_url=domain)
def getrawmd(url):
    # https://md.dhr.wtf/
    url='https://md.dhr.wtf/?url=https://example.com'

# Function to extract data from HTTP response
async def extract_price(response,domain):
    try:


        data = await response.text()
        raw=None
        raw=gettext(data)

        # raw=gettextmagic_html(data,domain)
        # .replace('\r',' ').replace('\n',' ')
        # print('get raw',type(raw),raw)
        print('=','\n' in raw)
        prices=[]
        c_symbol=None

        # for s in ['$','£','¥','CNY']:
            # if  s in raw:
                # c_symbol=s
                # break
            # print('there is no',s)
        # if not c_symbol:
            # return
        # prices('currency',c_symbol)
        lines=raw
        if '\n' in raw:
            lines=raw.split('\n')
        # if 'Attention Required!' in raw:
        #     outcffile.add_data(domain)
        # if  not  'price' in raw:
        #     return False

        print('lines',lines)
        for line in lines:
            # parts = line.split('$')
            for s in ['$','£','¥','CNY']:

                if s in line:
                    prices.append(line)
                    print(f'found price:{line}')

        # import py3langid as langid
        # lang=None
        # if raw:
            # lang= langid.classify(raw)
        if  prices==[]:
            logger.info( 'not found price info')
        data = {
            'domain': domain,
            'priceurl':domain.split('/')[-1],
            'prices':prices,

            'raw':raw.replace('\r','').replace('\n','')
        }
        logger.info(data)

        print('add dataa')
        outfile.add_data(data)
        if len(prices)>0:
            return True
        else:
            return False
    except aiohttp.ClientConnectionError:
        print(f"Task {domain} failed on get extract_price  . Skipping.")

    except Exception:
        print(f"Task {domain} failed on get extract_price  . Skipping.")





# Function to simulate a task asynchronously
async def get_priceplan(domain,valid_proxies):
    async with semaphore:
        url =f'https://{domain}'       
        retries =1
        for attempt in range(1, retries + 1):
            try:
                proxy_url=None

                # proxy_url = "http://127.0.0.1:1080"  # Example SOCKS5 proxy URL
                connector = aiohttp_socks.ProxyConnector.from_url(proxy_url) if proxy_url and proxy_url.startswith("socks") else None
                proxy=proxy_url if proxy_url and 'http' in proxy_url else None
                print('===proxy_url',proxy,proxy_url)
                async with aiohttp.ClientSession(connector=connector) as session:                
                    async with session.get(url,proxy=proxy) as response:
                        if response.status == 200:
                            data=await response.text()
                            if 'Attention Required!' in data or 'Cloudflare Ray ID' in data or 'Sorry, you have been blocked' in data:
                                outcffile.add_data(domain)   
                                break
              
                            data = await extract_price(response,domain)
                            if data:
                                logger.info(f"Task {url} completed on attempt {attempt}. Data: {data}")
                                return
                            else:
                                return 
                        elif response.status==404:
                            break
                        else:
                            print(f"Task {url} failed on attempt {attempt}.{proxy} Status code: {response.status}{await response.text()}")

                            if 'Attention Required!' in data or 'Cloudflare Ray ID' in data or 'Sorry, you have been blocked' in data:
                                outcffile.add_data(domain)   
                                break
            except aiohttp.ClientConnectionError:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}.{proxy} ClientConnectionError Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. ClientConnectionError.")

            except Exception:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Exception Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Exception.")


# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
def cleandomain(domain):
    if isinstance(domain,str)==False:
        
        domain=str(domain)

    domain=domain.strip()
    # if not 'http' in domain:
    #     domain='https://'+domain
    # if "https://" in domain:
    #     domain = domain.replace("https://", "")
    # if "http://" in domain:
    #     domain = domain.replace("http://", "")
    # if "www." in domain:
    #     domain = domain.replace("www.", "")
    # if domain.endswith("/"):
    #     domain = domain.rstrip("/")
    return domain
def getlocalproxies():

    raw_proxies = []

    for p in ['http','socks4','socks5']:
        proxyfile = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies\{p}.txt'


        proxy_dir = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-google\proxies'
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
# Function to run tasks asynchronously with specific concurrency
async def run_async_tasks():
    tasks = []
    df = pd.read_csv(inputfilepath, encoding="ISO-8859-1")

    # filtered_df = df[df['indexdate'] != 'unk']
    # filtered_df = df[df['indexdate'].str.contains('month', case=False, na=False)]
    

    # # filtered_df = df[df['indexdate'].str.contains('1 year', case=False, na=False)]

    # domains=filtered_df['domain'].to_list()

    # domains=set(domains)
    # df1year = pd.DataFrame(domains, columns=['domain'])
    # df1year.to_csv('domain-1year.csv')

    domains=df['domain'].to_list()

    domains=set(domains)    
    print(f'load domains：{len(domains)}')
    donedomains=[]
    # domains=['tutorai.me','magicslides.app']
    # try:
    #     db_manager = DatabaseManager()

    #     dbdata=db_manager.read_domain_all()

    #     for i in dbdata:
    #         if i.title is not None:
    #             donedomains.append(i.url)        
    # except Exception as e:
    #     print(f'query error: {e}')
    alldonedomains=[]

    if os.path.exists(outfilepath):
        df=pd.read_csv(outfilepath
                    #    ,encoding="ISO-8859-1"
                       )
        alldonedomains=df['domain'].to_list()
    # else:
        # df=pd.read_csv('top-domains-1m.csv')

        # donedomains=df['domain'].to_list()
    alldonedomains=set(alldonedomains)

    print(f'load alldonedomains:{len(list(alldonedomains))}')

    donedomains=[element for element in domains if element  in alldonedomains]
    print(f'load done domains {len(donedomains)}')
    tododomains=list(set([cleandomain(i) for i in domains])-set(donedomains))
    print(f'to be done {len(tododomains)}')
    for domain in tododomains:



        domain=cleandomain(domain)
        for suffix in [''
                    #    ,'premium','price','#price','#pricing','pricing','price-plan','pricing-plan','upgrade','purchase'
                       ]:



            task = asyncio.create_task(get_priceplan(domain+'/'+suffix,[]))
            tasks.append(task)
            if len(tasks) >= 100:
                # Wait for the current batch of tasks to complete
                await asyncio.gather(*tasks)
                tasks = []            
    await asyncio.gather(*tasks)

# Example usage: Main coroutine
async def main():
    start_time = time.time()
    await run_async_tasks()
    print(f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds")
# Manually manage the event loop in Jupyter Notebook or other environments
if __name__ == "__main__":
    logger.add(filename+'price-debug.log')

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    outfile.record()
    outcffile.record()

