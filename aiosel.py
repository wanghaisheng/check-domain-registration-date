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
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import time
import numpy as np
import datetime
# Replace this with your actual test URL
test_url = "http://example.com"

# Replace this with your actual outfile object and method for adding data
# outfile = YourOutfileClass()
MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global variable to store RDAP servers
RDAP_SERVERS = {}




# Semaphore to control concurrency
semaphore = asyncio.Semaphore(100)  # Allow up to 50 concurrent tasks
# db_manager = DatabaseManager()

filename='sel'

# filename='ahref-top'

folder_path='./output'
inputfilepath=filename + ".csv"
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)
outfilepath=inputfilepath.replace('.csv','-seller.csv')
outfile = Recorder(folder_path+'/'+outfilepath, cache_size=10)

keys = [
    "trackedSince",
    "domainId",
    "sellerId",
    "sellerName",
    "privatelabel",
    "lastUpdate",
    "isScammer",
    "hasFBA",
    "csv",
    "totalStorefrontAsins",
    "totalStorefrontAsins-updatetime",
    "totalStorefrontAsins-count",
    "sellerCategoryStatistics",
    "sellerCategoryStatistics-count",
    "sellerCategoryList",
    "sellerBrandStatistics",
    "sellerBrandStatistics-count",
    "sellerBrandList",
    "shipsFromChina",
    "address",
    "recentFeedback",
    "recentFeedback-count",
    "lastRatingUpdate",
    "neutralRating",
    "negativeRating",
    "positiveRating",
    "ratingCount",
    "businessName",
    "phoneNumber",
    "currentRating",
    "currentRatingCount",
    "ratingsLast30Days",
    "timestamp",
    "asinList",
    "asinListLastSeen",
    "vatID",
    "businessType",
    "shareCapital",
    "representative",
    "email",
    "customerServicesAddress",
]
keepaCatNameByIdTable = {
    "us": {
        133140011: "Kindle Store",
        9013971011: "Video Shorts",
        2350149011: "Apps & Games",
        165796011: "Baby Products",
        163856011: "Digital Music",
        13727921011: "Alexa Skills",
        165793011: "Toys & Games",
        2972638011: "Patio, Lawn & Garden",
        283155: "Books",
        2617941011: "Arts, Crafts & Sewing",
        229534: "Software",
        3375251: "Sports & Outdoors",
        2238192011: "Gift Cards",
        468642: "Video Games",
        11260432011: "Handmade Products",
        7141123011: "Clothing, Shoes & Jewelry",
        1064954: "Office Products",
        16310101: "Grocery & Gourmet Food",
        228013: "Tools & Home Improvement",
        2625373011: "Movies & TV",
        11091801: "Musical Instruments",
        4991425011: "Collectibles & Fine Art",
        2619525011: "Appliances",
        2619533011: "Pet Supplies",
        2335752011: "Cell Phones & Accessories",
        16310091: "Industrial & Scientific",
        10272111: "Everything Else",
        5174: "CDs & Vinyl",
        3760911: "Beauty & Personal Care",
        1055398: "Home & Kitchen",
        172282: "Electronics",
        15684181: "Automotive",
        599858: "Magazine Subscriptions",
        3760901: "Health & Household",
        18145289011: "Audible Books & Originals",
    },
    "ca": {
        2206275011: "Home & Kitchen",
        6205511011: "Office Products",
        3006902011: "Tools & Home Improvement",
        21204935011: "Clothing, Shoes & Accessories",
        16708697011: "Handmade",
        917972: "Movies & TV",
        2972705011: "Kindle Store",
        3561346011: "Baby",
        11076213011: "Industrial & Scientific",
        2242989011: "Sports & Outdoors",
        6205499011: "Patio, Lawn & Garden",
        916520: "Books",
        6967215011: "Grocery & Gourmet Food",
        18730296011: "Prime Video",
        916516: "Featured Stores",
        6205177011: "Health & Personal Care",
        6386371011: "Apps for Android",
        "223.7.1011": "Watches",
        6916844011: "Musical Instruments, Stage & Studio",
        16286269011: "Alexa Skills",
        667823011: "Electronics",
        916514: "Music",
        6205124011: "Beauty & Personal Care",
        6205517011: "Toys & Games",
        3198021: "Software",
        2356392011: "Everything Else",
        6948389011: "Automotive",
        6205514011: "Pet Supplies",
        3198031: "Video Games",
    },
}


KEEPA_ST_ORDINAL = np.datetime64("2011-01-01")


def unix_timestamp_ms_to_date(ms_timestamp):
    """
    Convert a Unix timestamp in milliseconds to a human-readable date string.

    :param ms_timestamp: Unix timestamp in milliseconds.
    :return: Human-readable date string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    # Convert milliseconds to seconds
    seconds = ms_timestamp // 1000

    # Convert Unix epoch time to a datetime object
    date_obj = datetime.datetime.utcfromtimestamp(seconds)

    # Format the datetime object into a string
    date_str = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    return date_str


def unix_timestamp_s_to_date(timestamp):
    """
    Convert a Unix timestamp to a human-readable date string.

    :param timestamp: Unix timestamp in seconds.
    :return: Human-readable date string in 'YYYY-MM-DD HH:MM:SS' format.
    """
    # Convert the timestamp to a datetime object
    if timestamp == "" or timestamp is None:
        return ""
    else:
        date_obj = datetime.datetime.utcfromtimestamp(timestamp)

        # Format the datetime object into a string
        date_str = date_obj.strftime("%Y-%m-%d %H:%M:%S")

        return date_str


def keepa_minutes_to_time(minutes, to_datetime=True):
    """Accept an array or list of minutes and converts it to a numpy datetime array.

    Assumes that keepa time is from keepa minutes from ordinal.
    """
    # Convert to timedelta64 and shift
    dt = np.array(minutes, dtype="timedelta64[m]")
    dt = dt + KEEPA_ST_ORDINAL  # shift from ordinal

    # Convert to datetime if requested
    if to_datetime:
        return dt.astype(datetime.datetime)
    return dt


# 对每个键进行操作
def standardize_column_names(df):
    for key in keys:
        if key not in df.columns:
            # 对于数值类型的键，可以初始化为0
            if key in [
                "trackedSince",
                "domainId",
                "lastUpdate",
                "ratingCount",
                "currentRating",
                "currentRatingCount",
                "ratingsLast30Days",
            ]:
                df[key] = 0
            # 对于布尔类型的键，可以初始化为False
            elif key in ["isScammer", "hasFBA", "shipsFromChina", "privatelabel"]:
                df[key] = False
            # 对于其他类型的键，可以初始化为空字符串
            else:
                df[key] = ""
    # df['csv']=None
    return df


import re


def check_similarity_new(correct_sentence, wrong_sentence):
    overall_percentage = 0

    def extract_words(input_string):
        words = re.findall(r"\b\w+\b", input_string)
        return words

    def count_letters(input_string):
        letter_counts = {}
        for letter in input_string:
            letter = letter.lower()
            letter_counts[letter] = letter_counts.get(letter, 0) + 1
        return letter_counts

    def word_similarity_check(dic1, dic2):
        word_similarity_score = 0

        if len(dic1) >= len(dic2):
            for letter in dic1.keys():
                if letter in dic2.keys():
                    if dic1[letter] >= dic2[letter]:
                        word_similarity_score = word_similarity_score + (
                            (dic2[letter] / dic1[letter]) * 100
                        )
                    if dic1[letter] < dic2[letter]:
                        word_similarity_score = word_similarity_score + (
                            (dic1[letter] / dic2[letter]) * 100
                        )
            return word_similarity_score / len(dic1)

        if len(dic1) < len(dic2):
            for letter in dic2.keys():
                if letter in dic1.keys():
                    if dic1[letter] >= dic2[letter]:
                        word_similarity_score = word_similarity_score + (
                            (dic2[letter] / dic1[letter]) * 100
                        )
                    if dic1[letter] < dic2[letter]:
                        word_similarity_score = word_similarity_score + (
                            (dic1[letter] / dic2[letter]) * 100
                        )
            return word_similarity_score / len(dic2)

    for correct_word, wrong_word in zip(
        extract_words(correct_sentence), extract_words(wrong_sentence)
    ):
        overall_percentage = overall_percentage + word_similarity_check(
            count_letters(correct_word), count_letters(wrong_word)
        )

    if extract_words(correct_sentence) >= extract_words(wrong_sentence):
        divider = len(extract_words(correct_sentence))
    elif extract_words(correct_sentence) < extract_words(wrong_sentence):
        divider = len(extract_words(wrong_sentence))

    return overall_percentage / divider


def json2csv(data,seller_id):
    """
    Reads and processes all JSON files within the specified folder path.
    """

    # Extract the seller data using the filename as the key
    # print(data)
    # print(seller_id,data.get('sellers'))
    try:
        seller_data = data["sellers"][seller_id]
        print('tiqu seller')
        for key in keys:
            if key not in seller_data:
                seller_data[key] = ""
                # print("missing col", key)
        if "csv" in seller_data:
            seller_data["csv"] = ""
        # print(seller_data)
        # for key in list(seller_data.keys()):

        if "phoneNumber" in seller_data:
            print(seller_id, seller_data["phoneNumber"])
        seller_data["totalStorefrontAsins-updatetime"] = ""
        if "totalStorefrontAsins" in seller_data:
            seller_data["totalStorefrontAsins-count"] = 0
            if (
                seller_data["totalStorefrontAsins"]
                and len(seller_data["totalStorefrontAsins"]) > 0
            ):
                asincount = seller_data["totalStorefrontAsins"][-1]
                seller_data["totalStorefrontAsins-count"] = asincount
                seller_data["totalStorefrontAsins-updatetime"] = (
                    keepa_minutes_to_time(
                        seller_data["totalStorefrontAsins"][0]
                    )
                )
        # print(key,type(seller_data[key]))
        for key in [
            "" "sellerCategoryStatistics",
            # "sellerCategoryStatistics-count",
            "sellerBrandStatistics",
            # "sellerBrandStatistics-count",
            "recentFeedback",
            # "recentFeedback-count",
        ]:
            seller_data[key + "-count"] = len(seller_data[key])
            # print('add count')
        seller_data["sellerCategoryList"] = []
        seller_data["sellerCategoryList-count"] = 0

        if (
            "sellerCategoryStatistics" in seller_data
            and len(seller_data.get("sellerCategoryStatistics")) > 0
        ):
            # 初始化一个空列表来存储所有的brand值
            cats = []

            # 遍历列表中的每个元素（这里只有一个元素，因为它是一个单层列表）
            for item in seller_data["sellerCategoryStatistics"]:
                if item:

                    if item["catId"] not in keepaCatNameByIdTable["us"]:
                        keepaCatNameByIdTable["us"][item["catId"]] = "unknown"
                    cat = keepaCatNameByIdTable["us"][item["catId"]]
                    item["catname"] = cat

                    cats.append(cat)
            # print(cats)
            seller_data["sellerCategoryList"] = cats
            seller_data["sellerCategoryList-count"] = len(cats)

        # print(seller_data['sellerCategoryList'])

        if "sellerBrandStatistics" in seller_data:

            seller_data["sellerBrandList"] = []
            seller_data["sellerBrandList-count"] = 0

            seller_data["privatelabel"] = False

            if (
                "sellerBrandStatistics" in seller_data
                and len(seller_data.get("sellerBrandStatistics")) > 0
            ):
                # 初始化一个空列表来存储所有的brand值
                brands = []

                # 遍历列表中的每个元素（这里只有一个元素，因为它是一个单层列表）
                for item in seller_data["sellerBrandStatistics"]:
                    # 将brand值添加到brands列表中
                    if item:
                        brands.append(item["brand"])
                seller_data["sellerBrandList"] = brands
                seller_data["sellerBrandList-count"] = len(brands)

                if len(brands) == 1:

                    if seller_data["sellerName"]:
                        isprivate = check_similarity_new(
                            brands[0], seller_data["sellerName"]
                        )
                        if isprivate > 0.8:
                            seller_data["privatelabel"] = True

        if "lastUpdate" in seller_data and seller_data["lastUpdate"] != "":
            seller_data["lastUpdate"] = keepa_minutes_to_time(
                seller_data["lastUpdate"]
            )
        if (
            "lastRatingUpdate" in seller_data
            and seller_data["lastRatingUpdate"]
        ):

            seller_data["lastRatingUpdate"] = keepa_minutes_to_time(
                seller_data["lastRatingUpdate"]
            )

        if "timestamp" in data and data["timestamp"]:

            seller_data["timestamp"] = unix_timestamp_ms_to_date(
                data["timestamp"]
            )

        else:
            seller_data["timestamp"] = ""
        if "address" in seller_data and len(seller_data["address"]) > 0:

            country = seller_data["address"][-1]
            seller_data["country"] = country
        else:
            seller_data["country"] = ""

        for i in [
            "neutralRating",
            "negativeRating",
            "positiveRating",
            "ratingCount",
        ]:
            # print(seller_data[i], type(seller_data[i]))
            seller_data["1month-" + i] = ""
            seller_data["3month-" + i] = ""
            seller_data["12month-" + i] = ""
            seller_data["lifetime-" + i] = ""
            if len(seller_data[i]) == 4:
                seller_data["1month-" + i] = seller_data[i][0]
                seller_data["3month-" + i] = seller_data[i][1]
                seller_data["12month-" + i] = seller_data[i][2]
                seller_data["lifetime-" + i] = seller_data[i][-1]

        # List of keys to ignore
        keys_to_ignore = [
            "neutralRating",
            "negativeRating",
            "positiveRating",
            "ratingCount",
            "totalStorefrontAsins",
        ]

        # Create a new dictionary with keys not in the ignore list
        filtered_dict = {
            key: value
            for key, value in seller_data.items()
            if key not in keys_to_ignore
        }
        print("country", seller_id, seller_data["country"])
    except:
        print('there is no seller key in json')
    return seller_data



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


async def read_and_process_json_files(filename, folder_path):
    """
    Reads and processes all JSON files within the specified folder path.
    """
    # 确保csv目录存在
    if True:
        # Construct the full file path
        file_path = os.path.join(folder_path, filename)
        seller_id = filename.replace(".json", "")
        print('json filepath',file_path,os.path.isfile(file_path) )
        # Define the CSV file name

        # Check if it's a file and has a .json extension
        if os.path.isfile(file_path) and filename.endswith(".json"):
            try:
                # Open and read the JSON file
                with open(file_path, "r", encoding="utf8") as json_file:
                    data = json.load(json_file)
                    print('load json from file',type(data))
                    seller_data = data["sellers"][seller_id]
                    print('json to csv')

                    raw=json2csv(data,seller_id)
                    data = raw
                    outfile.add_data(data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from file {filename}: {e}")


            except Exception as e:
                print(f"An error occurred while reading the file {filename}: {e}")

async def extract_sellerjson(raw,sellerid):
    try:
        date='unk'
        print(raw)
        try:
            raw["sellers"][sellerid]
            # with open(
            #     "json/" + sellerid + ".json", "w", encoding="utf8"
            # ) as f:
            #     json.dump(raw, f, ensure_ascii=False, indent=4)
            raw=json2csv(raw,sellerid)
        except:
            print("no seller json",sellerid)
            raw={}
        data = raw
        outfile.add_data(data)
        print(f"Data saved for sellerid: {sellerid}")

        return True
    except Exception as e:
        logger.error(f'parse index date error for:{e}')

        return False
def savedb(outfile,domain,domaindata):

    # Domain=
    new_domain = Domain(
        url=domain,tld=get_tld(domain),
    title=None,
    indexat=r[-1] or None,
    des=None,
    bornat=None)
    add_domain(new_domain)


# Function to simulate a task asynchronously
async def get_index_date(domain,valid_proxies):
    sellerid=domain
    url = "https://mercury.revseller.com/api/us/data5/"

    headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,zh;q=0.8,zh-CN;q=0.7",
    "Origin": "chrome-extension://gobliffocflfaekfcaccndlffkhcafhb",
    "Priority": "u=1, i",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}    
    async with semaphore:
        url = url + sellerid + "?domain=1&seller=" + sellerid

        retries = 4
        for attempt in range(1, retries + 1):
            try:
                proxy_url =None
                if attempt==1:
                    proxy_url=await get_proxy_proxypool()
                    proxy_url = f"http://{proxy_url}"  # Example SOCKS5 proxy URL

                if attempt==3:
                    # proxy_url=await get_proxy_proxypool()
                    proxy_url = "socks5://127.0.0.1:1080"  # Example SOCKS5 proxy URL
                elif attempt==2:
                    if valid_proxies:
                        proxy_url=random.choice(valid_proxies)
                    print('2======',proxy_url)
                elif attempt==4:
                    proxy_url=await get_proxy()
                    proxy_url = f"http://{proxy_url}"  # Example SOCKS5 proxy URL


                # proxy_url = "socks5://127.0.0.1:9050"  # Example SOCKS5 proxy URL
                connector = aiohttp_socks.ProxyConnector.from_url(proxy_url) if proxy_url and proxy_url.startswith("socks") else None
                proxy=proxy_url if proxy_url and 'http' in proxy_url else None
                print('===proxy',proxy,domain)
                async with aiohttp.ClientSession(connector=connector) as session:                
                    async with session.get(url,proxy=proxy,headers=headers) as response:
                        if response.status == 200:
                            raw = await response.json()

                            data = await extract_sellerjson(raw,domain)
                            # print('data',data)
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
                if proxy_url in valid_proxies:
                    valid_proxies.remove(proxy_url)

            except Exception:
                if attempt < retries:
                    print(f"Task {url} failed on attempt {attempt}. Retrying...")
                else:
                    print(f"Task {url} failed on all {retries} attempts. Skipping.")
                if proxy_url in valid_proxies:
                    valid_proxies.remove(proxy_url)


# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
def cleandomain(domain):
    if isinstance(domain,str)==False:
        domain=str(domain)
    domain=domain.strip()
    
    return domain
def getlocalproxies():

    raw_proxies = []

    for p in ['http','socks4','socks5']:
        proxyfile = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-revseller\proxies\{p}.txt'


        proxy_dir = r'D:\Download\audio-visual\a_proxy_Tool\proxy-scraper-checker\out-revseller\proxies'
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



async def prejson(folder_path):
    tasks = []

    for filename in os.listdir(folder_path):
        task = asyncio.create_task(read_and_process_json_files(filename,folder_path))
        tasks.append(task)
        if len(tasks) >= 100:
            # Wait for the current batch of tasks to complete
            await asyncio.gather(*tasks)
            tasks = []            
    await asyncio.gather(*tasks)



async def run_async_tasks():
    tasks = []
    df = pd.read_csv(inputfilepath,
                    #  , encoding="ISO-8859-1"
                    usecols=['sellerid']
                     )
    domains=df['sellerid'].to_list()
    print(f'load domains:{len(domains)}')
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
        donedomains=df['sellerId'].to_list()
    # else:
        # df=pd.read_csv('top-domains-1m.csv')
        # donedomains=df['sellerid'].to_list()
    donedomains=list(set(donedomains))
    print(f'load donedomains:{len(donedomains)}')
    valid_proxies=getlocalproxies()

    domains=list(set([cleandomain(i) for i in domains])-set(donedomains))
    print(f'to be done {len(domains)}')
    time.sleep(30)
    cnts=0
    for domain in domains:

        domain=cleandomain(domain)
        if os.path.exists(os.path.join(r'D:\Download\audio-visual\amazon\top10kseller',domain+'.json')):
            cnts+=1
            continue
        if len(domain)<10:
            cnts+=1

            continue
        if  domain not in donedomains:
            print('add domain',domain)
            task = asyncio.create_task(get_index_date(domain,valid_proxies))
            tasks.append(task)
            if len(tasks) >= 100:
                # Wait for the current batch of tasks to complete
                await asyncio.gather(*tasks)
                tasks = []            
    await asyncio.gather(*tasks)
    print('ignore===========',cnts)
# Example usage: Main coroutine
async def main():
    start_time = time.time()
    # await prejson('./json')

    await run_async_tasks()
    print(f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds")

# Manually manage the event loop in Jupyter Notebook or other environments
if __name__ == "__main__":
    # logger.add('a-seller.log')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()
    outfile.record()
