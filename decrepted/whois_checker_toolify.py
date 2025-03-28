import pandas as pd

import sqlite3
from contextlib import closing
import time, random
import requests
import whodap
import json
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import Counter
from bs4 import BeautifulSoup

filename = os.getenv("URL")
filename = "1"

output_folder = "./output"
if not os.path.exists("output"):
    os.mkdir("output")
headers_list = [
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Ubuntu/11.10 Chromium/27.0.1453.93 Chrome/27.0.1453.93 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0)"
    },
    {"User-Agent": "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)"},
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)"},
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)"},
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1"
    },
    {
        "User-Agent": "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11"
    },
    {"User-Agent": "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11"},
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11"
    },
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)"},
    {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)"
    },
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"},
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)"},
    {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)"
    },
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)"},
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)"},
    {"User-Agent": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"},
]

# Path to your CSV file
csv_file = "stripe\\stripe-final_combined.csv"
proxies = []
# with open("valid-proxies.txt", "r", encoding="utf8") as f:
#     proxies = [line.strip() for line in f.readlines() if line.strip()]

proxies = ["116.108.27.163:4005", "116.108.139.209:4006", "27.76.64.175:4001"]
# Define the increment value
increment_value = 1

def get_domain_first_index_date2(url):

    url="https://www.google.com/search?q=About+https://www.remini.com&tbm=ilp&sa=X&ved=2ahUKEwj3jraUsoGGAxUvSGwGHUbfAEwQv5AHegQIABAE"

    # Site first indexed by Google
# 2 years ago
#45,552

    #Source
# www.remini.com was first indexed by Google in July 2021
    proxies = {
        "http": "socks5://127.0.0.1:1080",
        "https": "socks5://127.0.0.1:1080",
    }
    lag = random.uniform(5, 10)
    headers=random.choice(headers_list)
    try:
        r = requests.get(url, headers=headers,proxies=proxies)
        print('status code')
        if r.status_code==200:
            print('okk')
        else:
            print('3333333333')
        html_doc=r.content
        print('==============get index date',r.content)
        # Parse the HTML document with BeautifulSoup
        soup = BeautifulSoup(html_doc, 'html.parser')

        # Find all elements that contain the text 'aaa'
        elements_with_aaa = soup.find_all(lambda tag: 'Site first indexed by Google' in tag.get_text())

        # # Output the text content of each element that contains 'aaa'
        for element in elements_with_aaa:
            print(element.get_text().strip())
        if len(elements_with_aaa)>0:
            r=elements_with_aaa[0].get_text()
            return r.replace('Site first indexed by Google','').strip()
        else:
            return ''
    except:
        print('failed to get index date')

        return ''

def get_domain_first_index_date(domain):

    url ="https://www.google.com/search/about-this-result?ri=CgwSCgoGY29mZmVlEAMSAggBGgIIASIAKgAyBggDEgJ1czoAQgQIARAASgBaAGoAcgA&url=https%3A%2F%2Fwww.remini.com&hl=en_US&gl=US"
    url ="https://www.google.com/search/about-this-result?url=https%3A%2F%2Fwww.remini.com&hl=en_US&gl=US"

    #Source
# www.remini.com was first indexed by Google in July 2021
    proxies = {
        "http": "socks5://127.0.0.1:1080",
        "https": "socks5://127.0.0.1:1080",
    }
    lag = random.uniform(5, 10)
    headers=random.choice(headers_list)
    try:
        r = requests.get(url, headers=headers,proxies=proxies)
        print('status code')
        if r.status_code==200:
            print('okk')
        else:
            print('3333333333')
        html_doc=r.content
        print('==============get index date',r.content)
        # Parse the HTML document with BeautifulSoup
        soup = BeautifulSoup(html_doc, 'html.parser')

        # Find all elements that contain the text 'aaa'
        elements_with_aaa = soup.find_all(lambda tag: 'was first indexed by Google' in tag.get_text())

        # # Output the text content of each element that contains 'aaa'
        for element in elements_with_aaa:
            print(element.get_text().strip())
        if len(elements_with_aaa)>0:
            r=elements_with_aaa[0].get_text()
            if 'in' in r:
                r=r.split('in')[-1]
                if 'More' in r:
                    print('rrrr',r)
                    r=r.split('More')[0]
                    return r
        else:
            return ''
    except:
        print('failed to get index date')

        return ''
def get_domain_date_rdap(value):
    headers = random.choice(headers_list)

    url = "https://rdap.verisign.com/com/v1/domain/" + value
    try:
        r = requests.get(url, headers=headers)
        #
        # Parse the JSON data
        # print(r.json())
        data = r.json()
        # print(data)

        # Locate the specific eventDate
        for event in data.get("events", []):
            #     print("=====", event)
            if event.get("eventAction") == "registration":
                # print("Found the event:", event)
                creation_date_str = event.get("eventDate")
                print(creation_date_str)
                return creation_date_str
    except:
        max_retries = 5  # Set a maximum number of retries
        for i in range(max_retries):
            try:
                proxy = get_proxy().get("proxy")
                proxies = {"http": "http://{}".format(proxy)}

                r = requests.get(url, headers=headers, proxies=proxies)

                # Parse the JSON data
                data = r.json()
                # Locate the specific eventDate
                for event in data.get("events", []):
                    if event.get("eventAction") == "registration":
                        print("Found the event:", event)
                        creation_date_str = event.get("eventDate")
                        return creation_date_str
                with open("valid-proxies.txt", "a+", encoding="utf8") as f:
                    proxy = get_proxy().get("proxy")
                    f.write(proxy + "\r\n")

            except Exception as e:
                print(f"Attempt {i+1} failed with error: {e} {proxy}")
                if i < max_retries - 1:
                    print("Retrying...")
                else:
                    print("Max retries reached. Exiting without a successful response.")
                return None


def get_domain_date_whois(value):
    # Split the value by '.'
    import whois, json

    try:
        domain_info = whois.whois(value)
        if (type(domain_info.creation_date)) == list:
            creation_date = domain_info.creation_date[0]
        else:
            # print(domain_info.creation_date)
            creation_date = domain_info.creation_date
            if creation_date == "" or creation_date is None:
                creation_date_str = ""
                return creation_date_str if creation_date_str else None

            else:
                creation_date_str = creation_date.strftime("%Y-%m-%d")
                return creation_date_str if creation_date_str else None
    except:
        return None


def get_proxy():
    return requests.get("http://demo.spiderpy.cn/get/").json()


def delete_proxy(proxy):
    requests.get("http://demo.spiderpy.cn/delete/?proxy={}".format(proxy))


import socket, json


def whois_server_list():
    # The provided text content
    # text_content = """
    #     ;WHOIS Servers List
    #     ;Maintained by Nir Sofer
    #     ;This servers list if freely available for any use and without any restriction.
    #     ;For more information: http://www.nirsoft.net/whois_servers_list.html
    #     ;Last updated on 16/02/2016
    #     ac whois.nic.ac
    #     ad whois.ripe.net
    #     ae whois.aeda.net.ae
    #     aero whois.aero
    #     af whois.nic.af
    #     ag whois.nic.ag
    #     ai whois.ai
    #     """
    # The provided text content

    response = requests.get("http://www.nirsoft.net/whois-servers.txt")
    text_content = None
    # Check if the request was successful
    if response.status_code == 200:
        # Get the text content from the response
        text_content = response.text
        # print(text_content)
    else:
        print(f"Failed to retrieve content, status code: {response.status_code}")
        print("load from local file")
        with open("nirsoft.net_whois-servers.txt", "r", encoding="utf8") as f:
            text_content = f.read()

    # Initialize an empty dictionary
    whois_servers = {}

    # Split the text content into lines and iterate over each line
    for line in text_content.strip().split("\n"):
        # Strip whitespace and ignore comments and empty lines
        line = line.strip()
        if line and not line.startswith(";"):
            # Split the line into domain and server
            domain, server = line.split()
            # Add the domain and server to the dictionary
            whois_servers[domain] = server

        # Print the resulting dictionary
    # print(whois_servers)
    # json_data = json.dumps(whois_servers, ensure_ascii=False, indent=4)
    if "network" in whois_servers == False:
        whois_servers["network"] = "whois.nic.network"
    return whois_servers


whoisservers = whois_server_list()


def whois_request(domain: str, server: str, port=43, timeout=5) -> str:
    """
    发送http请求，获取信息
    :param domain:
    :param server:
    :param port:
    :return:
    """
    # 创建连接
    sock = socket.create_connection((server, port))
    sock.settimeout(timeout)

    # 发送请求
    sock.send(("%s\r\n" % domain).encode("utf-8"))

    # 接收数据
    buff = bytes()
    while True:
        data = sock.recv(1024)
        if len(data) == 0:
            break
        buff += data

    # 关闭链接
    sock.close()
    buffdata = buff.decode("utf-8")
    # print(buffdata)
    if buffdata:
        results = {}

        # Split the text content into lines and iterate over each line
        for line in buffdata.strip().split("\n"):
            # Strip whitespace and ignore comments and empty lines
            line = line.strip()
            if ":" in line:
                # Split the line into domain and server
                # print(line.split(": "))
                res = line.split(": ")
                if len(res) == 2:
                    key = res[0]
                    value = res[-1]
                    # Add the domain and server to the dictionary
                    results[key] = value
        print("try to get date", results)
        if "Registration Time" in results:
            return results["Registration Time"]
        elif "Creation Date" in results:

            return results["Creation Date"]

        else:
            return None


def get_domain_date_whodap(value):

    tld = value.split(".")[-1]
    domain = value.replace("." + tld, "")
    import httpx
    import random

    proxy = random.choice(proxies)
    #     proxies = {"http": "http://{}".format(proxy)}
    #     print(proxy)
    #     httpx_client = httpx.Client(proxies=httpx.Proxy("http://{}".format(proxy)))
    try:

        response = whodap.lookup_domain(
            domain=domain,
            tld=tld,
            #     httpx_client=httpx_client
        )

        # Retrieving the registration date from above:
        # print(response.events[1].eventDate)
        #     """
        #         2008-08-18 13:19:55
        #         """
        return response.events[1].eventDate if len(response.events) > 1 else None

    except:
        max_retries = 5  # Set a maximum number of retries
        for i in range(max_retries):
            try:
                proxy = get_proxy().get("proxy")

                # Create an HTTPX client with a random proxy
                httpx_client = httpx.Client(
                    proxies=httpx.Proxy("http://{}".format(proxy))
                )
                # Perform the domain lookup
                response = whodap.lookup_domain(
                    domain=domain, tld=tld, httpx_client=httpx_client
                )
                with open("valid-proxies.txt", "a+", encoding="utf8") as f:
                    proxy = get_proxy().get("proxy")
                    f.write(proxy + "\r\n")
                return (
                    response.events[1].eventDate if len(response.events) > 1 else None
                )

            except Exception as e:
                print(f"Attempt {i+1} failed with error: {e} {proxy}")
                if i < max_retries - 1:
                    print("Retrying...")
                else:
                    print("Max retries reached. Exiting without a successful response.")
                    return None


def whois21_check(domain):

    import whois21

    whois = whois21.WHOIS(domain)

    print(f"Creation date   : {whois.creation_date}")
    # print(f"Expiration date : {whois.expires_date}")
    # print(f"Updated date    : {whois.updated_date}")
    return whois.creation_date


# 检查并转换字段的函数
def convert_to_string(value):
    if value == "" or value is None:
        print("===", type(value), value)
    if isinstance(value, datetime):
        print("value datetime", value)

        # 如果是datetime对象，格式化为字符串
        value= value.strftime("%Y-%m-%dT%H:%M:%SZ")
    # if pd.notnull(value):
    if not value or value.isspace() or value == "None":
        # value 是 None 或者是一个空字符串或只包含空白字符
        # print("The value is None, empty, or contains only whitespace.")
        return ""
    else:
        # value 是一个非空字符串
        print("The value is a non-empty string.", value)
        # 检查value是否不是None
        if isinstance(value, list):  # 如果是列表，处理列表中的每个元素
            print("value list", value)
            # 尝试将列表中的每个datetime对象转换为字符串
            # 返回第一个非None的字符串表示，或者一个空字符串
            return next(
                (
                    item.strftime("%Y-%m-%dT%H:%M:%SZ")
                    for item in value
                    if isinstance(item, datetime)
                ),
                "",
            )
        elif isinstance(value, str):
            print("value str", value)
            try:
                if "+" in value:

                    # 尝试将字符串解析为datetime对象，包含时区偏移
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S%z")
                    # 如果成功，格式化为字符串，不包含时区偏移
                    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                elif "/" in value:
                    dt = datetime.strptime(value, "%Y/%m/%dT%H:%M:%SZ")
                    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                elif ":" in value:

                    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
                    # 如果成功，格式化为字符串
                    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                else:
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    # 如果成功，格式化为字符串
                    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    # 尝试将字符串解析为datetime对象
            except ValueError:
                # 如果解析失败，保持原始字符串

                if "[" in value:
                    print("[] in value", value)
                    # 移除字符串两端的方括号
                    value = value.strip("[]").strip()

                    # 将清理后的字符串转换为datetime对象
                    if ", datetime" in value:
                        value = value.split(", datetime.datetime(")
                        print("split", value)
                        value = value[0]
                        print("split first", value)
                    else:
                        print("no found serveal", value)

                    # 这里的日期格式 "%Y, %m, %d, %H, %M" 与原始字符串中的格式相对应
                    # 假设 value 是一个字符串，它可能包含以下格式之一：
                    # "datetime.datetime(2021, 5, 31, 9, 35, 20)"
                    # "datetime.datetime(2021, 5, 31, 9, 35)"
                    # "datetime.datetime(2021, 5, 31)"

                    # 移除字符串 "datetime.datetime(" 和结尾的 ")"
                    cleaned_value = value[
                        value.find("(") + 1 : value.rfind(")")
                    ]  # 使用 find 和 rfind 来去除括号

                    # 检测 cleaned_value 中包含哪些格式化代码
                    if "," in cleaned_value:  # 检查是否有逗号分隔的多个组件
                        format_str = "%Y, %m, %d, %H, %M, %S"
                    else:  # 假设只有一个组件，如 "20210531"，可能是日期或日期时间
                        format_str = "%Y%m%d"  # 这可以根据实际的字符串格式进行调整

                    # 尝试解析字符串
                    try:
                        dt = datetime.strptime(cleaned_value, format_str)
                        print(dt)

                        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                    except ValueError as e:
                        print("Parsing error:", e)

                    # dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S%z")

                else:
                    print("value parse", value)

                return value
        elif isinstance(value, datetime):
            print("value datetime", value)

            # 如果是datetime对象，格式化为字符串
            return value.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            print("value else", value)

            # 其他类型，使用str()转换为字符串
            return str(value)


def query_whois_request_server(domain: str, server: str, port=43, timeout=5) -> str:
    """
    发送http请求，获取信息
    :param domain:
    :param server:
    :param port:
    :return:
    """
    # 创建连接
    sock = socket.create_connection((server, port))
    sock.settimeout(timeout)

    # 发送请求
    sock.send(("%s\r\n" % domain).encode("utf-8"))

    # 接收数据
    buff = bytes()
    while True:
        data = sock.recv(1024)
        if len(data) == 0:
            break
        buff += data

    # 关闭链接
    sock.close()

    buffdata = buff.decode("utf-8")
    # print(buffdata)
    if buffdata:
        results = {}

        # Split the text content into lines and iterate over each line
        for line in buffdata.strip().split("\n"):
            # Strip whitespace and ignore comments and empty lines
            line = line.strip()
            if ":" in line:
                # Split the line into domain and server
                # print(line.split(": "))
                # print(line.split(": "))
                res = line.split(": ")
                if len(res) == 2:
                    key = res[0]
                    value = res[-1]
                    # Add the domain and server to the dictionary
                    results[key] = value  # Add the domain and server to the dictionary
        if "whois" in results:
            return results["whois"]

        else:
            return None


# 定义一个函数来确定是否两个或更多的字段是相同的，并设置create_date
def check_and_assign_create_date(row):
    # 提取字段值，并处理None值，将None转换为对应格式的空字符串
    rdap_value = row["rdap"] if pd.notnull(row["rdap"]) else ""
    whodap_value = row["whodap"] if pd.notnull(row["whodap"]) else ""
    whois_value = row["whois"] if pd.notnull(row["whois"]) else ""

    # 比较字段值，确定是否有两个或更多字段相同
    if (
        (rdap_value == whodap_value)
        or (rdap_value == whois_value)
        or (whodap_value == whois_value)
    ):
        # 如果有相同的值，选择一个非空的字段作为create_date
        create_date = (
            rdap_value
            if rdap_value
            else (whodap_value if whodap_value else whois_value)
        )
        return create_date
    else:
        # 如果没有两个字段相同，create_date为None
        if whois_value:
            create_date = whois_value
            return create_date
        else:

            return None


# 定义一个函数来收集create_date为None的destination列表
def collect_destinations_with_none_create_date(df):
    destinations = df[df["create_date"].isnull()]["Website"]
    return list(destinations)


# 定义一个函数来分析域名后缀的频率
def analyze_domain_suffix_frequency(destinations):
    # 提取域名后缀
    suffixes = [destination.split(".")[-1] for destination in destinations]
    # 计算频率
    frequency = Counter(suffixes)
    return frequency


# 定义一个函数来随机选择一个具有特定后缀的域名
def random_choose_domain_by_suffix(destinations, suffix):
    # 筛选出所有以特定后缀结尾的域名
    domains_with_suffix = [domain for domain in destinations if domain.endswith(suffix)]
    # 如果有域名匹配，随机选择一个
    if domains_with_suffix:
        return random.choice(domains_with_suffix)
    else:
        # 如果没有域名匹配，返回None
        return None


# This function will be executed concurrently for each row.
def process_row(row, index, db_path):
    # print("----", type(row), row, row["Website"], row["category"])
    domain = row["Website"]
    domain = domain.replace("https://", "")
    # print(
    #     "===========1",
    #     index,
    #     row["Ranking"],
    #     row["Tools"],
    #     row["Website"],
    #     row["Snapshot"],
    #     row["Payment Platform"],
    #     row["Monthly visits"],
    #     row["Desc"],
    # )
    # print("========\r")
    data = {
        "id": index,
        "Ranking": row["Ranking"],
        "Tools": row["Tools"],
        "Website": row["Website"],
        "Snapshot": row["Snapshot"],
        "PaymentPlatform": row["Payment Platform"],
        "Monthlyvisits": row["Monthly visits"],
        "Desc": row["Desc"],
        # "rdap": str(row["rdap"]),
        # "whois": str(row["whois"]),
        # "whodap": str(row["whodap"]),
        "status": "0",
    }
    print("======", data, "\r")
    print("!!!!!!!!!!!rdap", index, domain)
    if "rdap" not in row or data["rdap"] is None or data["rdap"] == str(float("nan")):
        print("whoola", domain)
        data["rdap"] = get_domain_date_rdap(domain)
    else:
        print("rdap here", data["rdap"])

    print("!!!!!!!!!!!whois", index, data["Website"])

    if (
        "whois" not in row
        or data["whois"] is None
        or data["whois"] == str(float("nan"))
    ):
        domainsuffix = domain.split(".")[-1]
        server = whoisservers[domainsuffix]

        data["whois"] = whois_request(domain, server)
        print(f"{data['whois']}==========\n")

        if data["whois"] == None:
            print("try whois21")
            data["whois"] = whois21_check(domain)
        print(f"whois =={data['whois']}==========\n")

        print("==========\n")
    else:
        print("whois here", data["whois"])

    print("!!!!!!!!!!!whodap", index, data["Website"])

    if (
        "whodap" not in row
        or data["whodap"] is None
        or data["whodap"] == str(float("nan"))
    ):
        data["whodap"] = get_domain_date_whodap(domain)
    else:
        print("whodap here", data["whodap"])

    print('try to get index_date')
    if 'index_date' not in row or data['index_date'] is None or data['index_date'] ==str(float('nan')):
        print('call index date function')
        data['index_date']=get_domain_first_index_date(domain)
    else:
        print('')

    data["status"] = "1"
    # Insert the data into the SQLite database

    # 应用转换函数到data字典的每个字段
    data["rdap"] = convert_to_string(data.get("rdap", ""))
    data["whois"] = convert_to_string(data.get("whois", ""))
    data["whodap"] = convert_to_string(data.get("whodap", ""))

    print("==========", data)

    with sqlite3.connect(db_path) as conn:
        # Use a context manager to ensure the connection is closed properly
        cursor = conn.cursor()  # Create a cursor object using the connection
        try:
            # print("before add", data)

            cursor.execute(
                """
                    INSERT INTO destinations (id,Ranking, Tools, Website, Snapshot, PaymentPlatform, Monthlyvisits, Desc, rdap, whois, whodap,status,index_date)
                    VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)
                    """,
                (
                    data["id"],
                    data["Ranking"],
                    data["Tools"],
                    data["Website"],
                    data["Snapshot"],
                    data["PaymentPlatform"],
                    data["Monthlyvisits"],
                    data["Desc"],
                    data["rdap"],
                    data["whois"],
                    data["whodap"],
                    data["status"],
                    data['index_date']
                ),
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            conn.rollback()  # Rollback any changes if an error occurs
        finally:
            cursor.close()  # Close the cursor when done

    return data  # Optionally return something if needed


# Note: Be cautious with the number of workers you use, especially with IO-bound tasks like network requests.
# Too many workers can lead to issues such as running out of file descriptors or overwhelming the server with requests.
def startDB():
    df = pd.DataFrame()
    conn = None
    if os.path.exists(filename + "-output.db"):
        # Connect to the SQLite database
        conn = sqlite3.connect(filename + "-output.db")

        # Read the data from the 'destinations' table into a pandas DataFrame
        df = pd.read_sql_query("SELECT * FROM destinations", conn)
    else:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(
            filename + ".csv", encoding="utf-8"
        )  # Replace 'data.csv' with your CSV file name
        df.columns = df.columns.str.strip()

        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Connect to an SQLite database
        conn = sqlite3.connect(filename + "-output.db")
        # Create a table to store the results if it doesn't exist
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS destinations (
                id INTEGER PRIMARY KEY,
                Ranking TEXT,
                Tools TEXT,
                Website TEXT,
                Snapshot TEXT,
                PaymentPlatform TEXT,
                Monthlyvisits TEXT,
                Desc TEXT,
                rdap TEXT,
                whodap TEXT,
                whois TEXT,
                status TEXT,
                index_date TEXT

            )
            """
            )
            conn.commit()

        conn = sqlite3.connect(filename + "-output.db")

        # Read the data from the 'destinations' table into a pandas DataFrame
        # df = pd.read_sql_query("SELECT * FROM destinations", conn)
    return df, conn


df, conn = startDB()
# Use ThreadPoolExecutor or ProcessPoolExecutor for concurrency
# Note: ThreadPoolExecutor is generally used for IO-bound tasks, while ProcessPoolExecutor is for CPU-bound tasks.
with ThreadPoolExecutor(
    max_workers=50
) as executor:  # You can adjust the number of workers
    future_to_domain = {
        executor.submit(
            process_row, row, index, db_path=output_folder + "/" + filename + ".db"
        ): row["Website"]
        for index, row in df.iterrows()
    }

    for future in as_completed(future_to_domain):
        domain = future_to_domain[future]
        try:
            # Get the result of the execution, if you have returned something from process_row
            result = future.result()
        except Exception as exc:
            print(f"Generated an exception: {exc} for domain {domain}")
        else:
            # Process the result if needed
            print(f"Successfully processed: {domain}")


# Read the data from the 'destinations' table into a pandas DataFrame
df = pd.read_sql_query("SELECT * FROM destinations", conn)


# Write the DataFrame to a CSV file
df.to_csv(
    output_folder + "/" + filename + "-results.csv", index=False, encoding="utf-8"
)

df["whois"] = df["whois"].apply(convert_to_string)

df["whodap"] = df["whodap"].apply(convert_to_string)
df["rdap"] = df["rdap"].apply(convert_to_string)

# 应用函数到每一行，并创建新列create_date
df["create_date"] = df.apply(check_and_assign_create_date, axis=1)
# Write the DataFrame to a CSV file
df.to_csv(
    output_folder + "/" + filename + "-results-1.csv", index=False, encoding="utf-8"
)
# 应用第一个函数，收集destination列表
destinations_with_none_create_date = collect_destinations_with_none_create_date(df)

# 应用第二个函数，分析域名后缀频率
domain_suffix_frequency = analyze_domain_suffix_frequency(
    destinations_with_none_create_date
)

print("Destinations with None create_date: ", destinations_with_none_create_date)
print("Domain suffix frequency: ", domain_suffix_frequency)

# Close the database connection
conn.close()
