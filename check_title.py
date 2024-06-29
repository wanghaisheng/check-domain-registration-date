import pandas as pd
import requests
import time
import json, random, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import shutil
import zipfile
from bs4 import BeautifulSoup
from DataRecorder import Recorder
from datetime import datetime
import asyncio
from loguru import logger
from domaintitle import process_domains_title



# 创建一个新的列"store url"，并初始化为None

url = "https://www.google.com/search/about-this-result?origin=www.google.com&ons=2586&ri=CgwSCgoGcmVtaW5pEAMKCRIHCgNjb20QAxICCAEaACIAKgAyBggCEgJzZzoAQgQIARAASgBaDggBEgpyZW1pbmkuY29tcgB6AA&fd=GgIIAw&dis=EAE&url=https%3A%2F%2Fwww.remini.com%2F&sa=1&hl=en-SG&gl=SG&ilrm=zpr&vet=10CA8Qt5oMahcKEwjArc67poGGAxUAAAAAHQAAAAAQBA.iCM9ZqVaiN2x4w-i6YnYBQ.i&ved=0CA8Qt5oMahcKEwjArc67poGGAxUAAAAAHQAAAAAQBA&uact=1"
# 定义请求头部
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


def get_proxy():
    return requests.get("http://demo.spiderpy.cn/get/").json()


def delete_proxy(proxy):
    requests.get("http://demo.spiderpy.cn/delete/?proxy={}".format(proxy))


# 加载有效代理列表
def load_valid_proxies(config_file):
    try:
        with open(config_file, "r") as f:
            valid_proxies = json.load(f)
            return valid_proxies
    except (IOError, ValueError) as e:
        print(f"Error loading proxies: {e}")
        return []


def load_valid_proxies_txt(filename):
    with open(filename, "r") as file:
        p = file.read().splitlines()
        p = [line for line in p if line.strip()]
        return p


import os


# 保存有效代理列表，只追加新的变化到现有的列表中
def save_valid_proxies(config_file, valid_proxies):
    # 将代理列表转换为去重的集合
    unique_proxies = set(valid_proxies)

    if not os.path.exists(config_file):
        # 如果文件不存在，创建一个新文件并保存代理列表
        with open(config_file, "w") as f:
            for proxy in unique_proxies:
                f.write(proxy + "\n")  # 每个代理后面添加换行符
            print(f"Proxy list file created and saved as {config_file}")
    else:
        try:
            # 尝试读取已存在的代理列表
            with open(config_file, "r") as f:
                existing_proxies = set(f.read().splitlines())

            # 获取新代理列表中未在现有代理列表中的代理
            new_proxies = unique_proxies - existing_proxies

            # 如果有新的代理，追加它们到文件中
            if new_proxies:
                with open(config_file, "a") as f:
                    for proxy in new_proxies:
                        f.write(proxy + "\n")  # 每个新代理后面添加换行符
                print(f"New proxies appended to {config_file}")
            else:
                print("No new proxies to append.")
        except (IOError, ValueError) as e:
            print(f"Error loading or saving proxies: {e}")


def zip_folder(
    folder_path, output_folder, max_size_mb, zip_file, zip_temp_file, zip_count
):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Convert the maximum size from MB to bytes
    max_size_bytes = max_size_mb * 1024 * 1024

    # Initialize the size of the current zip file
    current_zip_size = 0

    # Iterate over the directory tree
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            # Check if adding the next file would exceed the maximum size
            if current_zip_size + os.path.getsize(file_path) > max_size_bytes:
                # Close the current ZIP archive
                zip_file.close()

                # Move the current ZIP file to the output folder
                final_zip_path = os.path.join(output_folder, f"archive{zip_count}.zip")
                shutil.move(zip_temp_file, final_zip_path)

                print(
                    f"Created '{final_zip_path}' (size: {os.path.getsize(final_zip_path)} bytes)"
                )

                # Reset the current zip size and create a new ZIP archive
                current_zip_size = 0
                zip_count += 1
                zip_temp_file = os.path.join(output_folder, f"temp{zip_count}.zip")
                zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)

            # Add each file to the current ZIP archive
            zip_file.write(file_path)
            # Update the size of the current zip file
            current_zip_size += os.path.getsize(file_path)

    # Close the last ZIP archive after all files have been added
    zip_file.close()

    # Move the last ZIP file to the output folder
    final_zip_path = os.path.join(output_folder, f"archive{zip_count}.zip")
    shutil.move(zip_temp_file, final_zip_path)

    print(f"Created '{final_zip_path}' (size: {os.path.getsize(final_zip_path)} bytes)")




def test_proxy(proxy_url, test_url):
    try:
        response = requests.get(
            test_url,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=10,
            # , verify=False  # Commented out the verify=False as it's not recommended to disable SSL verification
        )
        return proxy_url if response.status_code == 200 else None
    except requests.exceptions.RequestException:
        return None


def clean_proxies(valid_proxies, test_url):
    with ThreadPoolExecutor(max_workers=100) as executor:
        # Using a list to collect the results of the futures
        results = list(
            executor.map(lambda proxy: test_proxy(proxy, test_url), valid_proxies)
        )

    # Filter out the None values from the results to get only the working proxies
    updated_valid_proxies = [proxy for proxy in results if proxy is not None]
    return updated_valid_proxies


valid_proxies = []  # Fill this list with your proxy URLs

# File to store valid proxies
workingproxies = []


if os.path.exists("valid_proxies.txt"):
    valid_proxies = open("valid_proxies.txt", "r", encoding="utf8").readlines()
print(valid_proxies)
if valid_proxies:
    valid_proxies = list(set(valid_proxies))
    valid_proxies = [
        line.strip().replace("\n", "") for line in valid_proxies if "\n" in line
    ]

    # Perform test for each proxy
    print("start to clean up proxies")
    test_url = "https://mercury.revseller.com"

    # Call the function to clean up the proxies
    # cleaned_proxies = clean_proxies(valid_proxies, test_url)  # Replace with your test URL

    test_url = "https://google.com"

    # Call the function to clean up the proxies
    cleaned_proxies = clean_proxies(
        valid_proxies, test_url
    )  # Replace with your test URL

    print("Cleaned Proxies:", cleaned_proxies)
folder_path = "./result"

if os.path.exists(folder_path) == False:
    os.mkdir(folder_path)

output_folder = "./output"

# Check if the directory exists
if not os.path.exists(output_folder):
    # If the directory does not exist, create it
    os.mkdir(output_folder)
    print("Directory 'output' was created.")
else:
    # If the directory exists, do nothing (pass)
    print("Directory 'output' already exists.")
filename = os.getenv("filename")
colname = os.getenv("colname")


#  filename = "100"
counts = os.getenv("counts")
if filename and filename.strip():
    if colname and colname.strip():


        start=datetime.now()
        inputfilepath=filename + ".csv"
        # logger.add(f"{folder_path}/domain-index-title.log")
        # print(domains)
        outfilepath=inputfilepath.replace('.csv','-titles.csv')
        outfile = Recorder(folder_path+'/'+outfilepath, cache_size=50)
        asyncio.run(process_domains_title(inputfilepath,colname,outfilepath,outfile))
        end=datetime.now()
        print('costing',end-start)
        outfile.record()
else:
    print('please check input')




    # Specify the maximum size of each RAR file in MB
max_size_mb = 1500

# Create a temporary ZIP file for the first archive
zip_count = 1
zip_temp_file = os.path.join(folder_path, f"temp{zip_count}.zip")
zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)

# Compress the folder into multiple ZIP archives
zip_folder(
    folder_path, output_folder, max_size_mb, zip_file, zip_temp_file, zip_count
)
