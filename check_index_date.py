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


filename = os.getenv("URL")
# filename = "100"

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


def get_domain_first_index_date1(row, valid_proxies, workingproxies):
    sellerid = row["sellerId"].strip()
    from urllib.parse import urlencode

    domain = "https://www.amazon.com/s?me=" + sellerid.strip()
    # domain = (
    # f"https://www.amazon.com/s?me={sellerid.strip()}&marketplaceID=ATVPDKIKX0DER"
    # )
    # domain = f"https://www.amazon.com/sp?ie=UTF8&seller={sellerid.strip()}"

    url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp&sa=X&ved=2ahUKEwj3jraUsoGGAxUvSGwGHUbfAEwQv5AHegQIABAE"

    print("url to", url)
    # Source
    # www.remini.com was first indexed by Google in July 2021

    max_retries = 5

    proxies = None
    pro_str = None
    isbanned = True
    for i in range(max_retries):
        print(f"{i}th=={domain}")
        if i == 0:
            # proxies = {
            #     "http": "socks5h://127.0.0.1:1080",
            #     "https": "socks5h://127.0.0.1:1080",
            # }
            pass
        elif workingproxies != []:

            workingproxies = [p for p in workingproxies if p is not None]
            pro_str = random.choice(workingproxies)
            if "sock" not in pro_str:

                proxy = "http://{}".format(pro_str)
                proxies = {"http": proxy, "https": proxy.replace("http", "https")}

            else:
                proxy = pro_str
                # if 'socks4' in proxy:
                #     proxy=proxy.replace('socks4','socks4h')
                # if 'socks5' in proxy:

                #     proxy=proxy.replace('socks5','socks5h')

                # proxy = f"http://{rand_proxies()}"
                proxies = {"http": proxy, "https": proxy}
            # print(f"{i}========load exist proxy ======= {proxies}")
        elif valid_proxies == []:
            pro_str = get_proxy()["proxy"]

            proxy = "http://{}".format(pro_str)

            # proxy = f"http://{rand_proxies()}"
            proxies = {"http": proxy, "https": proxy.replace("http", "https")}
        else:

            pro_str = random.choice(valid_proxies)
            proxies = {"http": pro_str, "https": pro_str}

            print(f"{i}========load exist proxy ======= {proxies}")

        print(f"use google to check {domain} store index date")
        try:
            r = requests.get(url, headers=random.choice(headers_list), proxies=proxies)
            print("status code")
            if r.status_code == 200:
                print("okk")
            else:
                print("3333333333")
            html_doc = r.content
            # print("==============get index date", r.content)
            # Parse the HTML document with BeautifulSoup
            soup = BeautifulSoup(html_doc, "html.parser")

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

                    save_valid_proxies(
                        folder_path + "/proxies_config.txt", workingproxies
                    )
                    # save_valid_proxies(
                    #     folder_path + "/store-indexdate.txt", [sellerid + "-", r[-1]]
                    # )
                    dr.add_data({"sellerid": sellerid, "date": r[-1]})  # 接收dict数据

                    # return r[-1]
                    break
                else:
                    continue

            else:
                save_valid_proxies(
                    folder_path + "/failed-store-indexdate.txt", [domain]
                )
                break

        except Exception as e:  # 捕获请求相关的异常
            print(f"Attempt failed with{pro_str} error: {e}")

            # print(f"Attempt {i+1} failed with error:{e}")
            if pro_str in workingproxies:
                workingproxies.remove(pro_str)
                save_valid_proxies("proxies_config.txt", workingproxies)

            if i < max_retries - 1:
                # time.sleep(2**i)  # 指数退避策略
                # print("Retrying...")
                continue  # 继续下一次重试
            else:
                print("Max retries reached. Exiting without a successful response.")
                save_valid_proxies("failedstore.txt", [domain])

                break  # 最大重试次数达到，退出循环


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


# Example usage:
# zip_folder('/path/to/folder', '/path/to/output', 100, None, None, 0)
def zip_folder_old(
    folder_path, output_folder, max_size_mb, zip_file, zip_temp_file, zip_count
):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Convert the maximum size from MB to bytes
    max_size_bytes = max_size_mb * 1024 * 1024

    # Iterate over the directory tree
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)

            # Add each file to the current ZIP archive
            zip_file.write(file_path)

            # Check if the current ZIP file exceeds the maximum size
            if os.stat(file_path).st_size > max_size_bytes:
                # Close the current ZIP archive
                zip_file.close()

                # Move the current ZIP file to the output folder
                shutil.move(
                    zip_temp_file,
                    os.path.join(output_folder, f"archive{zip_count}.zip"),
                )

                print(
                    f"Created 'archive{zip_count}.zip' (size: {os.path.getsize(os.path.join(output_folder, f'archive{zip_count}.zip'))} bytes)"
                )

                # Create a new ZIP archive for the remaining files
                zip_count += 1
                zip_temp_file = os.path.join(output_folder, f"temp{zip_count}.zip")
                zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)

                # Delete the original file after adding it to the ZIP archive
                os.remove(file_path)

    # Close the last ZIP archive
    zip_file.close()

    # Move the last ZIP file to the output folder
    shutil.move(zip_temp_file, os.path.join(output_folder, f"archive{zip_count}.zip"))

    print(
        f"Created 'archive{zip_count}.zip' (size: {os.path.getsize(os.path.join(output_folder, f'archive{zip_count}.zip'))} bytes)"
    )


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

# 读取Excel文件A和CSV文件B
csv_b = pd.read_csv(filename + ".csv")
csv_b = csv_b.head(1000)

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

dr = Recorder(path=output_folder + "/data.csv", cache_size=500)

with ThreadPoolExecutor(
    max_workers=200
) as executor:  # You can adjust the number of workers
    future_to_domain = {
        executor.submit(
            get_domain_first_index_date1, row, valid_proxies, valid_proxies
        ): row["sellerId"]
        for index, row in csv_b.iterrows()
    }

    for future in as_completed(future_to_domain):
        domain = future_to_domain[future]
        try:
            # Get the result of the execution, if you have returned something from process_row
            result = future.result()
            dr.record()  # 手动调用写入方法

        except Exception as exc:
            print(f"Generated an exception: {exc} for domain {domain}")
        else:
            # Process the result if needed
            print(f"Successfully processed: {domain}")
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
