import pandas as pd
import requests
import time
import json, random, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import shutil
import zipfile

filename = os.getenv("URL")
# filename = "100"

output_folder = "./output"
if not os.path.exists("output"):
    os.mkdir("output")

# 读取Excel文件A和CSV文件B
# excel_a = pd.read_excel("to10k.xlsx")
csv_b = pd.read_csv(filename + ".csv")
# 创建一个新的列"store url"，并初始化为None

url = "https://mercury.revseller.com/api/us/data5/"

# 定义请求头部

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


def get_proxy():
    return requests.get("http://demo.spiderpy.cn/get/").json()


def delete_proxy(proxy):
    requests.get("http://demo.spiderpy.cn/delete/?proxy={}".format(proxy))


def process_row(row):

    # 遍历Excel A的每一行，检查"卖家记号"是否在CSV B中的"marketplaceid"或"me"中
    #     for index, row in excel_a.iterrows():
    #     time.sleep(lag)

    # 定义代理设置
    proxies = {
        "http": "socks5://127.0.0.1:1080",
        "https": "socks5://127.0.0.1:1080",
    }
    lag = random.uniform(5, 10)

    sellerid = row["me"].strip()
    if os.path.exists(output_folder + sellerid + ".json") == False:

        # 指定URL
        surl = url + sellerid + "?domain=1&seller=" + sellerid

        # 发送GET请求
        response = requests.get(
            surl,
            headers=headers,
            # , proxies=proxies
        )

        # 检查响应状态码
        if response.status_code == 200:
            # 请求成功，解析JSON数据
            data = response.json()

            # 打印或处理数据
            #     print(data)
            with open(output_folder + sellerid + ".json", "w", encoding="utf8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

        else:

            max_retries = 5  # Set a maximum number of retries
            for i in range(max_retries):
                try:
                    proxy = get_proxy().get("proxy")
                    proxies = {"http": "http://{}".format(proxy)}

                    response = requests.get(surl, headers=headers, proxies=proxies)

                    # 检查响应状态码
                    if response.status_code == 200:
                        # 请求成功，解析JSON数据
                        data = response.json()

                        # 打印或处理数据
                        print(data)
                        with open(
                            output_folder + sellerid + ".json", "w", encoding="utf8"
                        ) as f:
                            json.dump(data, f, ensure_ascii=False, indent=4)

                except Exception as e:
                    print(f"Attempt {i+1} failed with error: {e} {proxy}")
                    if i < max_retries - 1:
                        print("Retrying...")
                    else:
                        print(
                            "Max retries reached. Exiting without a successful response."
                        )


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


folder_path = "./result"

if not os.path.exists(folder_path):
    os.mkdir(folder_path)
with ThreadPoolExecutor(
    max_workers=100
) as executor:  # You can adjust the number of workers
    future_to_domain = {
        executor.submit(process_row, row): row["me"] for index, row in csv_b.iterrows()
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
    # Specify the maximum size of each RAR file in MB
    max_size_mb = 1500

    # Create a temporary ZIP file for the first archive
    zip_count = 1
    zip_temp_file = os.path.join(folder_path, f"temp{zip_count}.zip")
    zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)

    # Compress the folder into multiple ZIP archives
    zip_folder(
        output_folder, folder_path, max_size_mb, zip_file, zip_temp_file, zip_count
    )
