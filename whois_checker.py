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

filename = os.getenv("URL")
folder_path = "./result"

if not os.path.exists(folder_path):
    os.mkdir(folder_path)
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
        max_retries = 10  # Set a maximum number of retries
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

    if buffdata:
        results = {}

        # Split the text content into lines and iterate over each line
        for line in buffdata.strip().split("\n"):
            # Strip whitespace and ignore comments and empty lines
            line = line.strip()
            if ":" in line:
                # Split the line into domain and server
                # print(line.split(": "))
                key, value = line.split(": ")
                # Add the domain and server to the dictionary
                results[key] = value
        if "Registration Time" in results:
            return results["Registration Time"]
        elif "Creation Date" in results:

            return results["Creation Date"]

        else:
            return None


def get_domain_date_whodap(value):

    tld = value.split(".")[-1]
    domain = value.replace("." + tld, "")
    # Looking up a domain name
    #     print("tlld", tld, domain)
    import asyncio

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
        max_retries = 10  # Set a maximum number of retries
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


def zip_folder(
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


def whois21_check(domain):

    import whois21

    whois = whois21.WHOIS(domain)

    print(f"Creation date   : {whois.creation_date}")
    # print(f"Expiration date : {whois.expires_date}")
    # print(f"Updated date    : {whois.updated_date}")
    return whois.creation_date


# This function will be executed concurrently for each row.
# This function will be executed concurrently for each row.
def process_row(row, index, db_path):
    # print("----", type(row), row, row["destination"], row["category"])
    domain = row["destination"]
    # print("===========1", row["category"])
    data = {
        "id": index,
        "destination": domain,
        "category": row["category"],
        "traffic_share": row["traffic_share"],
        "visits": row["visits"],
        "changes": row["changes"],
        "channel": row["channel"],
        "type": row["type"],
        "rdap": str(row["rdap"]),
        "whois": str(row["whois"]),
        "whodap": str(row["whodap"]),
        "status": "0",
    }

    if "rdap" not in row or data["rdap"] is None or data["rdap"] == str(float("nan")):
        data["rdap"] = get_domain_date_rdap(domain)

    if (
        "whois" not in row
        or data["whois"] is None
        or data["whois"] == str(float("nan"))
    ):
        domainsuffix = domain.split(".")[-1]
        server = whoisservers[domainsuffix]
        print("----", domain, domainsuffix, server)

        data["whois"] = whois_request(domain, server)
        if data["whois"] == None:
            data["whois"] = whois21_check(domain)
        print("==========\n")

    if (
        "whodap" not in row
        or data["whodap"] is None
        or data["whodap"] == str(float("nan"))
    ):
        data["whodap"] = get_domain_date_whodap(domain)

    data["status"] = "1"
    # Insert the data into the SQLite database
    print("==========", data)

    with sqlite3.connect(db_path) as conn:
        # Use a context manager to ensure the connection is closed properly
        cursor = conn.cursor()  # Create a cursor object using the connection
        try:
            # print("before add", data)

            cursor.execute(
                """
                    INSERT INTO destinations (id,destination, category, traffic_share, visits, changes, channel, type, rdap, whois, whodap,status)
                    VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
                    """,
                (
                    data["id"],
                    data["destination"],
                    data["category"],
                    data["traffic_share"],
                    data["visits"],
                    data["changes"],
                    data["channel"],
                    data["type"],
                    data["rdap"],
                    data["whois"],
                    data["whodap"],
                    data["status"],
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
    if os.path.exists("output/output.db"):
        # Connect to the SQLite database
        conn = sqlite3.connect("output/output.db")

        # Read the data from the 'destinations' table into a pandas DataFrame
        df = pd.read_sql_query("SELECT * FROM destinations", conn)
    else:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(
            filename + ".csv", encoding="utf-8"
        )  # Replace 'data.csv' with your CSV file name

        # Connect to an SQLite database
        conn = sqlite3.connect("output/output.db")
        # Create a table to store the results if it doesn't exist
        with closing(conn.cursor()) as cursor:
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS destinations (
                id INTEGER PRIMARY KEY,
                destination TEXT,
                category TEXT,
                traffic_share TEXT,
                visits TEXT,
                changes TEXT,
                channel TEXT,
                type TEXT,
                rdap TEXT,
                whodap TEXT,
                whois TEXT,
                status TEXT

            )
            """
            )
            conn.commit()

        conn = sqlite3.connect("output/output.db")

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
        executor.submit(process_row, row, index, db_path="output/output.db"): row[
            "destination"
        ]
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
df.to_csv("output/results.csv", index=False, encoding="utf-8")

# Close the database connection
conn.close()

# # Write the DataFrame to a CSV file

max_size_mb = 1500

# Create a temporary ZIP file for the first archive
zip_count = 1
zip_temp_file = os.path.join(output_folder, f"temp{zip_count}.zip")
zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)

# Compress the folder into multiple ZIP archives
zip_folder(folder_path, output_folder, max_size_mb, zip_file, zip_temp_file, zip_count)