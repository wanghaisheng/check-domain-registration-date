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


folder_path = "./result"

if not os.path.exists(folder_path):
    os.mkdir(folder_path)
output_folder = "./output"
if not os.path.exists("output"):
    os.mkdir("output")


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


# This function will be executed concurrently for each row.
def process_row(row, index, conn):

    data = row
    data["id"] = index
    domain = row["destination"]
    if "status" not in row or (row["status"] and row["status"] == "0"):
        # Check and perform RDAP, Whois, and WhoDAP lookups if necessary
        if "rdap" not in row or row["rdap"] is None:
            data["rdap"] = get_domain_date_rdap(domain)
        else:
            data["rdap"] = row["rdap"]

        if "whois" not in row or row["whois"] is None:
            data["whois"] = get_domain_date_whois(domain)
        else:
            data["whois"] = row["whois"]

        if "whodap" not in row or row["whodap"] is None:
            data["whodap"] = get_domain_date_whodap(domain)
        else:
            data["whodap"] = row["whodap"]
        data["status"] = "1"
        # Insert the data into the SQLite database
        with conn.cursor() as cursor:
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
        return domain  # Optionally return something if needed


# Note: Be cautious with the number of workers you use, especially with IO-bound tasks like network requests.
# Too many workers can lead to issues such as running out of file descriptors or overwhelming the server with requests.
def startDB():
    df = pd.DataFrame()
    conn = None
    if os.path.exists("output.db"):
        # Connect to the SQLite database
        conn = sqlite3.connect("output.db")

        # Read the data from the 'destinations' table into a pandas DataFrame
        df = pd.read_sql_query("SELECT * FROM destinations", conn)
    else:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(
            "stripe/stripe-final_combined.csv", encoding="utf-8"
        )  # Replace 'data.csv' with your CSV file name

        # Connect to an SQLite database
        conn = sqlite3.connect("output.db")
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

        conn = sqlite3.connect("output.db")

        # Read the data from the 'destinations' table into a pandas DataFrame
        # df = pd.read_sql_query("SELECT * FROM destinations", conn)
    return df, conn


df, conn = startDB()
# Use ThreadPoolExecutor or ProcessPoolExecutor for concurrency
# Note: ThreadPoolExecutor is generally used for IO-bound tasks, while ProcessPoolExecutor is for CPU-bound tasks.
with ThreadPoolExecutor(
    max_workers=20
) as executor:  # You can adjust the number of workers
    future_to_domain = {
        executor.submit(process_row, row, index, conn): row["destination"]
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
# Close the database connection
conn.close()

# Write the DataFrame to a CSV file


# Specify the folder path you want to compress

# Specify the maximum size of each RAR file in MB
max_size_mb = 1500

# Create a temporary ZIP file for the first archive
zip_count = 1
zip_temp_file = os.path.join(output_folder, f"temp{zip_count}.zip")
zip_file = zipfile.ZipFile(zip_temp_file, "w", zipfile.ZIP_DEFLATED)

# Compress the folder into multiple ZIP archives
zip_folder(folder_path, output_folder, max_size_mb, zip_file, zip_temp_file, zip_count)


#
