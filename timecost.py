import gevent
from gevent import monkey, pool
monkey.patch_all()

import aiohttp
import asyncio
import time
import urllib3
from geventhttpclient import HTTPClient
from geventhttpclient.url import URL

monkey.patch_all()

# Constants
GOOGLE_URL = 'https://www.google.com/search?q='

# Method using aiohttp
async def query_google_aiohttp(session, search_term):
    async with session.get(GOOGLE_URL + search_term) as response:
        return await response.text()


# Method using GeventHTTPClient
def query_google_geventhttpclient(search_term):
    url = URL(GOOGLE_URL + search_term)
    http_client = HTTPClient.from_url(url)
    try:
        res=http_client.get(url.request_uri)  # Use request_uri argument
        return res.read()
    finally:
        http_client.close()
url = URL("https://www.google.com")
client = HTTPClient.from_url(url, concurrency=10)

# Method using GeventHTTPClient
def query_google_geventhttpclientwithresue(search_term):
    url = URL(f'/search?q={search_term}')
    try:
        res=client.get(url.request_uri)  # Use request_uri argument
        return res.read()
    finally:
        pass
        # Function to run tasks with concurrency using gevent pool
def run_tasks_with_gevent(concurrency, total_tasks, search_term, query_method):
    pool_size = min(concurrency, total_tasks)
    pool_obj = pool.Pool(size=pool_size)
    tasks = []

    for i in range(1, total_tasks + 1):
        task = pool_obj.spawn(query_method, search_term)
        tasks.append(task)

    gevent.joinall(tasks)

# Function to run tasks with aiohttp
async def run_tasks_with_aiohttp(concurrency, total_tasks, search_term):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for i in range(1, total_tasks + 1):
            task = asyncio.create_task(query_google_aiohttp(session, search_term))
            tasks.append(task)
            if len(tasks) >= concurrency:
                await asyncio.gather(*tasks)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks)





# Example usage: Running with 100 concurrent tasks for 1000 requests
def main():
    total_requests = 1000
    concurrency = 100
    search_term = "openai gpt-3"

    # Measure aiohttp time
    start_time = time.time()
    asyncio.run(run_tasks_with_aiohttp(concurrency, total_requests, search_term))
    aiohttp_time = time.time() - start_time

    # Measure GeventHTTPClient time
    start_time = time.time()
    run_tasks_with_gevent(concurrency, total_requests, search_term, query_google_geventhttpclient)
    geventhttpclient_time = time.time() - start_time



    # Measure GeventHTTPClient time
    start_time = time.time()
    run_tasks_with_gevent(concurrency, total_requests, search_term, query_google_geventhttpclientwithresue)
    geventhttpclient_time1 = time.time() - start_time
    client.close()
    
    print(f"Time taken for {total_requests} requests with concurrency {concurrency}:")
    print(f"aiohttp: {aiohttp_time} seconds")
    print(f"GeventHTTPClient: {geventhttpclient_time} seconds")
    print(f"GeventHTTPClient reuse: {geventhttpclient_time1} seconds")

if __name__ == "__main__":
    main()
