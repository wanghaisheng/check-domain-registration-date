import requests
import os
import json
from concurrent.futures import ThreadPoolExecutor

def get_proxy():
    try:
        return requests.get("http://demo.spiderpy.cn/get/").json().get('proxy')
    except Exception:
        return None

def get_proxy_proxypool():
    try:
        return requests.get("https://proxypool.scrape.center/random").text
    except Exception:
        return None

def test_proxy(proxy_url, test_url):
    try:
        response = requests.get(test_url, proxies={"http": proxy_url, "https": proxy_url}, timeout=10)
        return proxy_url if response.status_code == 200 else None
    except Exception:
        return None

def clean_proxies(valid_proxies, test_url):
    with ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(lambda proxy: test_proxy(proxy, test_url), valid_proxies))
    return [proxy for proxy in results if proxy is not None]

def load_valid_proxies(filename):
    try:
        with open(filename, "r") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception:
        return []

def save_valid_proxies(filename, proxies):
    unique_proxies = set(proxies)
    if not os.path.exists(filename):
        with open(filename, "w") as f:
            for proxy in unique_proxies:
                f.write(proxy + "\n")
    else:
        with open(filename, "r") as f:
            existing = set(f.read().splitlines())
        new_proxies = unique_proxies - existing
        if new_proxies:
            with open(filename, "a") as f:
                for proxy in new_proxies:
                    f.write(proxy + "\n")

def getlocalproxies(proxy_dir):
    raw_proxies = []
    for p in ['http','socks4','socks5']:
        proxyfile = os.path.join(proxy_dir, f'{p}.txt')
        if os.path.exists(proxyfile):
            with open(proxyfile, "r", encoding="utf8") as f:
                tmp = list(set(f.readlines()))
                raw_proxies += [f'{p}://'+v.strip() for v in tmp if v.strip()]
    return list(set(raw_proxies)) 