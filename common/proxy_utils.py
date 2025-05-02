import requests
import os
import json
from concurrent.futures import ThreadPoolExecutor
import re
import time

SHARED_PROXY_CACHE = 'shared_valid_proxies.txt'
SHARED_PROXY_CACHE_TTL = 3600  # 1 hour
SHARED_PROXY_LIST_URLS = [
    'https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/socks5/socks5.txt',
    'https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt',
    'https://raw.githubusercontent.com/dpangestuw/Free-Proxy/refs/heads/main/socks5_proxies.txt',
    'https://github.com/MuRongPIG/Proxy-Master/raw/main/socks5_checked.txt',
]

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

def fetch_proxies_from_urls(urls):
    """
    Fetch proxies from multiple URLs, normalize to socks5://host:port, and return as a list.
    Handles both space and newline separated formats, and ignores empty lines.
    """
    proxies = set()
    for url in urls:
        try:
            resp = requests.get(url, timeout=15)
            text = resp.text
            # Split by any whitespace (newline, space, tab)
            for line in re.split(r'[\s]+', text):
                line = line.strip()
                if not line:
                    continue
                # Remove protocol if present
                if line.startswith('socks5://'):
                    line = line[len('socks5://'):]
                m = re.match(r'^([0-9]{1,3}\.){3}[0-9]{1,3}:(\d{1,5})$', line)
                if m:
                    proxies.add(f'socks5://{line}')
        except Exception as e:
            print(f"Failed to fetch proxies from {url}: {e}")
    return list(proxies)

def validate_proxy(proxy, test_url='https://www.google.com', timeout=8):
    """
    Validate a socks5 proxy by trying to access Google. Return True if accessible.
    """
    import socks
    import socket
    import importlib
    from urllib.request import urlopen
    try:
        proxy_host, proxy_port = proxy.replace('socks5://', '').split(':')
        proxy_port = int(proxy_port)
        socks.set_default_proxy(socks.SOCKS5, proxy_host, proxy_port)
        socket.socket = socks.socksocket
        resp = urlopen(test_url, timeout=timeout)
        if resp.status == 200:
            return True
    except Exception:
        return False
    finally:
        importlib.reload(socket)
    return False

def get_valid_proxies_from_urls(urls, max_count=100):
    """
    Fetch proxies from URLs, validate by Google access, and return up to max_count valid proxies.
    """
    proxies = fetch_proxies_from_urls(urls)
    valid = []
    for proxy in proxies:
        if validate_proxy(proxy):
            valid.append(proxy)
            if len(valid) >= max_count:
                break
    return valid

def get_shared_valid_proxies(max_count=100, force_refresh=False):
    """
    Fetch and validate proxies from all sources, cache result, and share between scripts.
    Only re-validate if cache is older than TTL or force_refresh is True.
    """
    if not force_refresh and os.path.exists(SHARED_PROXY_CACHE):
        mtime = os.path.getmtime(SHARED_PROXY_CACHE)
        if time.time() - mtime < SHARED_PROXY_CACHE_TTL:
            with open(SHARED_PROXY_CACHE, 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
                if proxies:
                    return proxies[:max_count]
    # Fetch and validate
    proxies = get_valid_proxies_from_urls(SHARED_PROXY_LIST_URLS, max_count=max_count)
    if proxies:
        with open(SHARED_PROXY_CACHE, 'w') as f:
            for p in proxies:
                f.write(p + '\n')
    return proxies 