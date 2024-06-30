import random
import aiohttp
from loguru import logger

class ProxyManager:
    def __init__(self):
        self.valid_proxies = []

    async def get_proxy(self):
        if self.valid_proxies:
            return random.choice(self.valid_proxies)
        
        proxy = await self._fetch_proxy()
        if proxy:
            self.valid_proxies.append(proxy)
        return proxy

    async def _fetch_proxy(self):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get('http://demo.spiderpy.cn/get') as response:
                    data = await response.json()
                    return data['proxy']
            except Exception as e:
                logger.error(f"Error fetching proxy: {e}")
                return None

    async def get_proxy_proxypool(self):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get('https://proxypool.scrape.center/random') as response:
                    return await response.text()
            except Exception as e:
                logger.error(f"Error fetching proxy from proxypool: {e}")
                return None

    def add_valid_proxy(self, proxy):
        if proxy not in self.valid_proxies:
            self.valid_proxies.append(proxy)

    def remove_invalid_proxy(self, proxy):
        if proxy in self.valid_proxies:
            self.valid_proxies.remove(proxy)