import asyncio
import aiohttp
import httpx
from typing import List, Callable, Any, Optional

async def fetch_url(url: str, proxy: str = None, headers: dict = None) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, proxy=proxy, headers=headers) as response:
            return await response.text()

async def run_async_tasks(tasks: List[asyncio.Task], concurrency: int = 10):
    semaphore = asyncio.Semaphore(concurrency)
    async def sem_task(task):
        async with semaphore:
            return await task
    return await asyncio.gather(*(sem_task(t) for t in tasks))

# 新增通用的semaphore获取方法
def get_semaphore(concurrency: int = 10) -> asyncio.Semaphore:
    """获取一个指定并发数的asyncio.Semaphore实例。"""
    return asyncio.Semaphore(concurrency)

# 新增通用的aiohttp session管理方法
async def get_aiohttp_session(connector: Optional[aiohttp.BaseConnector] = None, **kwargs) -> aiohttp.ClientSession:
    """获取一个aiohttp.ClientSession，可选自定义connector。"""
    return aiohttp.ClientSession(connector=connector, **kwargs)

# ========== httpx async utils ==========
async def httpx_fetch(url: str, proxy: str = None, headers: dict = None, timeout: int = 10, method: str = 'GET', **kwargs) -> str:
    async with httpx.AsyncClient(proxies=proxy, timeout=timeout) as client:
        resp = await client.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp.text

async def get_httpx_client(proxies: str = None, timeout: int = 10, **kwargs) -> httpx.AsyncClient:
    """获取一个httpx.AsyncClient，可选代理和超时。"""
    return httpx.AsyncClient(proxies=proxies, timeout=timeout, **kwargs) 