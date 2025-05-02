import aiohttp
from typing import Callable, Optional

async def lookup_domain_borndate(
    domain: str,
    query_url_func: Callable[[str], str],
    parse_borndate_func: Callable[[dict], Optional[str]],
    proxy_url: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    timeout: int = 30,
) -> Optional[str]:
    """
    通用的域名born date查询方法。
    - query_url_func: 传入domain返回查询url
    - parse_borndate_func: 传入json dict返回born date字符串
    - session: 必须由调用方创建和关闭
    """
    url = query_url_func(domain)
    if session is None:
        raise ValueError("session must be provided and managed by the caller")
    try:
        async with session.get(url, proxy=proxy_url, timeout=timeout) as response:
            if response.status == 200:
                data = await response.json()
                return parse_borndate_func(data)
    except Exception as e:
        print(f"lookup_domain_borndate error: {e}")
    return None

# Revved专用

def revved_query_url(domain: str) -> str:
    return f'https://domains.revved.com/v1/whois?domains={domain}'

def revved_parse_borndate(data: dict) -> Optional[str]:
    if 'results' in data:
        for event in data.get('results', []):
            if 'createdDate' in event:
                return event['createdDate']
    return None

# RDAP专用

def rdap_query_url(domain: str, rdap_url: str) -> str:
    return f'{rdap_url}domain/{domain}'

def rdap_parse_borndate(data: dict) -> Optional[str]:
    for event in data.get('events', []):
        if event.get('eventAction') == 'registration':
            return event.get('eventDate')
    return None 