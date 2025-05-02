import re
from bs4 import BeautifulSoup
from typing import List
import json
import requests

def get_title_from_html(html: str) -> str:
    try:
        title_pattern = r'<title>(.*?)</title>'
        result = re.search(title_pattern, html, re.IGNORECASE | re.DOTALL)
        if result:
            return result.group(1).strip()
    except Exception:
        pass
    return 'not found'

def get_des_from_html(html: str) -> str:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        description_tag = soup.find('meta', attrs={'name': 'description'})
        if description_tag and description_tag.get('content'):
            return description_tag['content'].strip()
    except Exception:
        pass
    return 'not found'

def get_advanced_description(html: str) -> str:
    """
    更智能地提取网页描述：优先meta[name=description]，再尝试og:description、twitter:description、正文首段。
    """
    soup = BeautifulSoup(html, 'html.parser')
    # 1. meta[name=description]
    tag = soup.find('meta', attrs={'name': 'description'})
    if tag and tag.get('content'):
        return tag['content'].strip()
    # 2. og:description
    tag = soup.find('meta', attrs={'property': 'og:description'})
    if tag and tag.get('content'):
        return tag['content'].strip()
    # 3. twitter:description
    tag = soup.find('meta', attrs={'name': 'twitter:description'})
    if tag and tag.get('content'):
        return tag['content'].strip()
    # 4. 正文首段
    p = soup.find('p')
    if p and p.get_text():
        return p.get_text().strip()
    # 5. fallback: 全文前200字
    text = soup.get_text()
    if text:
        return text.strip()[:200]
    return ''

def get_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
    return soup.get_text()

def extract_indexdate_from_google_html(html: str) -> str:
    """
    从Google搜索结果页面HTML中提取"Site first indexed by Google"后的索引时间。
    返回indexdate字符串，未找到则返回空字符串。
    """
    soup = BeautifulSoup(html, "html.parser")
    elements = soup.find_all(lambda tag: "Site first indexed by Google" in tag.get_text())
    if elements:
        r = elements[0].get_text()
        if "Site first indexed by Google" in r:
            r = r.split("Site first indexed by Google")
            date = r[-1]
            if date and not date.endswith('ago'):
                date = date.split('ago')[0] + 'ago'
            return date.strip()
    return ''

def extract_price_plans_from_html(html: str) -> List[str]:
    """
    优化版：
    - 扩展多语言关键词
    - 排除弹窗/广告区块
    - 支持JSON-LD结构化offers/price
    - 区块打分排序，结构化提取
    """
    soup = BeautifulSoup(html, 'html.parser')
    # 1. JSON-LD结构化数据
    price_blocks = set()
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                offers = data.get('offers')
                if offers:
                    if isinstance(offers, dict):
                        price = offers.get('price')
                        if price:
                            price_blocks.add(f"JSON-LD price: {price}")
                    elif isinstance(offers, list):
                        for offer in offers:
                            price = offer.get('price')
                            if price:
                                price_blocks.add(f"JSON-LD price: {price}")
        except Exception:
            continue
    # 2. 多语言关键词
    keywords = [
        "price", "pricing", "plan", "upgrade", "purchase", "premium", "subscription", "fee", "cost", "billing",
        "套餐", "价格", "收费", "订阅", "年费", "月费", "价钱", "收費", "方案", "プラン", "価格", "料金", "구독", "요금", "플랜"
    ]
    # 3. 排除弹窗/广告区块
    exclude_classes = ["modal", "popup", "banner", "cookie", "advert", "ads", "subscribe", "newsletter"]
    def is_excluded(tag):
        cls = ' '.join(tag.get('class', [])).lower()
        id_ = tag.get('id', '').lower()
        return any(x in cls or x in id_ for x in exclude_classes)
    # 4. 结构化区块提取
    candidates = []
    for tag in soup.find_all(["section", "div", "table", "ul", "ol"]):
        if is_excluded(tag):
            continue
        text = tag.get_text(separator=" ", strip=True)
        text_l = text.lower()
        if any(kw in text_l for kw in keywords):
            # 价格正则
            price_matches = re.findall(r'(\$|€|¥|£|USD|CNY|RMB|JPY|KRW)?\s?\d{1,5}(?:[.,]\d{1,2})?\s?(元|円|₩|USD|CNY|RMB|JPY|KRW)?', text)
            score = 0
            if price_matches:
                score += 2 * len(price_matches)
            score += sum(text_l.count(kw) for kw in keywords)
            # 区块靠近正文顶部加分
            if tag.parent and tag.parent.name == 'body':
                score += 2
            # 只保留长度适中的区块
            if 10 < len(text) < 2000:
                candidates.append((score, text))
    # 5. 排序、去重
    candidates.sort(reverse=True)
    for _, text in candidates:
        price_blocks.add(text)
    return list(price_blocks)

def extract_markdown_from_html(html: str) -> str:
    """
    使用markitdown库将HTML内容转换为Markdown格式。
    需要先安装：pip install 'markitdown[all]'
    """
    try:
        from markitdown import MarkItDown
        md = MarkItDown()
        # markitdown支持文件路径和字符串，字符串需用convert_text
        result = md.convert_text(html, input_format="html")
        return result.text_content
    except ImportError:
        raise ImportError("Please install markitdown: pip install 'markitdown[all]'")
    except Exception as e:
        return f"[Markdown conversion error: {e}]"

def extract_price_from_markdown_with_openai(md: str, openai_api_key: str, model: str = "gpt-3.5-turbo") -> str:
    """
    使用OpenAI API对markdown内容进行价格信息抽取。
    返回OpenAI模型提取的价格信息摘要。
    需要安装openai库：pip install openai
    """
    import openai
    openai.api_key = openai_api_key
    prompt = (
        "请从以下Markdown内容中提取所有产品/服务的价格信息，输出结构化JSON数组，"
        "每个元素包含name, price, currency, plan/period（如有），忽略无关内容。Markdown内容如下：\n" + md
    )
    try:
        response = openai.ChatCompletion.create(
            model=model,
            messages=[
                {"role": "system", "content": "你是一个擅长网页信息抽取的AI助手。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=512
        )
        content = response["choices"][0]["message"]["content"]
        return content
    except Exception as e:
        return f"[OpenAI price extraction error: {e}]"

# 新增：通过自定义HTTP API（如本地LLM服务）提取价格信息

def extract_price_from_markdown_with_api(md: str, api_url: str, api_key: str = None, extra_headers: dict = None) -> str:
    """
    通过自定义HTTP API（如本地LLM服务）提取markdown中的价格信息。
    api_url: 你的API地址，需支持POST，body中包含'markdown'字段。
    api_key: 可选，若API需要鉴权。
    extra_headers: 可选，附加header。
    返回API返回的内容。
    """
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if extra_headers:
        headers.update(extra_headers)
    payload = {"markdown": md}
    try:
        resp = requests.post(api_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        return f"[HTTP API price extraction error: {e}]" 