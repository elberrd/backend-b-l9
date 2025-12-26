#!/usr/bin/env python3
"""
Standalone URL Scraper Script
Scrapes product information using Webshare proxy and Gemini AI extraction.
"""

import json
import os
import re
import time
from typing import Dict, Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Comment
from google import genai
from google.genai import types
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


TARGET_URL = "https://www.saojoaofarmacias.com.br/trealens-solucao-para-lentes-120ml---360ml-lebon-10036412/p"


def get_proxy_config():
    """Get proxy configuration from environment."""
    proxy_url = os.getenv('WEBSHARE_PROXY_URL')
    
    if not proxy_url:
        raise ValueError("WEBSHARE_PROXY_URL environment variable not set")
    
    print(f"🔐 Proxy configured")
    
    return {
        "http": proxy_url,
        "https": proxy_url
    }


def get_gemini_client():
    """Initialize Gemini AI client."""
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set")
    
    client = genai.Client(
        api_key=api_key,
        http_options=types.HttpOptions(api_version='v1beta')
    )
    print("🤖 Gemini AI client initialized")
    return client


def fetch_html_with_proxy(url: str) -> Optional[str]:
    """Fetch HTML content using Webshare proxy."""
    proxies = get_proxy_config()
    
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'max-age=0',
        'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    }
    
    print(f"\n📡 Fetching: {url}")
    
    try:
        response = requests.get(
            url,
            headers=headers,
            proxies=proxies,
            timeout=60,
            verify=False
        )
        
        if response.status_code == 200:
            print(f"✅ Fetched successfully: {len(response.text):,} bytes")
            return response.text
        else:
            print(f"❌ HTTP error: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Fetch error: {str(e)}")
        return None


def clean_html(html_content: str) -> Tuple[str, Dict[str, int]]:
    """Clean HTML content to reduce tokens while preserving product information."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    important_scripts = []
    for script in soup.find_all('script'):
        script_content = str(script.string or '')
        script_type = script.get('type', '')
        
        should_keep = False
        if script_type in ['application/ld+json', 'application/json']:
            should_keep = True
        elif script.get('id') == '__NEXT_DATA__':
            should_keep = True
        elif script_content:
            content_lower = script_content.lower()
            product_keywords = ['product', 'price', 'preco', 'sku', 'catalog', 'value', 'discount', 'vtex']
            if any(keyword in content_lower for keyword in product_keywords):
                should_keep = True
        
        if should_keep:
            important_scripts.append(str(script))
    
    for script in soup.find_all('script'):
        script.decompose()
    
    for tag in ['style', 'noscript', 'iframe', 'svg', 'link']:
        for element in soup.find_all(tag):
            element.decompose()
    
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    
    area_counts = {'product': 0, 'price': 0, 'purchase': 0, 'payment': 0, 'shipping': 0, 'stock': 0, 'review': 0}
    product_areas = []
    
    product_selectors = [
        {'class': re.compile('product|item|detail|pdp|sku', re.I)},
        {'class': re.compile('price|cost|value|amount|money', re.I)},
        {'class': re.compile('buy|purchase|cart|add|comprar|adicionar', re.I)},
        {'class': re.compile('payment|installment|parcel|pix|boleto', re.I)},
        {'class': re.compile('ship|delivery|frete|entrega', re.I)},
        {'class': re.compile('stock|availability|disponib|estoque', re.I)},
        {'class': re.compile('review|rating|score|estrela|avalia', re.I)},
    ]
    
    for selector in product_selectors:
        elements = soup.find_all(attrs=selector, limit=10)
        product_areas.extend(elements)
        selector_str = str(selector)
        if 'product' in selector_str.lower():
            area_counts['product'] += len(elements)
        elif 'price' in selector_str.lower():
            area_counts['price'] += len(elements)
    
    meta_tags = []
    for meta in soup.find_all('meta'):
        meta_name = meta.get('name', '').lower()
        meta_property = meta.get('property', '').lower()
        if any(kw in meta_name or kw in meta_property for kw in ['product', 'price', 'title', 'description', 'og:']):
            meta_tags.append(str(meta))
    
    unique_areas = list(set(str(area) for area in product_areas[:15]))
    
    combined = "\n".join(meta_tags) + "\n" + "\n".join(unique_areas) + "\n" + "\n".join(important_scripts)
    
    return combined, area_counts


def extract_product_info(client, url: str, cleaned_html: str) -> Dict[str, Any]:
    """Extract product information using Gemini AI."""
    
    prompt = f"""Analyze this HTML content from a product page and extract product information.

URL: {url}

HTML Content:
{cleaned_html[:50000]}

Extract and return a JSON object with these fields:
- scrapedProductName: Product name/title
- brandName: Brand name if found
- regularPrice: Regular price as a number
- discountPrice: Discounted price if any, as a number
- discountPercentage: Discount percentage if any
- currency: Currency symbol (e.g., "R$", "$", "€")
- isAvailable: Boolean indicating if product is in stock
- outOfStockReason: Reason if not available
- seller: Seller name if found
- shippingCost: Shipping cost if found
- deliveryTime: Estimated delivery time
- installmentOptions: Payment installment info
- product_image_url: Main product image URL
- review_score: Review rating if found
- unitMeasurement: Product unit/size
- kit: Boolean if this is a kit/bundle
- marketplaceWebsite: Website name
- sourceUrl: The scraped URL
- scrapedAt: Current timestamp

Return ONLY valid JSON, no markdown or explanation."""

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=4096,
            )
        )
        
        response_text = response.text.strip()
        
        if response_text.startswith('```'):
            lines = response_text.split('\n')
            response_text = '\n'.join(lines[1:-1])
        
        product_data = json.loads(response_text)
        product_data['sourceUrl'] = url
        product_data['scrapedAt'] = time.time()
        
        return product_data
        
    except Exception as e:
        print(f"❌ Gemini extraction error: {str(e)}")
        return {"error": str(e), "sourceUrl": url, "scrapedAt": time.time()}


def main():
    """Main function to run the URL scraper."""
    print("=" * 60)
    print("URL SCRAPER (WEBSHARE PROXY) - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    client = get_gemini_client()
    
    print(f"\n📡 Step 1/3: Fetching HTML...")
    start_time = time.time()
    html_content = fetch_html_with_proxy(TARGET_URL)
    fetch_time = time.time() - start_time
    
    if not html_content:
        print("\n❌ Failed to fetch HTML content")
        return
    
    print(f"\n🧹 Step 2/3: Cleaning HTML for AI processing...")
    start_time = time.time()
    cleaned_html, area_counts = clean_html(html_content)
    clean_time = time.time() - start_time
    
    reduction = ((len(html_content) - len(cleaned_html)) / len(html_content)) * 100
    print(f"✅ HTML cleaned in {clean_time:.2f}s ({reduction:.1f}% reduction)")
    print(f"   • Original: {len(html_content):,} bytes")
    print(f"   • Cleaned: {len(cleaned_html):,} bytes")
    
    print(f"\n🤖 Step 3/3: Sending to Gemini AI for extraction...")
    start_time = time.time()
    product_info = extract_product_info(client, TARGET_URL, cleaned_html)
    ai_time = time.time() - start_time
    
    total_elapsed = time.time() - total_start
    
    print(f"✅ AI extraction completed in {ai_time:.2f}s")
    
    print(f"\n📦 PRODUCT INFORMATION:")
    print(f"   • Name: {product_info.get('scrapedProductName', 'N/A')}")
    print(f"   • Brand: {product_info.get('brandName', 'N/A')}")
    print(f"   • Price: {product_info.get('currency', '')} {product_info.get('regularPrice', 'N/A')}")
    print(f"   • Available: {product_info.get('isAvailable', 'N/A')}")
    print(f"   • Seller: {product_info.get('seller', 'N/A')}")
    
    with open('url-product-data.json', 'w', encoding='utf-8') as f:
        json.dump(product_info, f, ensure_ascii=False, indent=2)
    
    print(f"\n📁 Data saved to: url-product-data.json")
    print(f"\n⏱️ Timing breakdown:")
    print(f"   • Fetch: {fetch_time:.2f}s")
    print(f"   • Clean: {clean_time:.2f}s")
    print(f"   • AI: {ai_time:.2f}s")
    print(f"   • Total: {total_elapsed:.2f}s")


if __name__ == "__main__":
    main()
