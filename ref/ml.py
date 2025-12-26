#!/usr/bin/env python3
"""
Standalone MercadoLivre Scraper Script
Scrapes MercadoLivre search results using Webshare proxy and BeautifulSoup.
"""

import json
import os
import re
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup, Tag
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


SEARCH_TERM = "renu bausch lomb"

MAX_RESULTS = 50


SEARCH_LAYOUT_ITEM_CLASS = 'ui-search-layout__item'
PRODUCT_TITLE_CLASS = 'poly-component__title'
PRODUCT_SELLER_CLASS = 'poly-component__seller'
PRODUCT_IMAGE_CLASS = 'poly-component__picture'
PRODUCT_SHIPPING_CLASS = 'poly-component__shipping'
PRICE_CURRENT_CLASS = 'poly-price__current'
PRICE_PREVIOUS_CLASS = 'andes-money-amount--previous'
PRICE_DISCOUNT_CLASS = 'andes-money-amount__discount'
PRICE_INSTALLMENTS_CLASS = 'poly-price__installments'
MONEY_AMOUNT_FRACTION_CLASS = 'andes-money-amount__fraction'
MONEY_AMOUNT_CENTS_CLASS = 'andes-money-amount__cents'


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


def construct_search_url(search_term: str) -> str:
    """Construct MercadoLivre search URL with proper encoding."""
    formatted_search = search_term.replace(' ', '-')
    encoded_search = quote(formatted_search, safe='-')
    url = f"https://lista.mercadolivre.com.br/{encoded_search}"
    
    print(f"📝 Constructed URL: {url}")
    
    return url


def fetch_with_proxy(url: str) -> Optional[str]:
    """Fetch HTML content using proxy."""
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


def extract_search_items(html_content: str) -> List[Tag]:
    """Extract search result items from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    items = soup.find_all('li', class_=SEARCH_LAYOUT_ITEM_CLASS)
    
    print(f"📦 Found {len(items)} search items")
    
    return items


def extract_money_amount(element: Tag) -> Optional[float]:
    """Extract money amount from an element."""
    if not element:
        return None
    
    fraction = element.find('span', class_=MONEY_AMOUNT_FRACTION_CLASS)
    cents = element.find('span', class_=MONEY_AMOUNT_CENTS_CLASS)
    
    if fraction:
        fraction_text = fraction.get_text(strip=True).replace('.', '').replace(',', '')
        try:
            amount = float(fraction_text)
            if cents:
                cents_text = cents.get_text(strip=True)
                amount += float(cents_text) / 100
            return amount
        except ValueError:
            return None
    
    return None


def extract_product_data(item_html: Tag) -> Dict[str, Any]:
    """Extract product data from a search result item."""
    product_data = {
        'product_name': None,
        'product_url': None,
        'image_url': None,
        'current_price': None,
        'original_price': None,
        'discount_percentage': None,
        'installments': None,
        'shipping_info': None,
        'seller': None,
        'currency': 'R$'
    }
    
    title_element = item_html.find('a', class_=PRODUCT_TITLE_CLASS)
    if title_element:
        product_data['product_name'] = title_element.get_text(strip=True)
        product_data['product_url'] = title_element.get('href')
    
    image_container = item_html.find('div', class_=PRODUCT_IMAGE_CLASS)
    if image_container:
        img = image_container.find('img')
        if img:
            product_data['image_url'] = img.get('data-src') or img.get('src')
    
    price_container = item_html.find('div', class_=PRICE_CURRENT_CLASS)
    if price_container:
        product_data['current_price'] = extract_money_amount(price_container)
    
    original_price = item_html.find('s', class_=PRICE_PREVIOUS_CLASS)
    if original_price:
        product_data['original_price'] = extract_money_amount(original_price)
    
    discount = item_html.find('span', class_=PRICE_DISCOUNT_CLASS)
    if discount:
        discount_text = discount.get_text(strip=True)
        match = re.search(r'(\d+)', discount_text)
        if match:
            product_data['discount_percentage'] = int(match.group(1))
    
    installments = item_html.find('span', class_=PRICE_INSTALLMENTS_CLASS)
    if installments:
        product_data['installments'] = installments.get_text(strip=True)
    
    shipping = item_html.find('div', class_=PRODUCT_SHIPPING_CLASS)
    if shipping:
        product_data['shipping_info'] = shipping.get_text(strip=True)
    
    seller = item_html.find('span', class_=PRODUCT_SELLER_CLASS)
    if seller:
        product_data['seller'] = seller.get_text(strip=True)
    
    return product_data


def main():
    """Main function to run the MercadoLivre scraper."""
    print("=" * 60)
    print("MERCADOLIVRE SCRAPER - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    url = construct_search_url(SEARCH_TERM)
    
    print(f"\n📡 Step 1/3: Fetching MercadoLivre search results...")
    start_time = time.time()
    html_content = fetch_with_proxy(url)
    fetch_time = time.time() - start_time
    
    if not html_content:
        print("\n❌ Failed to fetch search results")
        return
    
    print(f"\n📦 Step 2/3: Extracting product items...")
    start_time = time.time()
    items = extract_search_items(html_content)
    
    products = []
    for item_html in items:
        product_data = extract_product_data(item_html)
        if product_data['product_name']:
            products.append(product_data)
        
        if MAX_RESULTS and len(products) >= MAX_RESULTS:
            break
    
    extract_time = time.time() - start_time
    
    print(f"✅ Extracted {len(products)} products in {extract_time:.2f}s")
    
    print(f"\n🔄 Step 3/3: Preparing response...")
    result = {
        "search_term": SEARCH_TERM,
        "timestamp": datetime.now().isoformat(),
        "total_products": len(products),
        "products": products
    }
    
    with open('ml-results.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    total_elapsed = time.time() - total_start
    
    print(f"\n✅ MERCADOLIVRE SEARCH COMPLETED")
    print(f"\n📊 Results summary:")
    print(f"   • Search term: {SEARCH_TERM}")
    print(f"   • Total products: {len(products)}")
    
    if products:
        print(f"\n🛍️ Sample products:")
        for i, product in enumerate(products[:5]):
            print(f"   {i+1}. {product['product_name'][:50] if product['product_name'] else 'N/A'}...")
            print(f"      Price: R$ {product['current_price']} | Seller: {product['seller']}")
    
    print(f"\n📁 Data saved to: ml-results.json")
    print(f"\n⏱️ Timing breakdown:")
    print(f"   • Fetch: {fetch_time:.2f}s")
    print(f"   • Extract: {extract_time:.2f}s")
    print(f"   • Total: {total_elapsed:.2f}s")


if __name__ == "__main__":
    main()
