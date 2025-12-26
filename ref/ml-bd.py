#!/usr/bin/env python3
"""
Standalone MercadoLivre Scraper Script with Bright Data
Scrapes MercadoLivre search results using Bright Data API and BeautifulSoup.
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


SEARCH_TERM = "renu bausch lomb"

MAX_RESULTS = 50

API_TIMEOUT = 60


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


def get_bright_data_config():
    """Get Bright Data API configuration from environment."""
    api_key = os.getenv('BRIGHT_DATA_API')
    zone = os.getenv('BRIGHT_DATA_ZONE')
    
    if not api_key:
        raise ValueError("BRIGHT_DATA_API environment variable not set")
    if not zone:
        raise ValueError("BRIGHT_DATA_ZONE environment variable not set")
    
    print(f"🔐 Bright Data configured")
    print(f"   • Zone: {zone}")
    
    return api_key, zone


def construct_search_url(search_term: str) -> str:
    """Construct MercadoLivre search URL with proper encoding."""
    formatted_search = search_term.replace(' ', '-')
    encoded_search = quote(formatted_search, safe='-')
    url = f"https://lista.mercadolivre.com.br/{encoded_search}"
    
    print(f"📝 Constructed URL: {url}")
    
    return url


def fetch_with_bright_data(url: str) -> Optional[str]:
    """Fetch HTML content using Bright Data API."""
    api_key, zone = get_bright_data_config()
    
    print(f"\n📡 Bright Data API: Fetching...")
    print(f"   • URL: {url}")
    print(f"   • Zone: {zone}")
    
    try:
        api_url = "https://api.brightdata.com/request"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        payload = {
            "zone": zone,
            "url": url,
            "format": "raw"
        }
        
        start_time = time.time()
        response = requests.post(api_url, headers=headers, json=payload, timeout=API_TIMEOUT)
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            html_content = response.text
            print(f"✅ Fetched successfully in {elapsed:.2f}s: {len(html_content):,} bytes")
            return html_content
        else:
            print(f"❌ API error: Status {response.status_code}")
            print(f"   • Response: {response.text[:500] if response.text else 'No response'}")
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
    """Main function to run the MercadoLivre Bright Data scraper."""
    print("=" * 60)
    print("MERCADOLIVRE SCRAPER (BRIGHT DATA) - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    url = construct_search_url(SEARCH_TERM)
    
    print(f"\n📡 Step 1/3: Fetching MercadoLivre via Bright Data...")
    start_time = time.time()
    html_content = fetch_with_bright_data(url)
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
    
    with open('ml-bd-results.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    total_elapsed = time.time() - total_start
    
    print(f"\n✅ MERCADOLIVRE BD SEARCH COMPLETED")
    print(f"\n📊 Results summary:")
    print(f"   • Search term: {SEARCH_TERM}")
    print(f"   • Total products: {len(products)}")
    
    if products:
        print(f"\n🛍️ Sample products:")
        for i, product in enumerate(products[:5]):
            print(f"   {i+1}. {product['product_name'][:50] if product['product_name'] else 'N/A'}...")
            print(f"      Price: R$ {product['current_price']} | Seller: {product['seller']}")
    
    print(f"\n📁 Data saved to: ml-bd-results.json")
    print(f"\n⏱️ Timing breakdown:")
    print(f"   • Fetch: {fetch_time:.2f}s")
    print(f"   • Extract: {extract_time:.2f}s")
    print(f"   • Total: {total_elapsed:.2f}s")


if __name__ == "__main__":
    main()
