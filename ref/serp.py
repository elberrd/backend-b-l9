#!/usr/bin/env python3
"""
Standalone Google Shopping SERP Scraper Script
Fetches Google Shopping results using Bright Data API.
"""

import json
import os
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from urllib.parse import quote

import requests


SEARCH_TERM = "renu bausch lomb"

API_TIMEOUT = 120


def get_bright_data_config():
    """Get Bright Data API configuration from environment."""
    api_key = os.getenv('BRIGHT_DATA_API')
    zone = os.getenv('BRIGHT_DATA_ZONE_SERP')
    
    if not api_key:
        raise ValueError("BRIGHT_DATA_API environment variable not set")
    if not zone:
        raise ValueError("BRIGHT_DATA_ZONE_SERP environment variable not set")
    
    print(f"🔐 Bright Data SERP configured")
    print(f"   • Zone: {zone}")
    
    return api_key, zone


def build_google_shopping_url(search_term: str) -> str:
    """Build Google Shopping URL with proper encoding."""
    encoded_search = quote(search_term, safe='')
    url = f"https://www.google.com.br/search?q={encoded_search}&gl=BR&tbm=shop&brd_json=1"
    
    print(f"📝 Built Google Shopping URL:")
    print(f"   • Search: {search_term}")
    print(f"   • URL: {url}")
    
    return url


def fetch_serp_with_bright_data(url: str) -> Optional[Dict[str, Any]]:
    """Fetch SERP data using Bright Data API."""
    api_key, zone = get_bright_data_config()
    
    print(f"\n📡 Bright Data API: Requesting SERP...")
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
            "format": "json"
        }
        
        print(f"   • Making request to Bright Data API...")
        
        start_time = time.time()
        response = requests.post(api_url, headers=headers, json=payload, timeout=API_TIMEOUT)
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            print(f"✅ SERP data fetched in {elapsed:.2f}s")
            
            try:
                return response.json()
            except json.JSONDecodeError:
                print(f"⚠️ Response is not JSON, returning as text")
                return {"raw_response": response.text}
        else:
            print(f"❌ API error: Status {response.status_code}")
            print(f"   • Response: {response.text[:500] if response.text else 'No response'}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"❌ Request timeout after {API_TIMEOUT}s")
        return None
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return None


def extract_items_from_brightdata(serp_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract product items from Bright Data SERP response."""
    items = []
    
    popular_products = serp_data.get('popular_products', {})
    if popular_products:
        print(f"📦 Found popular_products section")
        
        categories = ['cheap', 'high_review', 'top', 'all']
        for category in categories:
            category_items = popular_products.get(category, [])
            if category_items:
                print(f"   • {category}: {len(category_items)} items")
                for item in category_items:
                    processed_item = {
                        'title': item.get('title', ''),
                        'price': item.get('price', ''),
                        'extracted_price': extract_price_value(item.get('price', '')),
                        'currency': 'R$',
                        'source': item.get('source', ''),
                        'link': item.get('link', ''),
                        'thumbnail': item.get('thumbnail', ''),
                        'rating': item.get('rating'),
                        'reviews': item.get('reviews'),
                        'category': category,
                        'section': 'popular_products'
                    }
                    items.append(processed_item)
    
    shopping_results = serp_data.get('shopping', [])
    if shopping_results:
        print(f"📦 Found shopping section: {len(shopping_results)} items")
        for item in shopping_results:
            processed_item = {
                'title': item.get('title', ''),
                'price': item.get('price', ''),
                'extracted_price': extract_price_value(item.get('price', '')),
                'currency': 'R$',
                'source': item.get('source', ''),
                'link': item.get('link', ''),
                'thumbnail': item.get('thumbnail', ''),
                'rating': item.get('rating'),
                'reviews': item.get('reviews'),
                'category': 'shopping',
                'section': 'shopping'
            }
            items.append(processed_item)
    
    return items


def extract_price_value(price_str: str) -> Optional[float]:
    """Extract numeric price value from string."""
    if not price_str:
        return None
    
    import re
    cleaned = re.sub(r'[^\d.,]', '', price_str)
    
    if not cleaned:
        return None
    
    if ',' in cleaned and '.' in cleaned:
        if cleaned.rfind(',') > cleaned.rfind('.'):
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        parts = cleaned.split(',')
        if len(parts) == 2 and len(parts[1]) <= 2:
            cleaned = cleaned.replace(',', '.')
        else:
            cleaned = cleaned.replace(',', '')
    
    try:
        return float(cleaned)
    except ValueError:
        return None


def main():
    """Main function to run the Google Shopping SERP scraper."""
    print("=" * 60)
    print("GOOGLE SHOPPING SERP SCRAPER - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    search_url = build_google_shopping_url(SEARCH_TERM)
    
    print(f"\n📡 Step 1/3: Fetching Google Shopping SERP...")
    start_time = time.time()
    serp_data = fetch_serp_with_bright_data(search_url)
    fetch_time = time.time() - start_time
    
    if not serp_data:
        print("\n❌ Failed to fetch SERP data")
        return
    
    print(f"\n📦 Step 2/3: Extracting items...")
    start_time = time.time()
    items = extract_items_from_brightdata(serp_data)
    extract_time = time.time() - start_time
    
    print(f"✅ Extracted {len(items)} items in {extract_time:.2f}s")
    
    print(f"\n🔄 Step 3/3: Preparing response...")
    result = {
        "search_term": SEARCH_TERM,
        "timestamp": datetime.now().isoformat(),
        "total_items": len(items),
        "items": items
    }
    
    with open('serp-results.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    total_elapsed = time.time() - total_start
    
    print(f"\n✅ SERP SEARCH COMPLETED")
    print(f"\n📊 Results summary:")
    print(f"   • Search term: {SEARCH_TERM}")
    print(f"   • Total items: {len(items)}")
    
    if items:
        print(f"\n🛍️ Sample items:")
        for i, item in enumerate(items[:5]):
            print(f"   {i+1}. {item['title'][:50]}...")
            print(f"      Price: {item['price']} | Source: {item['source']}")
    
    print(f"\n📁 Data saved to: serp-results.json")
    print(f"\n⏱️ Timing breakdown:")
    print(f"   • Fetch: {fetch_time:.2f}s")
    print(f"   • Extract: {extract_time:.2f}s")
    print(f"   • Total: {total_elapsed:.2f}s")


if __name__ == "__main__":
    main()
