#!/usr/bin/env python3
"""
Standalone Playwright URL Scraper Script
Scrapes product information using headless Chromium with Webshare proxy and Gemini AI extraction.
"""

import asyncio
import os
import time
import json
import re
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlparse, unquote

from playwright.async_api import async_playwright
from google import genai
from google.genai import types
from bs4 import BeautifulSoup, Comment


CHROMIUM_PATH = "/nix/store/qa9cnw4v5xkxyip6mb9kxqfq1z4x2dx1-chromium-138.0.7204.100/bin/chromium"

TARGET_URL = "https://www.saojoaofarmacias.com.br/trealens-solucao-para-lentes-120ml---360ml-lebon-10036412/p"

WAIT_TIME_MS = 5000


def get_proxy_config():
    """Parse proxy URL from environment and return Playwright proxy config."""
    proxy_url = os.getenv('WEBSHARE_PROXY_URL')
    if not proxy_url:
        raise ValueError("WEBSHARE_PROXY_URL environment variable not set")
    
    parsed = urlparse(proxy_url)
    
    proxy_host = parsed.hostname
    proxy_port = parsed.port or 80
    proxy_user = unquote(parsed.username) if parsed.username else None
    proxy_pass = unquote(parsed.password) if parsed.password else None
    
    print(f"🔐 Proxy configured: {proxy_host}:{proxy_port}")
    
    return {
        "server": f"http://{proxy_host}:{proxy_port}",
        "username": proxy_user,
        "password": proxy_pass
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


async def fetch_with_playwright(url: str, wait_time: int = 5000) -> Optional[str]:
    """Fetch page content using Playwright with proxy."""
    proxy_config = get_proxy_config()
    
    print(f"\n🎭 PLAYWRIGHT SCRAPER: Starting")
    print(f"   • Target URL: {url}")
    print(f"   • Wait time: {wait_time}ms")
    
    try:
        async with async_playwright() as p:
            print("🚀 Launching Chromium browser...")
            
            browser = await p.chromium.launch(
                headless=True,
                executable_path=CHROMIUM_PATH,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                ]
            )
            
            print("📡 Creating browser context with proxy...")
            context = await browser.new_context(
                proxy=proxy_config,
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='pt-BR',
                timezone_id='America/Sao_Paulo',
                extra_http_headers={
                    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                }
            )
            
            page = await context.new_page()
            
            print(f"🌐 Navigating to: {url}")
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            except Exception as nav_error:
                print(f"⚠️ Navigation warning: {str(nav_error)}")
            
            print(f"⏳ Waiting {wait_time}ms for dynamic content...")
            await asyncio.sleep(wait_time / 1000)
            
            try:
                await page.wait_for_load_state('networkidle', timeout=15000)
                print("✅ Network idle achieved")
            except Exception:
                print("⚠️ Network idle timeout, proceeding anyway")
            
            html_content = await page.content()
            
            await context.close()
            await browser.close()
            
            print(f"✅ HTML fetched: {len(html_content):,} bytes")
            return html_content
            
    except Exception as e:
        print(f"❌ Playwright error: {str(e)}")
        return None


async def extract_product_info(client, url: str, cleaned_html: str) -> Dict[str, Any]:
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


async def main():
    """Main function to run the Playwright URL scraper."""
    print("=" * 60)
    print("PLAYWRIGHT URL SCRAPER - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    client = get_gemini_client()
    
    start_time = time.time()
    html_content = await fetch_with_playwright(TARGET_URL, WAIT_TIME_MS)
    fetch_time = time.time() - start_time
    
    if not html_content:
        print("\n❌ Failed to fetch HTML content")
        return
    
    print(f"\n🧹 Cleaning HTML for AI processing...")
    start_time = time.time()
    cleaned_html, area_counts = clean_html(html_content)
    clean_time = time.time() - start_time
    
    reduction = ((len(html_content) - len(cleaned_html)) / len(html_content)) * 100
    print(f"✅ HTML cleaned in {clean_time:.2f}s ({reduction:.1f}% reduction)")
    print(f"   • Original: {len(html_content):,} bytes")
    print(f"   • Cleaned: {len(cleaned_html):,} bytes")
    
    print(f"\n🤖 Sending to Gemini AI for extraction...")
    start_time = time.time()
    product_info = await extract_product_info(client, TARGET_URL, cleaned_html)
    ai_time = time.time() - start_time
    
    total_elapsed = time.time() - total_start
    
    print(f"\n✅ AI extraction completed in {ai_time:.2f}s")
    print(f"\n📦 PRODUCT INFORMATION:")
    print(f"   • Name: {product_info.get('scrapedProductName', 'N/A')}")
    print(f"   • Brand: {product_info.get('brandName', 'N/A')}")
    print(f"   • Price: {product_info.get('currency', '')} {product_info.get('regularPrice', 'N/A')}")
    print(f"   • Available: {product_info.get('isAvailable', 'N/A')}")
    print(f"   • Seller: {product_info.get('seller', 'N/A')}")
    
    with open('product-data.json', 'w', encoding='utf-8') as f:
        json.dump(product_info, f, ensure_ascii=False, indent=2)
    
    print(f"\n📁 Data saved to: product-data.json")
    print(f"\n⏱️ Timing breakdown:")
    print(f"   • Fetch: {fetch_time:.2f}s")
    print(f"   • Clean: {clean_time:.2f}s")
    print(f"   • AI: {ai_time:.2f}s")
    print(f"   • Total: {total_elapsed:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
