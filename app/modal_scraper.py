"""
Modal-based Web Scraper v3.2 (Optimized Configuration)
=======================================================
Scalable web scraper using Modal.com infrastructure with 3-tier fallback system.
Fully async implementation with optimized container/concurrency configuration.

Scraping Priority (configurable via PRIMARY_SCRAPER):
- If PRIMARY_SCRAPER=firecrawl: Firecrawl -> Bright Data -> Playwright
- If PRIMARY_SCRAPER=brightdata: Bright Data -> Firecrawl -> Playwright

Key Optimization Insight:
- Firecrawl has API limits (Hobby=5 concurrent) → use FEW containers, MORE inputs each
- BrightData has NO limits → use MORE containers, moderate inputs each
- This minimizes cold start overhead while respecting API limits

Optimal Configurations:
- Firecrawl (Hobby):  2 containers × 10 inputs, concurrency=5
- BrightData:        50 containers × 20 inputs, concurrency=50

Environment Variables:
- PRIMARY_SCRAPER: Primary service ("firecrawl" or "brightdata", default: "firecrawl")
- MAX_CONCURRENCY: Override max concurrent scrapes
- MAX_CONTAINERS: Override max containers
- MAX_INPUTS: Override max inputs per container

Usage:
    modal run app/modal_scraper.py
    modal run app/modal_scraper.py --input-file urls.json
    modal deploy app/modal_scraper.py
"""

import modal
import asyncio
import json
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import List, Optional, Tuple

# =============================================================================
# Modal App Configuration
# =============================================================================

app = modal.App("web-scraper")

# Container image with all scraping dependencies
scraper_image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "wget", "gnupg", "ca-certificates", "fonts-liberation",
        "libasound2", "libatk-bridge2.0-0", "libatk1.0-0", "libatspi2.0-0",
        "libcups2", "libdbus-1-3", "libdrm2", "libgbm1", "libgtk-3-0",
        "libnspr4", "libnss3", "libxcomposite1", "libxdamage1", "libxfixes3",
        "libxkbcommon0", "libxrandr2", "xdg-utils",
    )
    .pip_install(
        "playwright==1.42.0",
        "playwright-stealth>=1.0.6",
        "httpx>=0.27.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
        "google-genai>=1.0.0",
        "pillow>=10.0.0",
        "boto3>=1.28.0",
        "firecrawl-py>=1.0.0",
    )
    .run_commands(
        "playwright install chromium",
        "playwright install-deps chromium",
    )
)

# Constants
MIN_HTML_SIZE = 5000
METHOD_FIRECRAWL = "firecrawl"
METHOD_BRIGHTDATA = "brightdata"
METHOD_PLAYWRIGHT = "playwright"

# Primary scraper options
PRIMARY_FIRECRAWL = "firecrawl"
PRIMARY_BRIGHTDATA = "brightdata"

# =============================================================================
# Optimal Configuration by Primary Scraper
# =============================================================================
#
# FIRECRAWL has hard API limits by plan:
#   - Hobby: 5 concurrent browsers → use fewer containers, more inputs/container
#   - Standard: 50 concurrent → moderate containers
#   - Growth: 100 concurrent → more containers
#
# BRIGHTDATA has NO concurrency limit:
#   - Can scale horizontally with more containers
#   - Optimal: 50 containers × 20 inputs = 1000 capacity
#
# The key insight: For Firecrawl, having 100 containers is WASTEFUL because
# only 5 can make API calls at once. The rest just wait and incur cold start costs.

# Firecrawl configuration (Hobby plan = 5 concurrent)
FIRECRAWL_MAX_CONCURRENCY = 5       # API limit (Hobby plan)
FIRECRAWL_MAX_CONTAINERS = 2        # 1 main + 1 buffer (minimal cold starts)
FIRECRAWL_MAX_INPUTS = 10           # Each container handles 2× the limit

# BrightData configuration (unlimited concurrency)
BRIGHTDATA_MAX_CONCURRENCY = 50     # Limited by Gemini/Modal, not BrightData
BRIGHTDATA_MAX_CONTAINERS = 50      # Scale horizontally
BRIGHTDATA_MAX_INPUTS = 20          # Optimal for I/O-bound work


# =============================================================================
# Data Models
# =============================================================================

@dataclass
class AttemptResult:
    """Result of a single scraping attempt."""
    success: bool
    html: Optional[str] = None
    screenshot_bytes: Optional[bytes] = None
    error: Optional[str] = None
    method: str = ""


@dataclass
class AttemptError:
    """Error from a single scraping attempt."""
    method: str
    operation: str
    error: str

    def to_dict(self) -> dict:
        return {"method": self.method, "operation": self.operation, "error": self.error}


@dataclass
class ScrapeResult:
    """Result of scraping a single URL."""
    urlId: str
    url: str
    status: str
    scrapedAt: int
    errorMessage: Optional[str] = None
    screenshotUrl: Optional[str] = None
    productTitle: Optional[str] = None
    brand: Optional[str] = None
    currentPrice: Optional[float] = None
    originalPrice: Optional[float] = None
    discountPercentage: Optional[float] = None
    currency: Optional[str] = None
    availability: Optional[bool] = None
    imageUrl: Optional[str] = None
    seller: Optional[str] = None
    shippingInfo: Optional[str] = None
    shippingCost: Optional[float] = None
    deliveryTime: Optional[str] = None
    review_score: Optional[str] = None
    installmentOptions: Optional[str] = None
    kit: Optional[bool] = None
    unitMeasurement: Optional[str] = None
    outOfStockReason: Optional[str] = None
    marketplaceWebsite: Optional[str] = None
    sku: Optional[str] = None
    ean: Optional[str] = None
    stockQuantity: Optional[int] = None
    otherPaymentMethods: Optional[str] = None
    promotionDetails: Optional[str] = None
    method: Optional[str] = None
    attempts: Optional[List[str]] = None
    errors: Optional[List[dict]] = None  # Array of errors from failed attempts

    def to_dict(self) -> dict:
        result = {
            "urlId": self.urlId,
            "productUrl": self.url,
            "status": self.status,
            "scrapedAt": self.scrapedAt,
        }
        for field_name in [
            "errorMessage", "screenshotUrl", "productTitle", "brand",
            "currentPrice", "originalPrice", "discountPercentage", "currency",
            "availability", "imageUrl", "seller", "shippingInfo", "shippingCost",
            "deliveryTime", "review_score", "installmentOptions", "kit", "unitMeasurement",
            "outOfStockReason", "marketplaceWebsite", "sku", "ean", "stockQuantity",
            "otherPaymentMethods", "promotionDetails", "method", "attempts", "errors"
        ]:
            value = getattr(self, field_name)
            if value is not None:
                result[field_name] = value
        return result


# =============================================================================
# HTML Cleaner
# =============================================================================

def clean_html(html_content: str) -> Tuple[str, dict]:
    """Clean HTML content to reduce tokens while preserving product information."""
    from bs4 import BeautifulSoup, Comment

    soup = BeautifulSoup(html_content, 'lxml')

    # Preserve important scripts
    important_scripts = []
    for script in soup.find_all('script'):
        script_content = str(script.string or '')
        script_type = script.get('type', '')
        should_keep = (
            script_type in ['application/ld+json', 'application/json'] or
            script.get('id') == '__NEXT_DATA__' or
            any(kw in script_content.lower() for kw in ['product', 'price', 'preco', 'sku', 'catalog', 'vtex'])
        )
        if should_keep:
            important_scripts.append(str(script))

    for script in soup.find_all('script'):
        script.decompose()
    for tag in ['style', 'noscript', 'iframe', 'svg', 'link']:
        for el in soup.find_all(tag):
            el.decompose()
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # Find product areas
    product_areas = []
    for selector in [
        {'class': re.compile('product|item|detail|pdp|sku', re.I)},
        {'class': re.compile('price|cost|value|amount|money', re.I)},
        {'class': re.compile('buy|purchase|cart|add|comprar', re.I)},
        {'class': re.compile('payment|installment|parcel|pix', re.I)},
        {'class': re.compile('ship|delivery|frete|entrega', re.I)},
        {'class': re.compile('stock|availability|disponib', re.I)},
        {'class': re.compile('review|rating|score|estrela', re.I)},
    ]:
        product_areas.extend(soup.find_all(attrs=selector, limit=10))

    # Get meta tags
    meta_tags = [str(m) for m in soup.find_all('meta')
                 if any(kw in m.get('name', '').lower() + m.get('property', '').lower()
                       for kw in ['product', 'price', 'title', 'description', 'og:'])]

    unique_areas = list(set(str(a) for a in product_areas[:15]))
    combined = "\n".join(meta_tags + unique_areas + important_scripts)

    stats = {
        "original_size": len(html_content),
        "cleaned_size": len(combined),
        "reduction_pct": round((1 - len(combined) / len(html_content)) * 100, 1) if html_content else 0,
    }
    return combined, stats


# =============================================================================
# Image Compression
# =============================================================================

def compress_image(image_bytes: bytes, quality: int = 85) -> Tuple[bytes, dict]:
    """Compress PNG image to optimized JPEG."""
    from PIL import Image

    original_size = len(image_bytes)
    img = Image.open(BytesIO(image_bytes))
    if img.mode in ('RGBA', 'P'):
        img = img.convert('RGB')

    output = BytesIO()
    img.save(output, format='JPEG', quality=quality, optimize=True)
    compressed = output.getvalue()

    stats = {
        'original_size': original_size,
        'compressed_size': len(compressed),
        'reduction_percent': round((1 - len(compressed) / original_size) * 100, 1) if original_size else 0,
    }
    return compressed, stats


# =============================================================================
# R2 Client (Pooled)
# =============================================================================

# Global R2 client - created once per container
_r2_client = None


def get_r2_client():
    """Get or create pooled R2 S3 client."""
    global _r2_client

    if _r2_client is not None:
        return _r2_client, None

    import boto3
    from botocore.config import Config

    account_id = os.environ.get("R2_ACCOUNT_ID")
    access_key = os.environ.get("R2_ACCESS_KEY_ID")
    secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

    if not all([account_id, access_key, secret_key]):
        return None, "R2 configuration incomplete"

    _r2_client = boto3.client(
        's3',
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4', retries={'max_attempts': 3})
    )
    return _r2_client, None


async def upload_to_r2_async(image_bytes: bytes, url_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Upload image to R2 (run in thread pool for async compatibility)."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, upload_to_r2_sync, image_bytes, url_id)


def upload_to_r2_sync(image_bytes: bytes, url_id: str) -> Tuple[Optional[str], Optional[str]]:
    """Synchronous R2 upload."""
    bucket_name = os.environ.get("R2_BUCKET_NAME", "screenshots")
    public_url = os.environ.get("R2_PUBLIC_URL")

    if not public_url:
        return None, "R2_PUBLIC_URL not configured"

    client, error = get_r2_client()
    if error:
        return None, error

    try:
        now = datetime.utcnow()
        filename = f"screenshots/{now.strftime('%Y/%m/%d')}/{url_id}_{uuid.uuid4().hex[:8]}.jpg"
        client.put_object(Bucket=bucket_name, Key=filename, Body=image_bytes, ContentType='image/jpeg')
        return f"{public_url.rstrip('/')}/{filename}", None
    except Exception as e:
        return None, f"R2 upload error: {str(e)[:200]}"


async def delete_from_r2_async(screenshot_url: str) -> bool:
    """Delete image from R2 asynchronously."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, delete_from_r2_sync, screenshot_url)


def delete_from_r2_sync(screenshot_url: str) -> bool:
    """Synchronous R2 delete."""
    bucket_name = os.environ.get("R2_BUCKET_NAME", "screenshots")
    public_url = os.environ.get("R2_PUBLIC_URL", "")

    if not screenshot_url or not public_url:
        return False

    client, error = get_r2_client()
    if error:
        return False

    try:
        key = screenshot_url.replace(public_url.rstrip('/') + '/', '')
        client.delete_object(Bucket=bucket_name, Key=key)
        return True
    except Exception:
        return False


# =============================================================================
# Method 1: Firecrawl API (Async)
# =============================================================================

async def attempt_firecrawl_async(url: str, url_id: str) -> AttemptResult:
    """Async Firecrawl scraping."""
    import httpx

    api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not api_key:
        return AttemptResult(success=False, error="FIRECRAWL_API_KEY not configured", method=METHOD_FIRECRAWL)

    try:
        print(f"[{url_id}] Firecrawl: Starting async scrape...")

        async with httpx.AsyncClient(timeout=httpx.Timeout(90.0)) as client:
            # Call Firecrawl API v2
            response = await client.post(
                "https://api.firecrawl.dev/v2/scrape",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "url": url,
                    "formats": ["html", "screenshot"],
                    "waitFor": 2000,
                    "timeout": 60000,
                }
            )

            if response.status_code != 200:
                return AttemptResult(
                    success=False,
                    error=f"Firecrawl API error: {response.status_code}",
                    method=METHOD_FIRECRAWL
                )

            data = response.json()

            if not data.get("success"):
                return AttemptResult(
                    success=False,
                    error=f"Firecrawl failed: {data.get('error', 'Unknown')}",
                    method=METHOD_FIRECRAWL
                )

            result_data = data.get("data", {})
            html_content = result_data.get("html") or result_data.get("rawHtml")

            if not html_content or len(html_content) < MIN_HTML_SIZE:
                return AttemptResult(
                    success=False,
                    error=f"Firecrawl HTML too small ({len(html_content) if html_content else 0} bytes)",
                    method=METHOD_FIRECRAWL
                )

            print(f"[{url_id}] Firecrawl: HTML {len(html_content):,} bytes")

            # Get screenshot
            screenshot_bytes = None
            screenshot_url = result_data.get("screenshot")

            if screenshot_url and screenshot_url.startswith("http"):
                print(f"[{url_id}] Firecrawl: Downloading screenshot...")
                try:
                    img_response = await client.get(screenshot_url, timeout=30.0)
                    if img_response.status_code == 200 and len(img_response.content) > 1000:
                        screenshot_bytes = img_response.content
                        print(f"[{url_id}] Firecrawl: Screenshot {len(screenshot_bytes):,} bytes")
                except Exception as e:
                    print(f"[{url_id}] Firecrawl: Screenshot download error: {str(e)[:50]}")

            return AttemptResult(
                success=True,
                html=html_content,
                screenshot_bytes=screenshot_bytes,
                method=METHOD_FIRECRAWL
            )

    except httpx.TimeoutException:
        return AttemptResult(success=False, error="Firecrawl timeout", method=METHOD_FIRECRAWL)
    except Exception as e:
        return AttemptResult(success=False, error=f"Firecrawl error: {str(e)[:200]}", method=METHOD_FIRECRAWL)


# =============================================================================
# Method 2: Bright Data Web Unlocker (Async)
# =============================================================================

async def attempt_brightdata_async(url: str, url_id: str) -> AttemptResult:
    """Async Bright Data scraping."""
    import httpx

    api_key = os.environ.get("BRIGHT_DATA_API")
    zone = os.environ.get("BRIGHT_DATA_ZONE", "web_unlocker1")

    if not api_key:
        return AttemptResult(success=False, error="BRIGHT_DATA_API not configured", method=METHOD_BRIGHTDATA)

    api_url = "https://api.brightdata.com/request"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(180.0)) as client:
            # Fetch HTML
            print(f"[{url_id}] Bright Data: Fetching HTML...")
            response = await client.post(
                api_url,
                headers=headers,
                json={"zone": zone, "url": url, "format": "raw"}
            )

            if response.status_code != 200:
                return AttemptResult(
                    success=False,
                    error=f"Bright Data HTML error: {response.status_code}",
                    method=METHOD_BRIGHTDATA
                )

            html_content = response.text
            if len(html_content) < MIN_HTML_SIZE:
                return AttemptResult(
                    success=False,
                    error=f"Bright Data HTML too small ({len(html_content)} bytes)",
                    method=METHOD_BRIGHTDATA
                )

            print(f"[{url_id}] Bright Data: HTML {len(html_content):,} bytes")

            # Fetch screenshot
            screenshot_bytes = None
            print(f"[{url_id}] Bright Data: Taking screenshot...")

            try:
                screen_response = await client.post(
                    api_url,
                    headers=headers,
                    json={"zone": zone, "url": url, "format": "raw", "data_format": "screenshot"},
                    timeout=httpx.Timeout(150.0)
                )
                if screen_response.status_code == 200 and len(screen_response.content) > 1000:
                    screenshot_bytes = screen_response.content
                    print(f"[{url_id}] Bright Data: Screenshot {len(screenshot_bytes):,} bytes")
            except Exception as e:
                print(f"[{url_id}] Bright Data: Screenshot error: {str(e)[:50]}")

            return AttemptResult(
                success=True,
                html=html_content,
                screenshot_bytes=screenshot_bytes,
                method=METHOD_BRIGHTDATA
            )

    except httpx.TimeoutException:
        return AttemptResult(success=False, error="Bright Data timeout", method=METHOD_BRIGHTDATA)
    except Exception as e:
        return AttemptResult(success=False, error=f"Bright Data error: {str(e)[:200]}", method=METHOD_BRIGHTDATA)


# =============================================================================
# Method 3: Playwright + Stealth (Async)
# =============================================================================

async def attempt_playwright_async(url: str, url_id: str) -> AttemptResult:
    """Async Playwright scraping with stealth mode."""
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth
    from urllib.parse import urlparse, unquote

    popup_blocker = """
    ['modal','popup','overlay','cookie','banner','consent','newsletter'].forEach(kw => {
        document.querySelectorAll(`[class*="${kw}"],[id*="${kw}"]`).forEach(el => {
            const s = getComputedStyle(el);
            if (parseInt(s.zIndex) > 100 || s.position === 'fixed') el.remove();
        });
    });
    document.body.style.overflow = 'auto';
    """

    proxy_url = os.environ.get("WEBSHARE_PROXY_URL")
    proxy_config = None
    if proxy_url:
        p = urlparse(proxy_url)
        proxy_config = {
            "server": f"http://{p.hostname}:{p.port or 80}",
            "username": unquote(p.username) if p.username else None,
            "password": unquote(p.password) if p.password else None,
        }

    try:
        async with async_playwright() as p:
            print(f"[{url_id}] Playwright: Launching browser...")
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )

            ctx_args = {
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36',
                'viewport': {'width': 1920, 'height': 1080},
                'locale': 'pt-BR',
                'timezone_id': 'America/Sao_Paulo',
            }
            if proxy_config:
                ctx_args['proxy'] = proxy_config

            context = await browser.new_context(**ctx_args)
            await Stealth().apply_stealth_async(context)

            page = await context.new_page()
            page.on("dialog", lambda d: asyncio.create_task(d.dismiss()))

            print(f"[{url_id}] Playwright: Navigating...")
            try:
                await page.goto(url, wait_until='domcontentloaded', timeout=60000)
            except Exception as e:
                print(f"[{url_id}] Playwright nav warning: {str(e)[:50]}")

            await asyncio.sleep(3)
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass

            html_content = await page.content()
            print(f"[{url_id}] Playwright: HTML {len(html_content):,} bytes")

            if len(html_content) < MIN_HTML_SIZE:
                await context.close()
                await browser.close()
                return AttemptResult(
                    success=False,
                    error=f"Playwright HTML too small ({len(html_content)} bytes)",
                    method=METHOD_PLAYWRIGHT
                )

            # Screenshot
            screenshot_bytes = None
            try:
                await page.evaluate(popup_blocker)
                await asyncio.sleep(0.5)
                screenshot_bytes = await page.screenshot(type='png', full_page=True, timeout=60000)
                print(f"[{url_id}] Playwright: Screenshot {len(screenshot_bytes):,} bytes")
            except Exception as e:
                print(f"[{url_id}] Playwright screenshot error: {str(e)[:50]}")

            await context.close()
            await browser.close()

            return AttemptResult(
                success=True,
                html=html_content,
                screenshot_bytes=screenshot_bytes,
                method=METHOD_PLAYWRIGHT
            )

    except Exception as e:
        return AttemptResult(success=False, error=f"Playwright error: {str(e)[:200]}", method=METHOD_PLAYWRIGHT)


# =============================================================================
# Gemini Extraction (Async)
# =============================================================================

async def extract_product_data_async(html_content: str, url: str, url_id: str) -> Tuple[Optional[dict], Optional[str]]:
    """Extract product data using async Gemini."""
    from google import genai
    from google.genai import types

    try:
        cleaned_html, stats = clean_html(html_content)
        print(f"[{url_id}] HTML cleaned: {stats['reduction_pct']}% reduction")

        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return None, "GEMINI_API_KEY not set"

        client = genai.Client(
            api_key=api_key,
            http_options=types.HttpOptions(api_version='v1beta')
        )

        prompt = f"""Analyze this HTML content from a product page and extract product information.

URL: {url}

HTML Content:
{cleaned_html[:50000]}

Extract and return a JSON object with these fields:
- productTitle: Product name/title
- brand: Brand name if found
- currentPrice: Current/discounted price as a number (the price the customer pays now)
- originalPrice: Original/regular price as a number (before discount, if any)
- discountPercentage: Discount percentage if any, as a number
- currency: Currency symbol (e.g., "R$", "$", "€")
- availability: Boolean indicating if product is in stock
- seller: Seller name if found
- shippingInfo: Shipping information text
- shippingCost: Shipping cost as a number
- deliveryTime: Estimated delivery time
- installmentOptions: Payment installment info (e.g., "10x de R$ 15,90")
- imageUrl: Main product image URL
- review_score: Review rating if found
- kit: Boolean if this is a kit/bundle
- unitMeasurement: Product unit/size (e.g., "100ml", "500g")
- outOfStockReason: Reason if product is not available
- marketplaceWebsite: Website/marketplace name (e.g., "Amazon", "Mercado Livre")
- sku: Product SKU code
- ean: Product EAN/barcode
- stockQuantity: Quantity in stock as a number
- otherPaymentMethods: Other payment methods available (e.g., "PIX, Boleto")
- promotionDetails: Promotion details if any

Return ONLY valid JSON, no markdown or explanation. Omit fields not found."""

        # Use async generate
        response = await client.aio.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1, max_output_tokens=4096)
        )

        text = response.text.strip()
        if text.startswith('```'):
            text = '\n'.join(text.split('\n')[1:-1])

        data = json.loads(text)

        # Debug: show what Gemini extracted
        print(f"[{url_id}] Gemini extracted: {json.dumps(data, ensure_ascii=False)[:500]}")

        if not data.get("currentPrice") and not data.get("originalPrice"):
            # Return partial data with error for debugging
            return None, f"Could not extract price. Got: {json.dumps(data, ensure_ascii=False)[:200]}"

        return data, None

    except json.JSONDecodeError as e:
        print(f"[{url_id}] Gemini raw response: {text[:500] if 'text' in dir() else 'N/A'}")
        return None, f"JSON error: {str(e)[:100]}"
    except Exception as e:
        return None, f"Extraction error: {str(e)[:200]}"


# =============================================================================
# Main Scraper Function (Fully Async)
# =============================================================================

def get_primary_scraper() -> str:
    """Get primary scraper from environment variable."""
    primary = os.environ.get("PRIMARY_SCRAPER", PRIMARY_FIRECRAWL).lower().strip()
    if primary in (PRIMARY_FIRECRAWL, PRIMARY_BRIGHTDATA):
        return primary
    return PRIMARY_FIRECRAWL


def parse_method_preference(method: Optional[str]) -> Optional[str]:
    """
    Parse user-specified method preference.

    Returns normalized method name or None if invalid/not specified.
    """
    if not method:
        return None

    normalized = method.lower().strip().replace(" ", "").replace("_", "").replace("-", "")

    # Map common variations to valid methods
    method_mapping = {
        "firecrawl": METHOD_FIRECRAWL,
        "fc": METHOD_FIRECRAWL,
        "brightdata": METHOD_BRIGHTDATA,
        "bd": METHOD_BRIGHTDATA,
        "playwright": METHOD_PLAYWRIGHT,
        "playwrite": METHOD_PLAYWRIGHT,
        "pw": METHOD_PLAYWRIGHT,
    }

    return method_mapping.get(normalized)


def get_config() -> dict:
    """Get optimal configuration based on primary scraper.

    Returns dict with: max_concurrency, max_containers, max_inputs

    For Firecrawl (Hobby=5 concurrent):
        - Few containers (2) to minimize cold starts
        - Higher inputs/container (10) to handle the queue
        - Concurrency = 5 (API limit)

    For BrightData (unlimited):
        - More containers (50) for horizontal scaling
        - Moderate inputs/container (20)
        - Concurrency = 50 (practical limit)
    """
    primary = get_primary_scraper()

    # Check for explicit overrides
    explicit_conc = os.environ.get("MAX_CONCURRENCY")
    explicit_containers = os.environ.get("MAX_CONTAINERS")
    explicit_inputs = os.environ.get("MAX_INPUTS")

    if primary == PRIMARY_BRIGHTDATA:
        config = {
            "max_concurrency": BRIGHTDATA_MAX_CONCURRENCY,
            "max_containers": BRIGHTDATA_MAX_CONTAINERS,
            "max_inputs": BRIGHTDATA_MAX_INPUTS,
        }
    else:  # Firecrawl
        config = {
            "max_concurrency": FIRECRAWL_MAX_CONCURRENCY,
            "max_containers": FIRECRAWL_MAX_CONTAINERS,
            "max_inputs": FIRECRAWL_MAX_INPUTS,
        }

    # Apply explicit overrides
    if explicit_conc:
        try:
            config["max_concurrency"] = int(explicit_conc)
        except ValueError:
            pass
    if explicit_containers:
        try:
            config["max_containers"] = int(explicit_containers)
        except ValueError:
            pass
    if explicit_inputs:
        try:
            config["max_inputs"] = int(explicit_inputs)
        except ValueError:
            pass

    return config


def get_max_concurrency() -> int:
    """Get max concurrency (for backward compatibility)."""
    return get_config()["max_concurrency"]


# Note: Modal decorators require static values at import time.
# We use maximum possible values here and control actual concurrency via semaphore.
# This allows the same code to work for both Firecrawl (2 containers) and BrightData (50 containers).


@app.function(
    image=scraper_image,
    max_containers=50,   # Max possible (BrightData mode) - actual usage controlled by semaphore
    memory=512,          # MB - enough for 20 concurrent scrapes
    timeout=300,
    retries=1,
    secrets=[modal.Secret.from_name("bausch")],
)
@modal.concurrent(max_inputs=20)  # Max possible - actual controlled by semaphore
async def scrape_url(url_id: str, url: str, method: Optional[str] = None) -> dict:
    """
    Scrape a single URL with 3-tier async fallback system.

    Args:
        url_id: Unique identifier for this URL
        url: The URL to scrape
        method: Optional preferred method to try first (firecrawl, brightdata, playwright)
    """
    config = get_config()
    primary = get_primary_scraper()

    # Check if user specified a method preference
    preferred_method = parse_method_preference(method)

    print(f"\n{'='*60}")
    print(f"[{url_id}] Starting scrape: {url[:60]}...")
    if preferred_method:
        print(f"[{url_id}] User preferred: {preferred_method.upper()}")
    print(f"[{url_id}] Primary: {primary.upper()} | Concurrency: {config['max_concurrency']}")
    print(f"{'='*60}")

    start_time = time.time()
    result = ScrapeResult(
        urlId=url_id, url=url, status="error",
        scrapedAt=int(time.time() * 1000), attempts=[], errors=[]
    )

    last_screenshot_url = None
    last_error = None

    # Define all methods with their functions
    all_methods = {
        METHOD_FIRECRAWL: ("Firecrawl", attempt_firecrawl_async),
        METHOD_BRIGHTDATA: ("Bright Data", attempt_brightdata_async),
        METHOD_PLAYWRIGHT: ("Playwright", attempt_playwright_async),
    }

    # Build attempt order based on preference or PRIMARY_SCRAPER
    if preferred_method and preferred_method in all_methods:
        # User specified a method - put it first, then others in default order
        attempt_order = [preferred_method]
        default_order = [METHOD_FIRECRAWL, METHOD_BRIGHTDATA, METHOD_PLAYWRIGHT]
        for m in default_order:
            if m != preferred_method:
                attempt_order.append(m)
    elif primary == PRIMARY_BRIGHTDATA:
        attempt_order = [METHOD_BRIGHTDATA, METHOD_FIRECRAWL, METHOD_PLAYWRIGHT]
    else:
        attempt_order = [METHOD_FIRECRAWL, METHOD_BRIGHTDATA, METHOD_PLAYWRIGHT]

    attempt_methods = [(all_methods[m][0], all_methods[m][1], m) for m in attempt_order]

    # Retry configuration per method
    # Firecrawl: 1 attempt (often blocked, no point retrying)
    # Bright Data: 3 attempts (reliable but sometimes needs retries)
    # Playwright: 2 attempts (local browser, worth retrying)
    RETRIES_CONFIG = {
        METHOD_FIRECRAWL: 1,
        METHOD_BRIGHTDATA: 3,
        METHOD_PLAYWRIGHT: 2,
    }

    for attempt_num, (method_name, attempt_func, method_key) in enumerate(attempt_methods, 1):
        # Determine how many retries this method gets
        max_retries = RETRIES_CONFIG.get(method_key, 1)

        attempt_result = None
        method_succeeded = False

        for retry in range(max_retries):
            retry_suffix = f" (attempt {retry + 1}/{max_retries})" if max_retries > 1 else ""
            print(f"\n[{url_id}] === METHOD {attempt_num}/3: {method_name}{retry_suffix} ===")

            if retry == 0:
                result.attempts.append(method_name)
            else:
                result.attempts.append(f"{method_name} (retry {retry})")

            try:
                attempt_result = await attempt_func(url, url_id)
            except Exception as e:
                error_msg = f"{method_name} exception: {str(e)[:200]}"
                print(f"[{url_id}] {method_name} exception: {str(e)[:100]}")
                result.errors.append({
                    "method": method_key,
                    "operation": "scrape",
                    "error": error_msg
                })
                last_error = error_msg
                continue  # Try next retry if available

            if not attempt_result.success:
                print(f"[{url_id}] {method_name} failed: {attempt_result.error}")
                result.errors.append({
                    "method": method_key,
                    "operation": "scrape",
                    "error": attempt_result.error or "Unknown error"
                })
                last_error = attempt_result.error
                continue  # Try next retry if available

            # Success! Break out of retry loop
            method_succeeded = True
            break

        # If all retries failed, move to next method
        if not method_succeeded:
            continue

        # Process screenshot
        screenshot_url = None
        if attempt_result.screenshot_bytes:
            try:
                compressed, stats = compress_image(attempt_result.screenshot_bytes, quality=85)
                print(f"[{url_id}] Image compressed: {stats['reduction_percent']}%")
                screenshot_url, err = await upload_to_r2_async(compressed, url_id)
                if screenshot_url:
                    print(f"[{url_id}] Screenshot uploaded: {screenshot_url}")
                    last_screenshot_url = screenshot_url
                elif err:
                    print(f"[{url_id}] R2 error: {err}")
                    result.errors.append({
                        "method": "r2",
                        "operation": "upload",
                        "error": err
                    })
            except Exception as e:
                print(f"[{url_id}] Screenshot error: {str(e)[:100]}")
                result.errors.append({
                    "method": method_key,
                    "operation": "screenshot_process",
                    "error": str(e)[:200]
                })

        # Extract data
        product_data, extraction_error = await extract_product_data_async(
            attempt_result.html, url, url_id
        )

        if product_data and (product_data.get("currentPrice") or product_data.get("originalPrice")):
            print(f"[{url_id}] SUCCESS with {method_name}!")

            result.status = "completed"
            result.method = attempt_result.method
            result.screenshotUrl = screenshot_url
            # Only include errors if there were any
            if not result.errors:
                result.errors = None
            for field in ["productTitle", "brand", "currentPrice", "originalPrice",
                          "discountPercentage", "currency", "availability", "imageUrl",
                          "seller", "shippingInfo", "shippingCost", "deliveryTime",
                          "review_score", "installmentOptions", "kit", "unitMeasurement",
                          "outOfStockReason", "marketplaceWebsite", "sku", "ean",
                          "stockQuantity", "otherPaymentMethods", "promotionDetails"]:
                setattr(result, field, product_data.get(field))

            print(f"[{url_id}] Completed in {time.time() - start_time:.2f}s")
            return result.to_dict()

        else:
            print(f"[{url_id}] {method_name}: Extraction failed - {extraction_error}")
            error_entry = {
                "method": "gemini",
                "operation": "extraction",
                "error": extraction_error or "Could not extract price"
            }
            # Keep screenshot URL in error for debugging (don't delete)
            if screenshot_url:
                error_entry["screenshotUrl"] = screenshot_url
                last_screenshot_url = screenshot_url
                print(f"[{url_id}] Keeping screenshot for debugging: {screenshot_url}")
            result.errors.append(error_entry)
            last_error = extraction_error or "Could not extract price"

    # All failed
    print(f"\n[{url_id}] ALL 3 ATTEMPTS FAILED")
    result.status = "error"
    result.errorMessage = last_error or "All methods failed"
    result.screenshotUrl = last_screenshot_url
    # Keep errors array (don't set to None for failed results)

    print(f"[{url_id}] Failed after {time.time() - start_time:.2f}s")
    return result.to_dict()


# =============================================================================
# Batch Processing with Semaphore
# =============================================================================

@app.function(
    image=scraper_image,
    timeout=7200,  # 2 hours for large batches
    secrets=[modal.Secret.from_name("bausch")],
)
async def process_batch(urls_data: List[dict]) -> List[dict]:
    """Process batch with concurrency control via semaphore."""
    config = get_config()
    primary = get_primary_scraper()

    # Build priority string based on primary
    if primary == PRIMARY_BRIGHTDATA:
        priority_str = "Bright Data -> Firecrawl -> Playwright"
    else:
        priority_str = "Firecrawl -> Bright Data -> Playwright"

    print(f"\n{'='*70}")
    print(f"MODAL WEB SCRAPER v3.2 - OPTIMIZED BATCH")
    print(f"{'='*70}")
    print(f"Total URLs: {len(urls_data)}")
    print(f"Primary Scraper: {primary.upper()}")
    print(f"{'='*70}")
    print(f"CONFIGURATION (optimized for {primary.upper()}):")
    print(f"  Max Concurrency: {config['max_concurrency']}")
    print(f"  Max Containers:  {config['max_containers']}")
    print(f"  Max Inputs/Container: {config['max_inputs']}")
    print(f"  Theoretical Capacity: {config['max_containers'] * config['max_inputs']}")
    print(f"{'='*70}")
    print(f"Scraping Priority: {priority_str}")
    print(f"{'='*70}\n")

    start_time = time.time()

    # Use semaphore to control concurrency (respects API limits)
    semaphore = asyncio.Semaphore(config["max_concurrency"])

    async def scrape_with_semaphore(url_id: str, url: str, method: Optional[str] = None) -> dict:
        async with semaphore:
            return await scrape_url.remote.aio(url_id, url, method)

    # Create all tasks (pass method if specified in the URL item)
    tasks = [
        scrape_with_semaphore(item["urlId"], item["url"], item.get("method"))
        for item in urls_data
    ]

    # Run all tasks concurrently (semaphore limits actual concurrency)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle any exceptions
    processed_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            processed_results.append({
                "urlId": urls_data[i]["urlId"],
                "productUrl": urls_data[i]["url"],
                "status": "error",
                "errorMessage": str(r)[:200],
                "scrapedAt": int(time.time() * 1000)
            })
        else:
            processed_results.append(r)

    elapsed = time.time() - start_time

    # Stats
    successful = sum(1 for r in processed_results if r.get("status") == "completed")
    failed = len(processed_results) - successful
    with_screenshots = sum(1 for r in processed_results if r.get("screenshotUrl"))
    via_fc = sum(1 for r in processed_results if r.get("method") == METHOD_FIRECRAWL)
    via_bd = sum(1 for r in processed_results if r.get("method") == METHOD_BRIGHTDATA)
    via_pw = sum(1 for r in processed_results if r.get("method") == METHOD_PLAYWRIGHT)

    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE")
    print(f"{'='*60}")
    print(f"Total: {len(processed_results)} | Success: {successful} | Failed: {failed}")
    print(f"Screenshots: {with_screenshots}")
    print(f"Methods: Firecrawl={via_fc}, BrightData={via_bd}, Playwright={via_pw}")
    print(f"Time: {elapsed:.2f}s | Rate: {len(processed_results)/elapsed:.2f} URLs/sec")
    print(f"{'='*60}\n")

    return processed_results


# =============================================================================
# Local Entrypoint
# =============================================================================

@app.local_entrypoint()
def main(input_file: str = None, input_json: str = None):
    """Main entrypoint."""
    if input_file:
        with open(input_file, 'r') as f:
            data = json.load(f)
    elif input_json:
        data = json.loads(input_json)
    else:
        data = {"urls": [{"urlId": "test_001", "url": "https://www.amazon.com.br/dp/B0BDHWDR12"}]}

    urls_data = data.get("urls", [])
    if not urls_data:
        print("No URLs!")
        return

    print(f"\n{'='*60}")
    print(f"INPUT: {len(urls_data)} URLs")
    print(f"{'='*60}\n")

    # Run async batch
    results = process_batch.remote(urls_data)

    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}\n")

    for r in results:
        print(json.dumps(r, indent=2, ensure_ascii=False))
        print("-" * 40)

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"test-{timestamp}.json"

    config = get_config()
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            "processedAt": int(time.time() * 1000),
            "timestamp": timestamp,
            "primaryScraper": get_primary_scraper(),
            "config": {
                "maxConcurrency": config["max_concurrency"],
                "maxContainers": config["max_containers"],
                "maxInputsPerContainer": config["max_inputs"],
            },
            "totalUrls": len(urls_data),
            "successful": sum(1 for r in results if r.get("status") == "completed"),
            "failed": sum(1 for r in results if r.get("status") == "error"),
            "withScreenshots": sum(1 for r in results if r.get("screenshotUrl")),
            "viaFirecrawl": sum(1 for r in results if r.get("method") == METHOD_FIRECRAWL),
            "viaBrightData": sum(1 for r in results if r.get("method") == METHOD_BRIGHTDATA),
            "viaPlaywright": sum(1 for r in results if r.get("method") == METHOD_PLAYWRIGHT),
            "results": results
        }, f, ensure_ascii=False, indent=2)

    print(f"\nSaved: {output_file}")
