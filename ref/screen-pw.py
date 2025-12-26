#!/usr/bin/env python3
"""
Standalone Playwright Screenshot Script
Captures full-page screenshots using headless Chromium with Webshare proxy.
Includes image compression using Pillow.
"""

import asyncio
import base64
import os
import time
from io import BytesIO
from urllib.parse import urlparse, unquote

from playwright.async_api import async_playwright
from PIL import Image


CHROMIUM_PATH = "/nix/store/qa9cnw4v5xkxyip6mb9kxqfq1z4x2dx1-chromium-138.0.7204.100/bin/chromium"

TARGET_URL = "https://www.saojoaofarmacias.com.br/trealens-solucao-para-lentes-120ml---360ml-lebon-10036412/p"

WAIT_TIME_MS = 5000
FULL_PAGE = True
JPEG_QUALITY = 85


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


async def take_screenshot(url: str, wait_time: int = 5000, full_page: bool = True) -> bytes | None:
    """
    Take a screenshot of a URL using Playwright headless browser with proxy.
    
    Args:
        url: The URL to screenshot
        wait_time: Time to wait for page to load in milliseconds (default 5000ms)
        full_page: Whether to capture full page or viewport only (default True)
        
    Returns:
        Binary data of the screenshot (PNG format) or None if failed
    """
    print(f"\n📸 PLAYWRIGHT SCREENSHOT: Starting screenshot capture")
    print(f"   • Target URL: {url}")
    print(f"   • Wait time: {wait_time}ms")
    print(f"   • Full page: {full_page}")
    
    proxy_config = get_proxy_config()
    start_time = time.time()
    
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
            
            print("📷 Taking screenshot...")
            screenshot_bytes = await page.screenshot(
                type='png',
                full_page=full_page
            )
            
            await context.close()
            await browser.close()
            
            total_time = time.time() - start_time
            print(f"✅ SCREENSHOT COMPLETED in {total_time:.2f}s")
            print(f"   • Screenshot size: {len(screenshot_bytes):,} bytes")
            
            return screenshot_bytes
            
    except Exception as e:
        total_time = time.time() - start_time
        print(f"❌ Screenshot error after {total_time:.2f}s: {str(e)}")
        return None


def compress_image(image_bytes: bytes, quality: int = 85) -> tuple[bytes, dict]:
    """
    Compress PNG image to optimized JPEG.
    
    Args:
        image_bytes: Original PNG image bytes
        quality: JPEG quality (1-100, default 85)
        
    Returns:
        Tuple of (compressed bytes, stats dict)
    """
    original_size = len(image_bytes)
    
    img = Image.open(BytesIO(image_bytes))
    
    if img.mode in ('RGBA', 'P'):
        img = img.convert('RGB')
    
    output = BytesIO()
    img.save(output, format='JPEG', quality=quality, optimize=True)
    compressed_bytes = output.getvalue()
    compressed_size = len(compressed_bytes)
    
    reduction = ((original_size - compressed_size) / original_size) * 100
    
    stats = {
        'original_size': original_size,
        'compressed_size': compressed_size,
        'reduction_percent': reduction,
        'quality': quality
    }
    
    return compressed_bytes, stats


async def main():
    """Main function to run the screenshot capture."""
    print("=" * 60)
    print("PLAYWRIGHT SCREENSHOT - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    screenshot_bytes = await take_screenshot(
        url=TARGET_URL,
        wait_time=WAIT_TIME_MS,
        full_page=FULL_PAGE
    )
    
    if screenshot_bytes:
        with open('test-screen-original.png', 'wb') as f:
            f.write(screenshot_bytes)
        
        compressed_bytes, stats = compress_image(screenshot_bytes, JPEG_QUALITY)
        
        with open('test-screen.jpg', 'wb') as f:
            f.write(compressed_bytes)
        
        screenshot_base64 = base64.b64encode(screenshot_bytes).decode('utf-8')
        
        total_elapsed = time.time() - total_start
        
        print(f"\n📦 Compressão aplicada:")
        print(f"   Tamanho original: {stats['original_size']:,} bytes ({stats['original_size']/1024:.1f} KB)")
        print(f"   Tamanho comprimido: {stats['compressed_size']:,} bytes ({stats['compressed_size']/1024:.1f} KB)")
        print(f"   Redução: {stats['reduction_percent']:.1f}%")
        print(f"\n📁 Arquivos salvos:")
        print(f"   - test-screen-original.png ({stats['original_size']/1024:.1f} KB)")
        print(f"   - test-screen.jpg ({stats['compressed_size']/1024:.1f} KB)")
        print(f"\n⏱️ Tempo total: {total_elapsed:.2f} segundos")
        print(f"📄 Base64 length: {len(screenshot_base64):,} caracteres")
        
    else:
        print("\n❌ Falha ao capturar screenshot")


if __name__ == "__main__":
    asyncio.run(main())
