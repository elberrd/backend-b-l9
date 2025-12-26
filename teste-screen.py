#!/usr/bin/env python3
"""
Quick test script for Playwright + Webshare proxy screenshot.
"""

import asyncio
import os
from urllib.parse import urlparse, unquote

from dotenv import load_dotenv
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# Load environment variables
load_dotenv()

TARGET_URL = "https://www.drogaraia.com.br/biotrue-bausch-lomb-300ml-120ml-estojo.html?origin=search"
OUTPUT_FILE = "teste-screen.png"


async def main():
    print("=" * 60)
    print("PLAYWRIGHT + WEBSHARE PROXY TEST")
    print("=" * 60)

    # Get proxy config
    proxy_url = os.environ.get("WEBSHARE_PROXY_URL")

    if not proxy_url:
        print("ERROR: WEBSHARE_PROXY_URL not set in .env")
        return

    parsed = urlparse(proxy_url)
    proxy_config = {
        "server": f"http://{parsed.hostname}:{parsed.port or 80}",
        "username": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
    }

    print(f"Proxy: {parsed.hostname}:{parsed.port}")
    print(f"Target: {TARGET_URL}")
    print("=" * 60)

    async with async_playwright() as p:
        print("\n[1/5] Launching browser...")
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
            ]
        )

        print("[2/5] Creating context with proxy...")
        context = await browser.new_context(
            proxy=proxy_config,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            locale='pt-BR',
            timezone_id='America/Sao_Paulo',
        )

        print("[3/5] Applying stealth mode...")
        stealth = Stealth()
        await stealth.apply_stealth_async(context)

        page = await context.new_page()

        # Auto-dismiss dialogs
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.dismiss()))

        print("[4/5] Navigating to page...")
        try:
            await page.goto(TARGET_URL, wait_until='domcontentloaded', timeout=60000)
        except Exception as e:
            print(f"Navigation warning: {str(e)[:100]}")

        # Wait for content
        await asyncio.sleep(5)

        try:
            await page.wait_for_load_state('networkidle', timeout=15000)
        except Exception:
            pass

        # Get HTML size
        html = await page.content()
        print(f"HTML size: {len(html):,} bytes")

        if len(html) < 5000:
            print("WARNING: HTML too small - likely blocked!")
            print(f"Page title: {await page.title()}")

        print(f"[5/5] Taking screenshot...")
        await page.screenshot(path=OUTPUT_FILE, full_page=True)

        await context.close()
        await browser.close()

        print("=" * 60)
        print(f"Screenshot saved: {OUTPUT_FILE}")
        print(f"File size: {os.path.getsize(OUTPUT_FILE):,} bytes")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
