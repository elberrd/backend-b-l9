#!/usr/bin/env python3
"""
Quick test script for Firecrawl screenshot.
"""

import os
import base64
from dotenv import load_dotenv
from firecrawl import FirecrawlApp

# Load environment variables
load_dotenv()

TARGET_URL = "https://www.drogaraia.com.br/biotrue-bausch-lomb-300ml-120ml-estojo.html?origin=search"
OUTPUT_FILE = "teste-firecrawl.png"


def main():
    print("=" * 60)
    print("FIRECRAWL SCREENSHOT TEST")
    print("=" * 60)

    api_key = os.environ.get("FIRECRAWL_API_KEY")

    if not api_key:
        print("ERROR: FIRECRAWL_API_KEY not set in .env")
        return

    print(f"API Key: {api_key[:10]}...")
    print(f"Target: {TARGET_URL}")
    print("=" * 60)

    print("\n[1/3] Initializing Firecrawl...")
    firecrawl = FirecrawlApp(api_key=api_key)

    print("[2/3] Scraping with screenshot...")
    result = firecrawl.scrape(
        TARGET_URL,
        formats=["screenshot", "markdown"],
        wait_for=3000,  # 3 seconds wait
        timeout=60000,  # 60 seconds timeout
    )

    print("[3/3] Processing result...")

    if result:
        # Debug: print all attributes
        print(f"\nResult attributes: {[a for a in dir(result) if not a.startswith('_')]}")

        # Access attributes directly (Document object)
        screenshot_data = getattr(result, 'screenshot', None)
        markdown = getattr(result, 'markdown', '')

        print(f"\nScreenshot data type: {type(screenshot_data)}")
        if screenshot_data:
            print(f"Screenshot data (first 100 chars): {str(screenshot_data)[:100]}")

        if screenshot_data:
            # Check if it's a URL or base64
            if screenshot_data.startswith("http"):
                # Download from URL
                import requests
                print(f"\nDownloading screenshot from URL...")
                response = requests.get(screenshot_data, timeout=30)
                if response.status_code == 200:
                    image_bytes = response.content
                    with open(OUTPUT_FILE, "wb") as f:
                        f.write(image_bytes)
                    print(f"Screenshot saved: {OUTPUT_FILE}")
                    print(f"File size: {len(image_bytes):,} bytes")
                else:
                    print(f"Failed to download: {response.status_code}")
            elif screenshot_data.startswith("data:image"):
                # Decode base64 data URL
                screenshot_data = screenshot_data.split(",", 1)[1]
                image_bytes = base64.b64decode(screenshot_data)
                with open(OUTPUT_FILE, "wb") as f:
                    f.write(image_bytes)
                print(f"\nScreenshot saved: {OUTPUT_FILE}")
                print(f"File size: {len(image_bytes):,} bytes")
            else:
                # Try base64 decode
                image_bytes = base64.b64decode(screenshot_data)
                with open(OUTPUT_FILE, "wb") as f:
                    f.write(image_bytes)
                print(f"\nScreenshot saved: {OUTPUT_FILE}")
                print(f"File size: {len(image_bytes):,} bytes")
        else:
            print("\nNo screenshot in response")

        if markdown:
            print(f"\nMarkdown length: {len(markdown):,} chars")
            # Find product info
            if "Biotrue" in markdown or "biotrue" in markdown.lower():
                print("\nProduct found in content!")
            print(f"\nFirst 500 chars:\n{markdown[:500]}")
    else:
        print("\nNo result returned")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
