#!/usr/bin/env python3
"""
Standalone Bright Data Screenshot Script
Captures screenshots using Bright Data API.
Includes image compression using Pillow.
"""

import base64
import os
import time
import random
from io import BytesIO
from typing import Optional

import requests
from PIL import Image


TARGET_URL = "https://www.saojoaofarmacias.com.br/trealens-solucao-para-lentes-120ml---360ml-lebon-10036412/p"

CONNECT_TIMEOUT = 30
READ_TIMEOUT = 150
MAX_RETRIES = 4
JPEG_QUALITY = 85


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


def calculate_backoff_with_jitter(attempt: int) -> float:
    """Calculate backoff with jitter for retries."""
    base_delay = 2.0
    max_delay = 60.0
    
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0.5, 1.5)
    return delay * jitter


def take_screenshot_with_bright_data(url: str) -> Optional[bytes]:
    """Take screenshot using Bright Data API with retries."""
    api_key, zone = get_bright_data_config()
    
    print(f"\n📸 BRIGHT DATA SCREENSHOT: Starting")
    print(f"   • Target URL: {url}")
    print(f"   • Zone: {zone}")
    print(f"   • Timeout: {CONNECT_TIMEOUT}s connect, {READ_TIMEOUT}s read")
    print(f"   • Max retries: {MAX_RETRIES}")
    
    for attempt in range(MAX_RETRIES + 1):
        if attempt > 0:
            wait_time = calculate_backoff_with_jitter(attempt)
            print(f"   • Retry attempt {attempt}/{MAX_RETRIES} after {wait_time:.2f}s delay...")
            time.sleep(wait_time)
        
        try:
            api_url = "https://api.brightdata.com/request"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            payload = {
                "zone": zone,
                "url": url,
                "format": "raw",
                "data_format": "screenshot"
            }
            
            print(f"   • Making request to Bright Data API (attempt {attempt + 1})...")
            
            start_time = time.time()
            response = requests.post(
                api_url,
                headers=headers,
                json=payload,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                screenshot_data = response.content
                
                if len(screenshot_data) > 1000:
                    print(f"✅ Screenshot captured in {elapsed:.2f}s")
                    print(f"   • Size: {len(screenshot_data):,} bytes")
                    return screenshot_data
                else:
                    print(f"⚠️ Response too small ({len(screenshot_data)} bytes), retrying...")
            else:
                print(f"❌ API error: Status {response.status_code}")
                print(f"   • Response: {response.text[:200] if response.text else 'No response'}")
                
        except requests.exceptions.Timeout as e:
            print(f"⚠️ Timeout error (attempt {attempt + 1}): {str(e)}")
        except requests.exceptions.ConnectionError as e:
            print(f"⚠️ Connection error (attempt {attempt + 1}): {str(e)}")
        except Exception as e:
            print(f"❌ Unexpected error (attempt {attempt + 1}): {str(e)}")
    
    print("❌ All retry attempts failed")
    return None


def compress_image(image_bytes: bytes, quality: int = 85) -> tuple[bytes, dict]:
    """Compress PNG image to optimized JPEG."""
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


def main():
    """Main function to run the Bright Data screenshot capture."""
    print("=" * 60)
    print("BRIGHT DATA SCREENSHOT - STANDALONE SCRIPT")
    print("=" * 60)
    
    total_start = time.time()
    
    screenshot_bytes = take_screenshot_with_bright_data(TARGET_URL)
    
    if screenshot_bytes:
        with open('test-screen-original.png', 'wb') as f:
            f.write(screenshot_bytes)
        
        compressed_bytes, stats = compress_image(screenshot_bytes, JPEG_QUALITY)
        
        with open('test-screen.jpg', 'wb') as f:
            f.write(compressed_bytes)
        
        screenshot_base64 = base64.b64encode(screenshot_bytes).decode('utf-8')
        
        total_elapsed = time.time() - total_start
        
        print(f"\n📦 Compression applied:")
        print(f"   Original: {stats['original_size']:,} bytes ({stats['original_size']/1024:.1f} KB)")
        print(f"   Compressed: {stats['compressed_size']:,} bytes ({stats['compressed_size']/1024:.1f} KB)")
        print(f"   Reduction: {stats['reduction_percent']:.1f}%")
        print(f"\n📁 Files saved:")
        print(f"   - test-screen-original.png ({stats['original_size']/1024:.1f} KB)")
        print(f"   - test-screen.jpg ({stats['compressed_size']/1024:.1f} KB)")
        print(f"\n⏱️ Total time: {total_elapsed:.2f} seconds")
        print(f"📄 Base64 length: {len(screenshot_base64):,} characters")
        
    else:
        print("\n❌ Failed to capture screenshot")


if __name__ == "__main__":
    main()
