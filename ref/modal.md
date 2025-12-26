# Modal.com - Complete Reference Guide for Web Scraping at Scale

> **Document Created:** December 25, 2025
> **Purpose:** Reference documentation for implementing high-performance web scraping using Modal.com serverless infrastructure
> **Use Case:** Scrape 5,000+ URLs with 10 concurrent scrapes per instance, up to 100 instances

---

## Table of Contents

1. [Request Overview](#request-overview)
2. [Modal Configuration (Your Account)](#modal-configuration-your-account)
3. [Core Concepts](#core-concepts)
4. [Architecture for Web Scraping](#architecture-for-web-scraping)
5. [Scaling & Concurrency](#scaling--concurrency)
6. [Batch Processing Patterns](#batch-processing-patterns)
7. [Job Queue System](#job-queue-system)
8. [Container Images](#container-images)
9. [Secrets Management](#secrets-management)
10. [Web Endpoints](#web-endpoints)
11. [Volumes & Storage](#volumes--storage)
12. [Pricing & Limits](#pricing--limits)
13. [CLI Commands Reference](#cli-commands-reference)
14. [Complete Implementation Example](#complete-implementation-example)
15. [Best Practices](#best-practices)

---

## Request Overview

### Original Request

> "Research on how to use modal.com because we will use it to run our app. I want to run multiple instances to run as soon as possible the urls. Lets say we will have 5K urls to scrape. I want to run up to 10 scrapes per instance at once. So, the app will need to make math: with modal (i have the free version), it can have up to 100 instances running. So, it will need to split the total of scraping up to 100 instances."

### Requirements Summary

| Requirement | Value |
|-------------|-------|
| Total URLs to scrape | 5,000+ |
| Concurrent scrapes per instance | 10 |
| Maximum instances (free tier) | 100 |
| Maximum concurrent scrapes | 1,000 (100 × 10) |
| Execution model | Run locally, jobs execute on Modal |

### Math Examples

```
Example 1: 100 URLs
├── 100 ÷ 10 (concurrent per instance) = 10 instances
└── Each instance processes 10 URLs concurrently

Example 2: 2,000 URLs
├── 2,000 ÷ 100 (max instances) = 20 URLs per instance
├── Each instance handles 20 URLs
└── Processes 10 at a time in 2 batches

Example 3: 5,000 URLs
├── 5,000 ÷ 100 (max instances) = 50 URLs per instance
├── Each instance handles 50 URLs
└── Processes 10 at a time in 5 batches
```

---

## Modal Configuration (Your Account)

### Current Configuration (Auto-detected from CLI)

```bash
# Profile Information
Profile Name: elberrd
Workspace: elberrd
Environment: main
Server URL: https://api.modal.com

# Authentication
Token ID: ak-8g2lsPzV41jwXmFKmG1UgJ
Token Secret: as-QykbVaRpQIz02Fd2Knb2GK

# Existing Secrets
Secret Name: bausch (created 2025-12-25)
```

### Modal Configuration File Location

```
~/.modal.toml
```

### How to Use Your Secret in Code

```python
import modal

@app.function(secrets=[modal.Secret.from_name("bausch")])
def my_function():
    import os
    # Access any key stored in the "bausch" secret
    api_key = os.environ["YOUR_KEY_NAME"]
```

---

## Core Concepts

### What is Modal?

Modal is a serverless platform that lets you run Python code in the cloud without managing infrastructure. Key features:

- **Serverless containers**: Auto-scaling from 0 to thousands of containers
- **Pay-per-use**: Billed by CPU-second (no idle costs)
- **Custom environments**: Define container images with any dependencies
- **Distributed execution**: Run functions in parallel across containers

### Key Components

| Component | Description |
|-----------|-------------|
| `modal.App` | Application container, groups functions |
| `@app.function()` | Decorator to make a function run on Modal |
| `@modal.concurrent()` | Enable concurrent input processing |
| `modal.Image` | Container image with dependencies |
| `modal.Secret` | Secure environment variable storage |
| `modal.Volume` | Persistent distributed storage |

### Execution Models

```
┌─────────────────────────────────────────────────────────────────┐
│  LOCAL MACHINE                     MODAL CLOUD                  │
│  ┌─────────────┐                  ┌─────────────────────────┐   │
│  │ modal run   │ ───triggers───▶ │ Container 1: fn.remote() │  │
│  │ my_app.py   │                  │ Container 2: fn.remote() │  │
│  └─────────────┘                  │ Container N: fn.remote() │  │
│                                    └─────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Architecture for Web Scraping

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     LOCAL MACHINE (Orchestrator)                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  @app.local_entrypoint()                                    │ │
│  │  def main():                                                │ │
│  │      urls = load_urls()  # 5K URLs                          │ │
│  │      results = list(scrape_url.map(urls))  # Parallel exec  │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ Modal auto-distributes across containers
┌─────────────────────────────────────────────────────────────────┐
│                         MODAL CLOUD                              │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  @app.function(max_containers=100)                         │ │
│  │  @modal.concurrent(max_inputs=10)                          │ │
│  │  async def scrape_url(url: str) -> dict:                   │ │
│  │      # Scraping logic (httpx, playwright, etc.)            │ │
│  │      return scraped_data                                   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ┌──────────────┐ ┌──────────────┐     ┌──────────────┐         │
│  │ Container 1  │ │ Container 2  │ ... │ Container 100│         │
│  │ 10 concurrent│ │ 10 concurrent│     │ 10 concurrent│         │
│  │ scrapes      │ │ scrapes      │     │ scrapes      │         │
│  └──────────────┘ └──────────────┘     └──────────────┘         │
│                                                                  │
│  Total: Up to 1,000 concurrent scrapes (100 × 10)               │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
1. Local: Load 5,000 URLs from database/file
2. Local: Call scrape_url.map(urls)
3. Modal: Auto-distributes URLs across up to 100 containers
4. Modal: Each container processes 10 URLs concurrently
5. Modal: Returns results as they complete
6. Local: Collect and store results
```

---

## Scaling & Concurrency

### Autoscaling Behavior

Modal automatically scales containers based on demand:

- **Scale up**: New containers spin up when inputs queue
- **Scale down**: Idle containers terminate after `scaledown_window`
- **Scale to zero**: No containers = no cost when idle

### Configuration Parameters

```python
@app.function(
    # Scaling limits
    max_containers=100,      # Maximum parallel containers (free tier: 100)
    min_containers=0,        # Minimum warm containers (default: 0)
    buffer_containers=0,     # Extra containers for burst traffic

    # Idle timeout
    scaledown_window=60,     # Seconds before idle container terminates (max: 1200)

    # Execution limits
    timeout=300,             # Max execution time per input (default: 300s, max: 86400s)
    retries=3,               # Auto-retry failed inputs
)
```

### Concurrent Inputs per Container

```python
@app.function(max_containers=100)
@modal.concurrent(
    max_inputs=10,           # Max concurrent inputs per container
    target_inputs=8,         # Target concurrency (allows burst to max_inputs)
)
async def scrape_url(url: str) -> dict:
    # Async function for I/O-bound work (web scraping)
    pass
```

**Concurrency Mechanisms:**

| Function Type | Mechanism | Requirements |
|---------------|-----------|--------------|
| `async def` | asyncio tasks | Don't block event loop |
| `def` (sync) | Python threads | Must be thread-safe |

### Scaling Limits (Free Tier)

| Limit | Value |
|-------|-------|
| Max containers per function | 100 |
| Max pending inputs | 2,000 |
| Max total inputs (pending + running) | 25,000 |
| Max inputs per `.map()` call | 1,000 concurrent |
| Rate limit | 200 inputs/sec (5s burst) |

---

## Batch Processing Patterns

### Pattern 1: Synchronous Map (Recommended for Your Use Case)

```python
@app.function(max_containers=100)
@modal.concurrent(max_inputs=10)
async def scrape_url(url: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return {"url": url, "status": response.status_code}

@app.local_entrypoint()
def main():
    urls = ["https://example1.com", "https://example2.com", ...]  # 5K URLs

    # Process all URLs in parallel, collect results
    results = list(scrape_url.map(urls))

    # Save results
    for result in results:
        save_to_database(result)
```

**Behavior:**
- `.map()` distributes URLs across containers
- Results stream back as they complete
- Blocks until all URLs processed

### Pattern 2: Background Processing (Fire-and-Forget)

```python
@app.local_entrypoint()
def main():
    urls = load_urls()  # 5K URLs

    # Submit all URLs, don't wait for results
    scrape_url.spawn_map(urls)

    print("Jobs submitted! Check Modal dashboard for progress.")
```

**Behavior:**
- Returns immediately after submission
- Results stored externally (database, volume)
- Check progress via Modal dashboard

### Pattern 3: Async Job Queue with Status Polling

```python
@app.function()
def scrape_url(url: str) -> dict:
    return {"url": url, "data": "..."}

def submit_job(url: str) -> str:
    """Submit a single URL and return job ID"""
    scrape_fn = modal.Function.from_name("my-app", "scrape_url")
    call = scrape_fn.spawn(url)
    return call.object_id

def get_job_result(call_id: str) -> dict:
    """Poll for job result"""
    function_call = modal.FunctionCall.from_id(call_id)
    try:
        return function_call.get(timeout=5)
    except TimeoutError:
        return {"status": "pending"}
```

### Pattern 4: Starmap for Multiple Arguments

```python
@app.function()
@modal.concurrent(max_inputs=10)
async def scrape_with_config(url: str, timeout: int, proxy: str) -> dict:
    # Scraping logic with custom config
    pass

@app.local_entrypoint()
def main():
    # List of (url, timeout, proxy) tuples
    inputs = [
        ("https://site1.com", 30, "proxy1"),
        ("https://site2.com", 60, "proxy2"),
    ]

    # Starmap unpacks tuples as arguments
    results = list(scrape_with_config.starmap(inputs))
```

---

## Job Queue System

### Creating Async Jobs with `.spawn()`

```python
import modal

# Submit job
scrape_fn = modal.Function.from_name("my-scraper-app", "scrape_url")
call = scrape_fn.spawn("https://example.com")
job_id = call.object_id

# Later: retrieve result
function_call = modal.FunctionCall.from_id(job_id)
result = function_call.get(timeout=60)  # Wait up to 60s
```

### Job Lifecycle

```
spawn() ──▶ PENDING ──▶ RUNNING ──▶ COMPLETED
                │              │
                └──▶ FAILED ◀──┘
```

### Integration with FastAPI

```python
@app.function()
@modal.asgi_app()
def api():
    from fastapi import FastAPI

    web_app = FastAPI()

    @web_app.post("/scrape")
    async def submit_scrape(url: str):
        scrape_fn = modal.Function.from_name("my-app", "scrape_url")
        call = await scrape_fn.spawn.aio(url)
        return {"job_id": call.object_id}

    @web_app.get("/result/{job_id}")
    async def get_result(job_id: str):
        call = modal.FunctionCall.from_id(job_id)
        try:
            result = await call.get.aio(timeout=0)
            return result
        except TimeoutError:
            return {"status": "pending"}

    return web_app
```

---

## Container Images

### Basic Image with Scraping Dependencies

```python
import modal

# Lightweight image for HTTP-based scraping
scraper_image = (
    modal.Image.debian_slim(python_version="3.11")
    .uv_pip_install(
        "httpx>=0.25.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
    )
)

# Heavyweight image for JavaScript rendering
playwright_image = (
    modal.Image.debian_slim(python_version="3.11")
    .run_commands(
        "apt-get update",
        "apt-get install -y software-properties-common",
        "apt-add-repository non-free",
        "apt-add-repository contrib",
    )
    .uv_pip_install("playwright==1.42.0")
    .run_commands(
        "playwright install-deps chromium",
        "playwright install chromium",
    )
)
```

### Image Best Practices

1. **Use `uv_pip_install`** over `pip_install` (faster)
2. **Pin versions** for reproducibility
3. **Order layers by change frequency** (static first)
4. **Use `force_build=True`** to rebuild when needed

### Adding Local Code

```python
image = (
    modal.Image.debian_slim()
    .add_local_python_source("my_module")  # Import local module
    .add_local_dir("./config", remote_path="/app/config")  # Copy files
)
```

---

## Secrets Management

### Your Configured Secret

```
Secret Name: bausch
Created: 2025-12-25
```

### Using Secrets in Code

```python
@app.function(secrets=[modal.Secret.from_name("bausch")])
def my_function():
    import os

    # Access secret values as environment variables
    api_key = os.environ["FIRECRAWL_API_KEY"]
    proxy_url = os.environ["WEBSHARE_PROXY_URL"]
```

### Creating Secrets via CLI

```bash
# Create new secret with key-value pairs
modal secret create my-secret \
    FIRECRAWL_API_KEY="fc-xxxxx" \
    WEBSHARE_PROXY_URL="http://user:pass@proxy.webshare.io:80"

# List secrets
modal secret list

# Delete secret
modal secret delete my-secret
```

### Creating Secrets from .env File

```python
# In your Modal app
@app.function(secrets=[modal.Secret.from_dotenv()])
def my_function():
    import os
    api_key = os.environ["MY_KEY"]
```

### Multiple Secrets

```python
@app.function(
    secrets=[
        modal.Secret.from_name("bausch"),
        modal.Secret.from_name("other-secret"),
    ]
)
def my_function():
    # Later secrets override earlier ones for duplicate keys
    pass
```

---

## Web Endpoints

### Simple FastAPI Endpoint

```python
image = modal.Image.debian_slim().pip_install("fastapi[standard]")

@app.function(image=image)
@modal.fastapi_endpoint()
def hello(name: str = "World"):
    return {"message": f"Hello, {name}!"}
```

### Full ASGI App

```python
@app.function(image=image)
@modal.concurrent(max_inputs=100)
@modal.asgi_app()
def web_app():
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/")
    async def root():
        return {"status": "ok"}

    @app.post("/scrape")
    async def scrape(urls: list[str]):
        scrape_fn = modal.Function.from_name("my-app", "scrape_url")
        results = list(scrape_fn.map(urls))
        return {"results": results}

    return app
```

### Development vs Production

```bash
# Development (hot-reload, temporary URL)
modal serve my_app.py

# Production (persistent URL)
modal deploy my_app.py
```

---

## Volumes & Storage

### Creating a Volume

```bash
modal volume create scraper-results
```

### Using Volumes

```python
results_volume = modal.Volume.from_name("scraper-results", create_if_missing=True)

@app.function(volumes={"/data": results_volume})
def scrape_and_save(url: str):
    result = scrape(url)

    # Write to volume
    with open(f"/data/{hash(url)}.json", "w") as f:
        json.dump(result, f)

    # IMPORTANT: Commit changes before exit
    results_volume.commit()
```

### Volume Limits

| Feature | v1 Limit | v2 Limit |
|---------|----------|----------|
| Max files | 500,000 | Unlimited |
| Concurrent writers | 5 | Hundreds |
| Max file size | Unlimited | 1 TiB |

---

## Pricing & Limits

### Free Tier (Starter Plan)

| Resource | Limit |
|----------|-------|
| Monthly credits | $30 |
| Containers | 100 |
| GPU concurrency | 10 |
| Workspace seats | 3 |
| Deployed crons | 5 |
| Deployed web endpoints | 8 |
| Log retention | 1 day |

### Compute Costs (Per Second)

| Resource | Cost |
|----------|------|
| CPU (per core) | $0.0000131 |
| Memory (per GiB) | $0.00000222 |
| T4 GPU | $0.000164 |
| A10 GPU | $0.000306 |
| L4 GPU | $0.000222 |
| L40S GPU | $0.000542 |
| A100 40GB | $0.000583 |
| A100 80GB | $0.000694 |
| H100 | $0.001097 |

### Cost Estimation for Web Scraping

```
Scenario: 5,000 URLs, ~5s per URL average

Without concurrency:
- 5,000 × 5s = 25,000 seconds total
- Single container CPU time

With concurrency (100 containers × 10 concurrent):
- 5,000 ÷ 1,000 = 5 batches
- 5 batches × 5s = ~25s wall-clock time
- 100 containers × 25s = 2,500 container-seconds

Estimated cost (assuming 0.25 cores per container):
- 2,500s × 0.25 cores × $0.0000131 = $0.0082
- Plus memory: ~negligible for basic scraping

Total: ~$0.01 per 5,000 URLs (well within $30 free tier)
```

---

## CLI Commands Reference

### Essential Commands

```bash
# Run ephemeral app
modal run my_app.py

# Run with specific entrypoint
modal run my_app.py::my_function

# Hot-reload development server
modal serve my_app.py

# Deploy persistent app
modal deploy my_app.py

# Interactive shell in container
modal shell --image python:3.11

# Shell with volume mounted
modal shell --volume scraper-results
```

### App Management

```bash
# List apps
modal app list

# Stop deployed app
modal app stop my-app

# View app logs (requires app ID)
# Best viewed in Modal dashboard
```

### Secret Management

```bash
# List secrets
modal secret list

# Create secret
modal secret create my-secret KEY1=value1 KEY2=value2

# Delete secret
modal secret delete my-secret
```

### Volume Management

```bash
# Create volume
modal volume create my-volume

# List volumes
modal volume list

# Upload file
modal volume put my-volume local_file.txt /remote/path/

# Download file
modal volume get my-volume /remote/file.txt ./local_file.txt

# List files
modal volume ls my-volume /path/

# Delete file
modal volume rm my-volume /path/to/file
```

### Configuration

```bash
# Show current config
modal config show

# List profiles
modal profile list

# Switch profile
modal profile activate profile-name

# List environments
modal environment list
```

---

## Complete Implementation Example

### File: `scraper_modal.py`

```python
"""
Modal-based Web Scraper
=======================
Scrapes 5K+ URLs with 10 concurrent scrapes per instance,
up to 100 instances (free tier limit).

Usage:
    modal run scraper_modal.py --urls-file urls.txt
    modal deploy scraper_modal.py
"""

import modal
import asyncio
from typing import Optional

# =============================================================================
# Modal App Configuration
# =============================================================================

app = modal.App("web-scraper")

# Container image with scraping dependencies
scraper_image = (
    modal.Image.debian_slim(python_version="3.11")
    .uv_pip_install(
        "httpx>=0.25.0",
        "beautifulsoup4>=4.12.0",
        "lxml>=4.9.0",
    )
)

# Optional: Volume for storing results
results_volume = modal.Volume.from_name("scraper-results", create_if_missing=True)

# =============================================================================
# Scraping Function
# =============================================================================

@app.function(
    image=scraper_image,
    max_containers=100,          # Free tier limit
    timeout=300,                 # 5 min per URL
    retries=3,                   # Auto-retry on failure
    secrets=[modal.Secret.from_name("bausch")],  # Your secrets
    volumes={"/results": results_volume},         # Optional: persistent storage
)
@modal.concurrent(max_inputs=10)  # 10 concurrent scrapes per container
async def scrape_url(url: str) -> dict:
    """
    Scrape a single URL and return extracted data.

    Args:
        url: The URL to scrape

    Returns:
        Dictionary with scraped data
    """
    import httpx
    from bs4 import BeautifulSoup
    import os
    import json
    from datetime import datetime

    # Access secrets if needed
    # proxy_url = os.environ.get("WEBSHARE_PROXY_URL")

    result = {
        "url": url,
        "scraped_at": datetime.utcnow().isoformat(),
        "success": False,
        "error": None,
        "data": None,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            result["success"] = True
            result["data"] = {
                "title": soup.title.string if soup.title else None,
                "status_code": response.status_code,
                "content_length": len(response.text),
                "links_count": len(soup.find_all("a")),
            }

    except Exception as e:
        result["error"] = str(e)

    # Optional: Save to volume
    # url_hash = hash(url) & 0xFFFFFFFF
    # with open(f"/results/{url_hash}.json", "w") as f:
    #     json.dump(result, f)
    # results_volume.commit()

    return result


# =============================================================================
# Batch Processing Function
# =============================================================================

@app.function(
    image=scraper_image,
    timeout=3600,  # 1 hour for batch processing
)
def process_batch(urls: list[str]) -> list[dict]:
    """
    Process a batch of URLs.
    Called when you want to split work manually.
    """
    results = list(scrape_url.map(urls))
    return results


# =============================================================================
# Local Entrypoint
# =============================================================================

@app.local_entrypoint()
def main(
    urls_file: str = None,
    max_urls: int = 5000,
):
    """
    Main entrypoint for running the scraper.

    Args:
        urls_file: Path to file with URLs (one per line)
        max_urls: Maximum URLs to process
    """
    import json
    from datetime import datetime

    # Load URLs
    if urls_file:
        with open(urls_file) as f:
            urls = [line.strip() for line in f if line.strip()]
    else:
        # Example URLs for testing
        urls = [
            "https://example.com",
            "https://httpbin.org/get",
            "https://jsonplaceholder.typicode.com/posts/1",
        ]

    # Limit URLs
    urls = urls[:max_urls]

    print(f"Starting scrape of {len(urls)} URLs...")
    print(f"Max containers: 100")
    print(f"Concurrent per container: 10")
    print(f"Max concurrent scrapes: 1000")
    print()

    start_time = datetime.now()

    # Process all URLs in parallel
    results = list(scrape_url.map(urls))

    elapsed = (datetime.now() - start_time).total_seconds()

    # Summary
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    print()
    print(f"Completed in {elapsed:.2f} seconds")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Rate: {len(results)/elapsed:.1f} URLs/second")

    # Save results
    output_file = "scrape_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {output_file}")


# =============================================================================
# Web API (Optional)
# =============================================================================

api_image = scraper_image.pip_install("fastapi[standard]")

@app.function(image=api_image)
@modal.concurrent(max_inputs=100)
@modal.asgi_app()
def api():
    """
    REST API for submitting scrape jobs.

    Endpoints:
        POST /scrape - Submit URLs for scraping
        GET /health - Health check
    """
    from fastapi import FastAPI, BackgroundTasks
    from pydantic import BaseModel

    web_app = FastAPI(title="Web Scraper API")

    class ScrapeRequest(BaseModel):
        urls: list[str]

    @web_app.get("/health")
    async def health():
        return {"status": "ok"}

    @web_app.post("/scrape")
    async def scrape(request: ScrapeRequest):
        results = list(scrape_url.map(request.urls))
        return {"results": results}

    @web_app.post("/scrape/async")
    async def scrape_async(request: ScrapeRequest):
        # Fire-and-forget
        scrape_url.spawn_map(request.urls)
        return {"status": "submitted", "count": len(request.urls)}

    return web_app
```

### Usage

```bash
# Test with example URLs
modal run scraper_modal.py

# Scrape from file
modal run scraper_modal.py --urls-file urls.txt --max-urls 1000

# Deploy for production
modal deploy scraper_modal.py

# Run web API
modal serve scraper_modal.py  # Development
modal deploy scraper_modal.py  # Production
```

---

## Best Practices

### 1. Use Async for I/O-Bound Work

```python
# GOOD: Async for web scraping
@modal.concurrent(max_inputs=10)
async def scrape_url(url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.text

# BAD: Sync blocks the event loop
@modal.concurrent(max_inputs=10)
def scrape_url(url: str):
    response = requests.get(url)  # Blocks!
    return response.text
```

### 2. Handle Errors Gracefully

```python
@app.function(retries=3)
async def scrape_url(url: str):
    try:
        result = await do_scrape(url)
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}
```

### 3. Use Timeouts

```python
@app.function(timeout=300)  # 5 min max
async def scrape_url(url: str):
    async with httpx.AsyncClient(timeout=30.0) as client:  # Per-request timeout
        response = await client.get(url)
```

### 4. Batch Commits to Volumes

```python
# BAD: Commit after every write
for url in urls:
    save_to_volume(url)
    volume.commit()  # Slow!

# GOOD: Batch commits
for url in urls:
    save_to_volume(url)
volume.commit()  # Once at end
```

### 5. Monitor Progress

```python
@app.local_entrypoint()
def main():
    from tqdm import tqdm

    urls = load_urls()
    results = []

    for result in tqdm(scrape_url.map(urls), total=len(urls)):
        results.append(result)
```

### 6. Use Return Values for Small Data

```python
# GOOD: Return data directly for small results
results = list(scrape_url.map(urls))

# For large data, use volumes or external storage
```

---

## References

- **Modal Documentation:** https://modal.com/docs/guide
- **Modal Examples:** https://modal.com/docs/examples
- **Modal API Reference:** https://modal.com/docs/reference
- **Modal Pricing:** https://modal.com/pricing
- **Modal Dashboard:** https://modal.com/apps

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────────┐
│                    MODAL QUICK REFERENCE                         │
├─────────────────────────────────────────────────────────────────┤
│ COMMANDS                                                         │
│   modal run app.py          Run ephemeral app                   │
│   modal serve app.py        Hot-reload dev server               │
│   modal deploy app.py       Deploy persistent app               │
│   modal secret list         List secrets                        │
│   modal volume list         List volumes                        │
├─────────────────────────────────────────────────────────────────┤
│ DECORATORS                                                       │
│   @app.function()           Make function run on Modal          │
│   @modal.concurrent(N)      N concurrent inputs per container   │
│   @app.local_entrypoint()   Local CLI entrypoint                │
│   @modal.asgi_app()         Serve ASGI web app                  │
├─────────────────────────────────────────────────────────────────┤
│ EXECUTION PATTERNS                                               │
│   fn.remote(arg)            Single remote call                  │
│   fn.map(iterable)          Parallel calls, collect results     │
│   fn.spawn(arg)             Async call, returns job ID          │
│   fn.spawn_map(iterable)    Async parallel, fire-and-forget     │
├─────────────────────────────────────────────────────────────────┤
│ KEY PARAMETERS                                                   │
│   max_containers=100        Limit parallel containers           │
│   timeout=300               Max execution time (seconds)        │
│   retries=3                 Auto-retry on failure               │
│   secrets=[...]             Inject secrets as env vars          │
│   volumes={path: vol}       Mount persistent storage            │
└─────────────────────────────────────────────────────────────────┘
```
