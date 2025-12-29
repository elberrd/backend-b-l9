"""
Modal-based Web Scraper v3.3 (With Initial Data Override)
==========================================================
Scalable web scraper using Modal.com infrastructure with 3-tier fallback system.
Fully async implementation with optimized container/concurrency configuration.

NEW IN v3.3: Initial Data Override
----------------------------------
Input JSON can include additional fields that will OVERRIDE scraped data:
- companyName, alertId, hasAlert, screenshotId (metadata fields)
- productTitle, seller, brand, etc. (product fields)

This ensures user-provided metadata is preserved in the final Tinybird output.
The URL can be specified as either 'url' or 'productUrl'.

Example Input:
    {
        "urls": [
            {
                "urlId": "test_001",
                "url": "https://example.com/product",
                "companyName": "My Company",       // Will override scraped data
                "alertId": "alert_123",            // Will be added to output
                "productTitle": "Custom Title",   // Will override scraped title
                "method": "brightdata"            // Force specific scraper
            }
        ]
    }

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
- TINYBIRD_TOKEN: Token for Tinybird Events API (sends scrape results to database)
- FIRECRAWL_RETRIES: Retries for Firecrawl (default: 1, set to 0 to disable)
- BRIGHTDATA_RETRIES: Retries for Bright Data (default: 3, set to 0 to disable)
- PLAYWRIGHT_RETRIES: Retries for Playwright (default: 2, set to 0 to disable)

Usage:
    modal run app/modal_scraper.py
    modal run app/modal_scraper.py --input-file urls.json
    modal deploy app/modal_scraper.py
"""

import modal
import asyncio
import gzip
import json
import os
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from typing import List, Optional, Tuple, Any

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

# Default retry configuration (can be overridden via environment variables)
# Set to 0 to disable a method entirely
DEFAULT_FIRECRAWL_RETRIES = 1    # Firecrawl: often blocked, 1 attempt
DEFAULT_BRIGHTDATA_RETRIES = 3   # Bright Data: reliable, worth retrying
DEFAULT_PLAYWRIGHT_RETRIES = 2   # Playwright: local browser, worth retrying

# Tinybird configuration
TINYBIRD_HOST = "https://api.us-east.tinybird.co"
TINYBIRD_DATASOURCE = "product_scrapes"

# Tinybird Batcher configuration
TINYBIRD_BATCH_SIZE = 10          # Flush every N records
TINYBIRD_FLUSH_TIMEOUT = 30.0     # Flush after N seconds even if batch not full
TINYBIRD_MAX_RETRIES = 3          # Max retry attempts per batch
TINYBIRD_BASE_DELAY = 1.0         # Initial retry delay (seconds)
TINYBIRD_MAX_DELAY = 10.0         # Maximum retry delay (seconds)
TINYBIRD_REQUEST_TIMEOUT = 30.0   # HTTP request timeout

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
    product_image_url: Optional[str] = None
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
    # New fields from Tinybird schema
    alertId: Optional[str] = None
    companyName: Optional[str] = None
    hasAlert: Optional[bool] = None
    screenshotId: Optional[str] = None
    # Categorization fields (Business, Channel, Family)
    businessId: Optional[str] = None
    businessName: Optional[str] = None
    channelId: Optional[str] = None
    channelName: Optional[str] = None
    familyId: Optional[str] = None
    familyName: Optional[str] = None

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
            "availability", "product_image_url", "seller", "shippingInfo", "shippingCost",
            "deliveryTime", "review_score", "installmentOptions", "kit", "unitMeasurement",
            "outOfStockReason", "marketplaceWebsite", "sku", "ean", "stockQuantity",
            "otherPaymentMethods", "promotionDetails", "method", "attempts", "errors",
            "alertId", "companyName", "hasAlert", "screenshotId",
            "businessId", "businessName", "channelId", "channelName", "familyId", "familyName"
        ]:
            value = getattr(self, field_name)
            if value is not None:
                result[field_name] = value
        return result


# =============================================================================
# HTML Cleaner
# =============================================================================

# =============================================================================
# CSS Selector Patterns for E-commerce Data Extraction
# =============================================================================
# These patterns use PARTIAL MATCHING (contains) which is the RECOMMENDED approach:
#
# WHY PARTIAL MATCHING IS GOOD:
# 1. Catches BEM variations: "product__price", "product-price", "product_price"
# 2. Handles prefixed classes: "pdp-price-box", "main-product-title"
# 3. Works across platforms: VTEX, Magento, WooCommerce, Shopify, custom sites
# 4. Resilient to site-specific naming: "raia-price", "amazon-price", etc.
# 5. More robust than exact matching for diverse e-commerce sites
#
# CAUTION: Very short patterns might match unintended elements
# (e.g., "de" in Spanish could match "header"). Use minimum 3-4 char patterns.
# =============================================================================

# Organized by data field with patterns in English, Portuguese, and Spanish
SELECTOR_PATTERNS = {
    # Product identification (title, name, heading)
    "product_title": (
        # English
        r"product|item|article|goods|merchandise|listing|"
        r"title|name|heading|header|headline|"
        r"pdp|plp|detail|description|"
        # Portuguese
        r"produto|titulo|nome|detalhe|descricao|cabecalho|"
        # Spanish
        r"producto|articulo|titulo|nombre|detalle|descripcion|encabezado|"
        # Platform-specific (VTEX, Magento, Shopify, WooCommerce)
        r"vtex|magento|shopify|woo|"
        r"sku-name|product-name|item-title"
    ),

    # Price (current, original, sale, discount)
    "price": (
        # English
        r"price|cost|amount|value|money|fee|"
        r"sale|offer|deal|discount|saving|"
        r"current|original|regular|list|retail|msrp|rrp|"
        r"old|new|final|total|subtotal|"
        r"was|now|from|"
        # Portuguese
        r"preco|preço|valor|custo|quantia|"
        r"oferta|promocao|promoção|desconto|economia|"
        r"atual|original|antigo|novo|final|"
        r"por|era|"  # "Por: R$ X" / "Era: R$ Y"
        # Spanish
        r"precio|costo|valor|importe|monto|"
        r"oferta|promocion|promoción|descuento|ahorro|rebaja|"
        r"actual|original|anterior|nuevo|final|"
        r"antes|ahora|desde|"
        # Platform-specific
        r"best-price|sale-price|special-price|spot-price|"
        r"price-box|price-tag|price-value|price-amount|"
        r"instalment|installment"  # Often near price
    ),

    # Discount and promotion
    "discount": (
        # English
        r"discount|save|saving|off|percent|badge|tag|"
        r"promo|promotion|deal|special|clearance|flash|"
        r"coupon|voucher|code|"
        # Portuguese
        r"desconto|economia|economize|porcento|selo|tag|"
        r"promocao|promoção|oferta|especial|liquidacao|liquidação|"
        r"cupom|voucher|codigo|"
        # Spanish
        r"descuento|ahorro|ahorra|porciento|etiqueta|"
        r"promocion|promoción|oferta|especial|liquidacion|liquidación|"
        r"cupon|cupón|codigo|código"
    ),

    # Payment and installments
    "payment": (
        # English
        r"payment|pay|checkout|purchase|buy|"
        r"installment|instalment|financing|credit|"
        r"card|visa|master|amex|paypal|"
        r"split|divide|monthly|"
        # Portuguese
        r"pagamento|pagar|compra|comprar|"
        r"parcela|parcelamento|parcelado|financiamento|credito|crédito|"
        r"cartao|cartão|pix|boleto|"
        r"vezes|"  # "10x de R$ 15"
        # Spanish
        r"pago|pagar|compra|comprar|"
        r"cuota|cuotas|financiamiento|financiacion|credito|crédito|"
        r"tarjeta|debito|débito|"
        r"meses|mensual|"  # "12 meses sin intereses"
        # Platform-specific
        r"installment-option|payment-method|checkout-button"
    ),

    # Shipping and delivery
    "shipping": (
        # English
        r"ship|shipping|delivery|deliver|freight|dispatch|"
        r"fulfillment|logistics|carrier|courier|"
        r"free-ship|express|standard|overnight|"
        r"arrive|arrival|estimated|eta|"
        r"track|tracking|"
        # Portuguese
        r"frete|entrega|entregar|envio|enviar|despacho|"
        r"transportadora|correios|sedex|pac|"
        r"gratis|gratuito|expresso|normal|"
        r"prazo|chegada|previsao|previsão|"
        r"rastreio|rastrear|"
        # Spanish
        r"envio|envío|entrega|entregar|flete|despacho|"
        r"transportadora|correo|mensajeria|mensajería|"
        r"gratis|gratuito|express|estandar|estándar|"
        r"plazo|llegada|estimado|"
        r"rastreo|rastrear|seguimiento|"
        # Time-related
        r"dias|días|days|hours|horas|business|util|útil"
    ),

    # Stock and availability
    "stock": (
        # English
        r"stock|inventory|available|availability|"
        r"instock|in-stock|out-of-stock|outofstock|"
        r"sold-out|soldout|unavailable|"
        r"quantity|qty|units|left|remaining|"
        r"low-stock|last-units|few-left|"
        r"backorder|preorder|pre-order|"
        # Portuguese
        r"estoque|disponivel|disponível|disponibilidade|"
        r"esgotado|indisponivel|indisponível|"
        r"quantidade|unidades|restantes|"
        r"ultimas|últimas|poucos|"
        r"encomenda|reserva|"
        # Spanish
        r"stock|existencias|inventario|disponible|disponibilidad|"
        r"agotado|nodisponible|no-disponible|"
        r"cantidad|unidades|quedan|restantes|"
        r"ultimas|últimas|ultimos|últimos|pocos|"
        r"pedido|reserva|preventa"
    ),

    # Images (product photos, gallery, thumbnails)
    "image": (
        # English
        r"image|img|photo|picture|pic|"
        r"gallery|carousel|slider|slideshow|"
        r"thumbnail|thumb|preview|"
        r"zoom|magnify|lightbox|"
        r"main-image|product-image|hero-image|"
        r"media|visual|figure|"
        # Portuguese
        r"imagem|foto|fotografia|figura|"
        r"galeria|carrossel|"
        r"miniatura|mini|"
        r"ampliar|zoom|"
        # Spanish
        r"imagen|foto|fotografía|fotografía|figura|"
        r"galeria|galería|carrusel|"
        r"miniatura|vista-previa|"
        r"ampliar|zoom"
    ),

    # Brand and manufacturer
    "brand": (
        # English
        r"brand|manufacturer|maker|vendor|"
        r"supplier|producer|"
        r"logo|trademark|"
        # Portuguese
        r"marca|fabricante|fornecedor|produtor|"
        r"logo|logotipo|"
        # Spanish
        r"marca|fabricante|proveedor|productor|"
        r"logo|logotipo|"
        # Platform-specific
        r"brand-name|product-brand|manufacturer-name"
    ),

    # Seller and store
    "seller": (
        # English
        r"seller|vendor|merchant|retailer|dealer|"
        r"shop|store|marketplace|"
        r"sold-by|soldby|shipped-by|"
        r"fulfilled|fulfillment|"
        # Portuguese
        r"vendedor|loja|lojista|comerciante|"
        r"vendido-por|vendidopor|enviado-por|"
        r"marketplace|parceiro|"
        # Spanish
        r"vendedor|tienda|comerciante|distribuidor|"
        r"vendido-por|vendidopor|enviado-por|"
        r"marketplace|socio|aliado"
    ),

    # Reviews and ratings
    "review": (
        # English
        r"review|rating|rate|score|stars|"
        r"feedback|testimonial|comment|opinion|"
        r"evaluation|assessment|"
        r"votes|voted|recommend|"
        r"verified|helpful|"
        # Portuguese
        r"avaliacao|avaliação|avaliacoes|avaliações|"
        r"nota|estrela|estrelas|"
        r"comentario|comentário|opiniao|opinião|"
        r"votos|recomendar|"
        r"verificado|"
        # Spanish
        r"resena|reseña|resenas|reseñas|"
        r"calificacion|calificación|puntuacion|puntuación|"
        r"estrellas|valoracion|valoración|"
        r"comentario|opinion|opinión|"
        r"votos|recomendar|"
        r"verificado"
    ),

    # SKU, EAN, product codes
    "sku_code": (
        # Universal codes
        r"sku|upc|ean|gtin|asin|mpn|isbn|"
        r"barcode|bar-code|"
        r"part-number|model-number|"
        r"ref|reference|"
        r"code|codigo|código|"
        # Portuguese
        r"referencia|referência|modelo|"
        # Spanish
        r"referencia|modelo|numero-parte"
    ),

    # Kit and bundle
    "kit": (
        # English
        r"kit|bundle|pack|set|combo|"
        r"collection|group|multipack|multi-pack|"
        r"package|assortment|variety|"
        # Portuguese
        r"kit|conjunto|combo|pacote|"
        r"colecao|coleção|sortido|"
        # Spanish
        r"kit|paquete|combo|conjunto|"
        r"coleccion|colección|surtido|variedad"
    ),

    # Unit and measurement
    "unit": (
        # English
        r"unit|size|weight|volume|quantity|"
        r"measure|dimension|capacity|"
        r"per-unit|each|piece|"
        # Portuguese
        r"unidade|tamanho|peso|volume|quantidade|"
        r"medida|dimensao|dimensão|capacidade|"
        r"cada|peca|peça|"
        # Spanish
        r"unidad|tamano|tamaño|peso|volumen|cantidad|"
        r"medida|dimension|dimensión|capacidad|"
        r"cada|pieza|"
        # Common units (universal)
        r"ml|mg|kg|lb|oz|cm|mm|inch|"
        r"liter|litro|gram|grama|gramo"
    ),

    # Buy button and CTA (Call to Action)
    "buy_cta": (
        # English
        r"buy|purchase|add-to-cart|addtocart|"
        r"add-cart|cart|basket|bag|"
        r"checkout|order|get-it|"
        r"cta|action|button|btn|"
        # Portuguese
        r"comprar|adicionar|carrinho|sacola|bolsa|"
        r"finalizar|pedido|"
        r"botao|botão|"
        # Spanish
        r"comprar|agregar|carrito|canasta|bolsa|"
        r"finalizar|pedido|"
        r"boton|botón"
    ),

    # Warranty and guarantee
    "warranty": (
        # English
        r"warranty|guarantee|protection|"
        r"coverage|policy|return|refund|"
        # Portuguese
        r"garantia|protecao|proteção|"
        r"cobertura|politica|política|devolucao|devolução|reembolso|"
        # Spanish
        r"garantia|garantía|proteccion|protección|"
        r"cobertura|politica|política|devolucion|devolución|reembolso"
    ),
}


def clean_html(html_content: str) -> Tuple[str, dict]:
    """
    Clean HTML content to reduce tokens while preserving product information.

    Uses partial matching (contains) for CSS selectors - this is the recommended
    approach because it catches variations like BEM naming, platform prefixes,
    and site-specific customizations.
    """
    from bs4 import BeautifulSoup, Comment

    soup = BeautifulSoup(html_content, 'lxml')

    # Preserve important scripts (JSON-LD, Next.js data, etc.)
    important_scripts = []
    script_keywords = [
        # Product data
        'product', 'price', 'sku', 'stock', 'inventory',
        # Portuguese
        'preco', 'preço', 'produto', 'estoque',
        # Spanish
        'precio', 'producto', 'existencias',
        # Platforms
        'catalog', 'vtex', 'magento', 'shopify', 'woocommerce',
        # Schema.org
        'schema', '@type', 'offer', 'aggregaterating'
    ]

    for script in soup.find_all('script'):
        script_content = str(script.string or '')
        script_type = script.get('type', '')
        script_id = script.get('id', '')
        should_keep = (
            script_type in ['application/ld+json', 'application/json'] or
            script_id in ['__NEXT_DATA__', '__NUXT_DATA__', 'schema-org'] or
            any(kw in script_content.lower() for kw in script_keywords)
        )
        if should_keep:
            important_scripts.append(str(script))

    # Remove non-essential elements
    for script in soup.find_all('script'):
        script.decompose()
    for tag in ['style', 'noscript', 'iframe', 'svg', 'link', 'footer', 'nav']:
        for el in soup.find_all(tag):
            el.decompose()
    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    # Build combined regex for all selector patterns
    all_patterns = '|'.join(SELECTOR_PATTERNS.values())
    combined_regex = re.compile(all_patterns, re.I)

    # Find all elements with matching class names
    product_areas = []
    for element in soup.find_all(attrs={'class': combined_regex}, limit=50):
        product_areas.append(str(element))

    # Also find by id attribute
    for element in soup.find_all(attrs={'id': combined_regex}, limit=20):
        product_areas.append(str(element))

    # Also find by data-* attributes (common in React/Vue/Angular)
    data_patterns = re.compile(r'data-(product|price|sku|stock|brand|seller|image)', re.I)
    for element in soup.find_all(lambda tag: any(data_patterns.match(attr) for attr in tag.attrs.keys() if attr.startswith('data-')), limit=20):
        product_areas.append(str(element))

    # Get product images directly
    product_images = []
    image_skip_patterns = ['icon', 'logo', 'sprite', 'pixel', 'tracking', 'analytics', 'badge', 'flag', 'banner', 'ad-', 'advertisement']

    for img in soup.find_all('img', limit=30):
        src = img.get('src', '') or img.get('data-src', '') or img.get('data-lazy', '') or ''
        srcset = img.get('srcset', '') or img.get('data-srcset', '') or ''

        # Keep images that are likely product images
        if (src or srcset) and len(src + srcset) > 20:
            if not any(skip in (src + srcset).lower() for skip in image_skip_patterns):
                product_images.append(str(img))

    # Get meta tags (OpenGraph, Twitter Cards, Schema.org hints)
    meta_keywords = [
        'product', 'price', 'amount', 'currency',
        'title', 'name', 'description',
        'image', 'og:', 'twitter:',
        'brand', 'manufacturer',
        'availability', 'sku',
        'review', 'rating'
    ]
    meta_tags = []
    for m in soup.find_all('meta'):
        meta_name = m.get('name', '').lower()
        meta_property = m.get('property', '').lower()
        meta_itemprop = m.get('itemprop', '').lower()
        combined_attrs = meta_name + meta_property + meta_itemprop
        if any(kw in combined_attrs for kw in meta_keywords):
            meta_tags.append(str(m))

    # Get elements with itemprop (Schema.org microdata)
    schema_props = [
        'name', 'description', 'image', 'brand', 'sku', 'gtin', 'mpn',
        'price', 'priceCurrency', 'availability', 'seller',
        'aggregateRating', 'ratingValue', 'reviewCount', 'offers'
    ]
    schema_elements = []
    for prop in schema_props:
        for el in soup.find_all(attrs={'itemprop': prop}, limit=5):
            schema_elements.append(str(el))

    # Deduplicate while preserving order
    seen = set()
    unique_areas = []
    for area in product_areas[:30]:
        area_hash = hash(area)
        if area_hash not in seen:
            seen.add(area_hash)
            unique_areas.append(area)

    # Combine all extracted content
    combined = "\n".join(
        meta_tags +
        schema_elements +
        product_images[:10] +
        unique_areas +
        important_scripts
    )

    stats = {
        "original_size": len(html_content),
        "cleaned_size": len(combined),
        "reduction_pct": round((1 - len(combined) / len(html_content)) * 100, 1) if html_content else 0,
        "areas_found": len(unique_areas),
        "images_found": len(product_images),
        "meta_tags_found": len(meta_tags),
        "schema_elements_found": len(schema_elements),
    }
    return combined, stats


# =============================================================================
# Image Compression
# =============================================================================

def compress_image(image_bytes: bytes, quality: int = 85) -> Tuple[bytes, dict]:
    """Compress PNG image to optimized WebP."""
    from PIL import Image

    original_size = len(image_bytes)
    img = Image.open(BytesIO(image_bytes))
    # WebP supports RGBA, no need to convert unless we want to reduce size further
    if img.mode == 'P':
        img = img.convert('RGBA')

    output = BytesIO()
    img.save(output, format='WEBP', quality=quality, method=6)  # method=6 = best compression
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
        filename = f"screenshots/{now.strftime('%Y/%m/%d')}/{url_id}_{uuid.uuid4().hex[:8]}.webp"
        client.put_object(Bucket=bucket_name, Key=filename, Body=image_bytes, ContentType='image/webp')
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
# Tinybird Batcher (Resilient Batch Ingestion)
# =============================================================================

class TinybirdBatcher:
    """
    Resilient batch sender for Tinybird Events API.

    Features:
    - Batches records (default: 10) before sending
    - Auto-flush on timeout (default: 30s) even if batch not full
    - Exponential backoff with jitter on failures
    - Gzip compression for efficiency
    - Thread-safe async operations

    Usage:
        async with TinybirdBatcher() as batcher:
            await batcher.add(record1)
            await batcher.add(record2)
            # Auto-flushes when batch_size reached or timeout expires
        # Remaining records flushed on context exit
    """

    def __init__(
        self,
        batch_size: int = TINYBIRD_BATCH_SIZE,
        flush_timeout: float = TINYBIRD_FLUSH_TIMEOUT,
        max_retries: int = TINYBIRD_MAX_RETRIES,
        base_delay: float = TINYBIRD_BASE_DELAY,
        max_delay: float = TINYBIRD_MAX_DELAY,
    ):
        self.batch_size = batch_size
        self.flush_timeout = flush_timeout
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

        self._buffer: List[dict] = []
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._last_add_time: float = 0
        self._client = None

        # Metrics
        self._total_records = 0
        self._total_batches = 0
        self._failed_batches = 0
        self._total_retries = 0

    async def __aenter__(self):
        """Initialize HTTP client on context enter."""
        import httpx
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(TINYBIRD_REQUEST_TIMEOUT))
        self._start_flush_timer()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Flush remaining records and cleanup on context exit."""
        # Cancel timer
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush
        await self.flush()

        # Close client
        if self._client:
            await self._client.aclose()

        # Print metrics
        self._print_metrics()

        return False

    def _start_flush_timer(self):
        """Start or restart the flush timeout timer."""
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
        self._flush_task = asyncio.create_task(self._flush_timer())

    async def _flush_timer(self):
        """Background task that flushes on timeout."""
        try:
            while True:
                await asyncio.sleep(self.flush_timeout)
                async with self._lock:
                    if self._buffer and (time.time() - self._last_add_time) >= self.flush_timeout:
                        print(f"[TinybirdBatcher] Timeout flush: {len(self._buffer)} records")
                        await self._flush_internal()
        except asyncio.CancelledError:
            pass

    def _prepare_record(self, scrape_result: dict) -> dict:
        """Convert scrape result to Tinybird-compatible record."""
        scraped_at_ms = scrape_result.get("scrapedAt", int(time.time() * 1000))
        scraped_at_iso = datetime.utcfromtimestamp(scraped_at_ms / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        record = {
            "urlId": scrape_result.get("urlId", ""),
            "productUrl": scrape_result.get("productUrl", ""),
            "status": scrape_result.get("status", "error"),
            "scrapedAt": scraped_at_iso,
            "errorMessage": scrape_result.get("errorMessage"),
            "screenshotUrl": scrape_result.get("screenshotUrl"),
            "productTitle": scrape_result.get("productTitle"),
            "productName": scrape_result.get("productTitle"),
            "brand": scrape_result.get("brand"),
            "brandName": scrape_result.get("brand"),
            "currentPrice": scrape_result.get("currentPrice"),
            "originalPrice": scrape_result.get("originalPrice"),
            "discountPercentage": scrape_result.get("discountPercentage"),
            "currency": scrape_result.get("currency"),
            "availability": 1 if scrape_result.get("availability") else (0 if scrape_result.get("availability") is False else None),
            "imageUrl": scrape_result.get("product_image_url"),
            "seller": scrape_result.get("seller"),
            "sellerName": scrape_result.get("seller"),
            "shippingInfo": scrape_result.get("shippingInfo"),
            "shippingCost": scrape_result.get("shippingCost"),
            "deliveryTime": scrape_result.get("deliveryTime"),
            "review_score": str(scrape_result.get("review_score")) if scrape_result.get("review_score") is not None else None,
            "installmentOptions": scrape_result.get("installmentOptions"),
            "kit": 1 if scrape_result.get("kit") else (0 if scrape_result.get("kit") is False else None),
            "unitMeasurement": scrape_result.get("unitMeasurement"),
            "outOfStockReason": scrape_result.get("outOfStockReason"),
            "marketplaceWebsite": scrape_result.get("marketplaceWebsite"),
            "sku": scrape_result.get("sku"),
            "ean": scrape_result.get("ean"),
            "stockQuantity": scrape_result.get("stockQuantity"),
            "otherPaymentMethods": scrape_result.get("otherPaymentMethods"),
            "promotionDetails": scrape_result.get("promotionDetails"),
            "method": scrape_result.get("method"),
            "attempts": json.dumps(scrape_result.get("attempts")) if scrape_result.get("attempts") else None,
            "errors": json.dumps(scrape_result.get("errors")) if scrape_result.get("errors") else None,
            # New fields from Tinybird schema
            "alertId": scrape_result.get("alertId"),
            "companyName": scrape_result.get("companyName"),
            "hasAlert": 1 if scrape_result.get("hasAlert") else (0 if scrape_result.get("hasAlert") is False else None),
            "screenshotId": scrape_result.get("screenshotId"),
            # Categorization fields (Business, Channel, Family)
            "businessId": scrape_result.get("businessId"),
            "businessName": scrape_result.get("businessName"),
            "channelId": scrape_result.get("channelId"),
            "channelName": scrape_result.get("channelName"),
            "familyId": scrape_result.get("familyId"),
            "familyName": scrape_result.get("familyName"),
        }

        return {k: v for k, v in record.items() if v is not None}

    async def add(self, scrape_result: dict):
        """Add a scrape result to the batch buffer."""
        record = self._prepare_record(scrape_result)

        async with self._lock:
            self._buffer.append(record)
            self._last_add_time = time.time()
            self._total_records += 1

            url_id = scrape_result.get("urlId", "unknown")
            print(f"[{url_id}] TinybirdBatcher: Queued ({len(self._buffer)}/{self.batch_size})")

            if len(self._buffer) >= self.batch_size:
                print(f"[TinybirdBatcher] Batch full: flushing {len(self._buffer)} records")
                await self._flush_internal()

    async def flush(self):
        """Force flush all buffered records."""
        async with self._lock:
            if self._buffer:
                await self._flush_internal()

    async def _flush_internal(self):
        """Internal flush - must be called with lock held."""
        if not self._buffer:
            return

        token = os.environ.get("TINYBIRD_TOKEN")
        if not token:
            print("[TinybirdBatcher] ERROR: TINYBIRD_TOKEN not configured - discarding batch")
            self._buffer.clear()
            return

        batch = self._buffer.copy()
        self._buffer.clear()

        success = await self._send_batch_with_retry(batch, token)

        self._total_batches += 1
        if not success:
            self._failed_batches += 1

    async def _send_batch_with_retry(self, batch: List[dict], token: str) -> bool:
        """Send batch with exponential backoff retry."""
        import httpx

        # Prepare NDJSON payload
        ndjson_lines = [json.dumps(record, ensure_ascii=False) for record in batch]
        ndjson_data = "\n".join(ndjson_lines)

        # Gzip compress
        compressed_data = gzip.compress(ndjson_data.encode('utf-8'))
        compression_ratio = (1 - len(compressed_data) / len(ndjson_data.encode('utf-8'))) * 100

        batch_id = f"batch_{int(time.time())}_{len(batch)}"
        url_ids = [r.get("urlId", "?") for r in batch[:3]]
        url_ids_str = ", ".join(url_ids) + ("..." if len(batch) > 3 else "")

        print(f"[TinybirdBatcher] Sending {batch_id}: {len(batch)} records, "
              f"{len(compressed_data):,} bytes (gzip: {compression_ratio:.1f}% reduction)")
        print(f"[TinybirdBatcher] URLs: [{url_ids_str}]")

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = await self._client.post(
                    f"{TINYBIRD_HOST}/v0/events?name={TINYBIRD_DATASOURCE}",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                        "Content-Encoding": "gzip",
                    },
                    content=compressed_data,
                )

                if response.status_code in (200, 202):
                    result = response.json()
                    rows = result.get('successful_rows', 0)
                    quarantined = result.get('quarantined_rows', 0)
                    print(f"[TinybirdBatcher] {batch_id} SUCCESS: {rows} rows ingested"
                          + (f", {quarantined} quarantined" if quarantined else ""))
                    return True

                elif response.status_code == 429:
                    # Rate limited - use Retry-After header if available
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else self._calculate_delay(attempt)
                    print(f"[TinybirdBatcher] {batch_id} Rate limited (429), retry in {delay:.1f}s")
                    last_error = "Rate limited (429)"

                else:
                    last_error = f"HTTP {response.status_code}: {response.text[:100]}"
                    print(f"[TinybirdBatcher] {batch_id} Error: {last_error}")

            except httpx.TimeoutException:
                last_error = "Request timeout"
                print(f"[TinybirdBatcher] {batch_id} Timeout on attempt {attempt}/{self.max_retries}")

            except Exception as e:
                last_error = str(e)[:100]
                print(f"[TinybirdBatcher] {batch_id} Exception: {last_error}")

            # Retry with exponential backoff + jitter
            if attempt < self.max_retries:
                delay = self._calculate_delay(attempt)
                self._total_retries += 1
                print(f"[TinybirdBatcher] {batch_id} Retrying in {delay:.1f}s (attempt {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(delay)

        # All retries exhausted
        print(f"[TinybirdBatcher] {batch_id} FAILED after {self.max_retries} attempts: {last_error}")
        print(f"[TinybirdBatcher] Lost {len(batch)} records: [{url_ids_str}]")
        return False

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff + jitter."""
        delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        jitter = random.uniform(0, delay * 0.3)  # Up to 30% jitter
        return delay + jitter

    def _print_metrics(self):
        """Print batch processing metrics."""
        print(f"\n{'='*60}")
        print(f"TINYBIRD BATCHER METRICS")
        print(f"{'='*60}")
        print(f"Total records processed: {self._total_records}")
        print(f"Total batches sent: {self._total_batches}")
        print(f"Failed batches: {self._failed_batches}")
        print(f"Total retries: {self._total_retries}")
        if self._total_batches > 0:
            success_rate = ((self._total_batches - self._failed_batches) / self._total_batches) * 100
            print(f"Success rate: {success_rate:.1f}%")
        print(f"{'='*60}\n")


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
            wait_seconds = int(os.environ.get("FIRECRAWL_WAIT_SECONDS", "2"))
            response = await client.post(
                "https://api.firecrawl.dev/v2/scrape",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "url": url,
                    "formats": ["html", "screenshot"],
                    "waitFor": wait_seconds * 1000,
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
                else:
                    print(f"[{url_id}] Bright Data: Screenshot failed - status={screen_response.status_code}, size={len(screen_response.content)} bytes")
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

def repair_truncated_json(text: str) -> str:
    """
    Attempt to repair truncated or malformed JSON.

    Common issues:
    - Truncated at end (missing closing braces)
    - Trailing commas
    - Incomplete string values
    """
    text = text.strip()

    # Remove markdown code blocks
    if text.startswith('```'):
        lines = text.split('\n')
        # Remove first line (```json or ```)
        lines = lines[1:]
        # Remove last line if it's ```
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        text = '\n'.join(lines)

    # Remove any trailing ``` that might be in the middle
    if '```' in text:
        text = text.split('```')[0]

    text = text.strip()

    # Count braces to check if truncated
    open_braces = text.count('{')
    close_braces = text.count('}')
    open_brackets = text.count('[')
    close_brackets = text.count(']')

    # If truncated, try to fix
    if open_braces > close_braces or open_brackets > close_brackets:
        # Remove incomplete last line (often truncated mid-value)
        lines = text.rstrip().split('\n')

        # Check if last line is incomplete (no comma or closing brace)
        while lines:
            last_line = lines[-1].strip()
            # If line ends properly, stop
            if last_line.endswith((',', '{', '[', '}', ']', 'true', 'false', 'null')) or \
               (last_line.endswith('"') and ':' not in last_line):
                break
            # If line has unclosed string, remove it
            if last_line.count('"') % 2 != 0:
                lines.pop()
                continue
            # If line doesn't end with proper JSON, remove it
            if not any(last_line.endswith(c) for c in (',', '{', '[', '}', ']', '"', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'true', 'false', 'null')):
                lines.pop()
                continue
            break

        text = '\n'.join(lines)

        # Remove trailing comma before adding closing braces
        text = text.rstrip()
        if text.endswith(','):
            text = text[:-1]

        # Add missing closing braces/brackets
        open_braces = text.count('{')
        close_braces = text.count('}')
        open_brackets = text.count('[')
        close_brackets = text.count(']')

        text += ']' * (open_brackets - close_brackets)
        text += '}' * (open_braces - close_braces)

    # Remove trailing commas before closing braces (common JSON error)
    text = re.sub(r',(\s*[}\]])', r'\1', text)

    return text


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
- product_image_url: Main product image URL
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

IMPORTANT: Return ONLY valid JSON, no markdown, no explanation. Omit fields not found.
Keep the response concise - only include fields with actual values."""

        # Use async generate with higher token limit
        response = await client.aio.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(temperature=0.1, max_output_tokens=8192)
        )

        text = response.text.strip()

        # Try to repair truncated/malformed JSON
        repaired_text = repair_truncated_json(text)

        try:
            data = json.loads(repaired_text)
        except json.JSONDecodeError:
            # If repair failed, try original text with basic cleanup
            text_clean = text.strip()
            if text_clean.startswith('```'):
                text_clean = '\n'.join(text_clean.split('\n')[1:])
            if text_clean.endswith('```'):
                text_clean = text_clean[:-3]
            data = json.loads(text_clean)

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
# Input Data Helpers
# =============================================================================

def extract_url_from_item(item: dict) -> Optional[str]:
    """
    Extract the URL to scrape from an input item.

    Looks for 'url' or 'productUrl' fields.
    Returns None if no valid URL found.
    """
    # Try 'url' first, then 'productUrl'
    url = item.get("url") or item.get("productUrl")
    if url and isinstance(url, str) and url.strip():
        return url.strip()
    return None


def extract_initial_data(item: dict) -> dict:
    """
    Extract initial data from input item that should override scraped data.

    These fields will be preserved and override any data extracted by the scraper.
    This ensures that metadata provided by the user takes precedence.
    """
    # Fields that can be provided in input and should override scraped results
    # Mapping: input field name -> output field name (for Tinybird compatibility)
    field_mappings = {
        # Identity fields (always preserved)
        "urlId": "urlId",
        # Convex reference IDs (for tracking)
        "productId": "productId",
        "sellerId": "sellerId",
        # Metadata fields from input
        "alertId": "alertId",
        "companyName": "companyName",
        "hasAlert": "hasAlert",
        "screenshotId": "screenshotId",
        # Product fields - keep cadastrado vs extraído separate
        "productTitle": "productTitle",  # Extracted title from scraper
        "productName": "productName",    # Cadastrado name from Convex (kept separate)
        "brand": "brand",
        "brandName": "brandName",        # Cadastrado brand from Convex
        "seller": "seller",              # Extracted seller from scraper
        "sellerName": "sellerName",      # Cadastrado seller from Convex (kept separate)
        # Price fields
        "currentPrice": "currentPrice",
        "originalPrice": "originalPrice",
        "discountPercentage": "discountPercentage",
        "currency": "currency",
        # Alert configuration (for price breach detection)
        "minPrice": "minPrice",
        "maxPrice": "maxPrice",
        "alertsEnabled": "alertsEnabled",
        # Other product fields
        "availability": "availability",
        "imageUrl": "product_image_url",
        "product_image_url": "product_image_url",
        "shippingInfo": "shippingInfo",
        "shippingCost": "shippingCost",
        "deliveryTime": "deliveryTime",
        "review_score": "review_score",
        "installmentOptions": "installmentOptions",
        "kit": "kit",
        "unitMeasurement": "unitMeasurement",
        "outOfStockReason": "outOfStockReason",
        "marketplaceWebsite": "marketplaceWebsite",
        "sku": "sku",
        "ean": "ean",
        "stockQuantity": "stockQuantity",
        "otherPaymentMethods": "otherPaymentMethods",
        "promotionDetails": "promotionDetails",
        # Categorization fields (Business, Channel, Family)
        "businessId": "businessId",
        "businessName": "businessName",
        "channelId": "channelId",
        "channelName": "channelName",
        "familyId": "familyId",
        "familyName": "familyName",
    }

    initial_data = {}
    for input_field, output_field in field_mappings.items():
        value = item.get(input_field)
        if value is not None:
            # Don't override if we already have a value for this output field
            # (handles cases like productName and productTitle both being set)
            if output_field not in initial_data:
                initial_data[output_field] = value

    return initial_data


def merge_with_initial_data(result: dict, initial_data: dict) -> dict:
    """
    Merge scrape result with initial data, giving priority to initial data.

    Initial data (provided by user) takes precedence over scraped data.
    This ensures user-provided metadata is preserved in the final output.
    """
    if not initial_data:
        return result

    merged = result.copy()

    for key, value in initial_data.items():
        if value is not None:
            # Initial data always overrides scraped data
            merged[key] = value

    return merged


def check_price_alert(result: dict) -> Optional[dict]:
    """
    Check if scraped price breaches min/max limits.

    Returns alert dict if breach detected, None otherwise.
    Used by webhook to notify Convex of price alerts.
    """
    # Skip if alerts not enabled or no price found
    if not result.get("alertsEnabled"):
        return None

    current_price = result.get("currentPrice")
    if current_price is None:
        return None

    min_price = result.get("minPrice")
    max_price = result.get("maxPrice")

    # Check min price breach (price dropped below minimum)
    if min_price is not None and current_price < min_price:
        breach_amount = round(min_price - current_price, 2)
        breach_percentage = round(((min_price - current_price) / min_price) * 100, 2)
        return {
            "type": "price_min_breach",
            "currentPrice": current_price,
            "priceLimit": min_price,
            "breachAmount": breach_amount,
            "breachPercentage": breach_percentage,
        }

    # Check max price breach (price went above maximum)
    if max_price is not None and current_price > max_price:
        breach_amount = round(current_price - max_price, 2)
        breach_percentage = round(((current_price - max_price) / max_price) * 100, 2)
        return {
            "type": "price_max_breach",
            "currentPrice": current_price,
            "priceLimit": max_price,
            "breachAmount": breach_amount,
            "breachPercentage": breach_percentage,
        }

    return None


# =============================================================================
# Main Scraper Function (Fully Async)
# =============================================================================

def get_primary_scraper() -> str:
    """Get primary scraper from environment variable."""
    primary = os.environ.get("PRIMARY_SCRAPER", PRIMARY_FIRECRAWL).lower().strip()
    if primary in (PRIMARY_FIRECRAWL, PRIMARY_BRIGHTDATA):
        return primary
    return PRIMARY_FIRECRAWL


def get_retries_config() -> dict:
    """
    Get retry configuration from environment variables.

    Environment variables:
    - FIRECRAWL_RETRIES: Number of retries for Firecrawl (default: 1)
    - BRIGHTDATA_RETRIES: Number of retries for Bright Data (default: 3)
    - PLAYWRIGHT_RETRIES: Number of retries for Playwright (default: 2)

    Setting a value to 0 disables that method entirely.

    Returns:
        dict: Mapping of method name to retry count
    """
    def get_retry_value(env_var: str, default: int) -> int:
        value = os.environ.get(env_var, "")
        if value.strip() == "":
            return default
        try:
            return max(0, int(value))  # Ensure non-negative
        except ValueError:
            return default

    return {
        METHOD_FIRECRAWL: get_retry_value("FIRECRAWL_RETRIES", DEFAULT_FIRECRAWL_RETRIES),
        METHOD_BRIGHTDATA: get_retry_value("BRIGHTDATA_RETRIES", DEFAULT_BRIGHTDATA_RETRIES),
        METHOD_PLAYWRIGHT: get_retry_value("PLAYWRIGHT_RETRIES", DEFAULT_PLAYWRIGHT_RETRIES),
    }


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


def should_force_brightdata_for_url(url: str) -> bool:
    """
    Check if URL should force Bright Data based on URL patterns.

    Currently checks for Mercado Livre URLs when MERCADOLIVRE_FORCE_BRIGHTDATA=true.
    """
    mercadolivre_env = os.environ.get("MERCADOLIVRE_FORCE_BRIGHTDATA", "")
    mercadolivre_enabled = mercadolivre_env.lower() in ("true", "1", "yes")
    is_mercadolivre_url = "mercadolivre" in url.lower()

    # Debug log
    if is_mercadolivre_url:
        print(f"[DEBUG] MERCADOLIVRE_FORCE_BRIGHTDATA={mercadolivre_env!r}, enabled={mercadolivre_enabled}")

    if mercadolivre_enabled and is_mercadolivre_url:
        return True

    return False


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
async def scrape_url(url_id: str, url: str, method: Optional[str] = None, initial_data: Optional[dict] = None) -> dict:
    """
    Scrape a single URL with 3-tier async fallback system.

    Args:
        url_id: Unique identifier for this URL
        url: The URL to scrape
        method: Optional preferred method to try first (firecrawl, brightdata, playwright)
        initial_data: Optional dict with fields from input that override scraped data
    """
    config = get_config()
    primary = get_primary_scraper()
    retries_config = get_retries_config()

    # Check if user specified a method preference
    preferred_method = parse_method_preference(method)

    # Log retry configuration
    enabled_methods = [m for m, r in retries_config.items() if r > 0]
    disabled_methods = [m for m, r in retries_config.items() if r == 0]

    print(f"\n{'='*60}")
    print(f"[{url_id}] Starting scrape: {url[:60]}...")
    if preferred_method:
        print(f"[{url_id}] User preferred: {preferred_method.upper()}")
    print(f"[{url_id}] Primary: {primary.upper()} | Concurrency: {config['max_concurrency']}")
    print(f"[{url_id}] Enabled methods: {', '.join(f'{m}({retries_config[m]})' for m in enabled_methods)}")
    if disabled_methods:
        print(f"[{url_id}] Disabled methods: {', '.join(disabled_methods)}")
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

    # Check URL-based rules (e.g., Mercado Livre -> Bright Data)
    url_forced_brightdata = should_force_brightdata_for_url(url)
    if url_forced_brightdata:
        print(f"[{url_id}] URL pattern detected: forcing BRIGHTDATA first (mercadolivre)")

    # Build attempt order based on preference, URL rules, or PRIMARY_SCRAPER
    if preferred_method and preferred_method in all_methods:
        # User specified a method - put it first, then others in default order
        attempt_order = [preferred_method]
        default_order = [METHOD_FIRECRAWL, METHOD_BRIGHTDATA, METHOD_PLAYWRIGHT]
        for m in default_order:
            if m != preferred_method:
                attempt_order.append(m)
    elif url_forced_brightdata:
        # URL pattern forces Bright Data first
        attempt_order = [METHOD_BRIGHTDATA, METHOD_FIRECRAWL, METHOD_PLAYWRIGHT]
    elif primary == PRIMARY_BRIGHTDATA:
        attempt_order = [METHOD_BRIGHTDATA, METHOD_FIRECRAWL, METHOD_PLAYWRIGHT]
    else:
        attempt_order = [METHOD_FIRECRAWL, METHOD_BRIGHTDATA, METHOD_PLAYWRIGHT]

    # Filter out methods with 0 retries (disabled methods)
    attempt_order = [m for m in attempt_order if retries_config.get(m, 0) > 0]

    if not attempt_order:
        print(f"[{url_id}] ERROR: All scraping methods are disabled!")
        result.status = "error"
        result.errorMessage = "All scraping methods are disabled (all retry counts set to 0)"
        return merge_with_initial_data(result.to_dict(), initial_data)

    attempt_methods = [(all_methods[m][0], all_methods[m][1], m) for m in attempt_order]
    total_methods = len(attempt_methods)

    for attempt_num, (method_name, attempt_func, method_key) in enumerate(attempt_methods, 1):
        # Get retry count from environment-based config
        max_retries = retries_config.get(method_key, 1)

        attempt_result = None
        method_succeeded = False

        for retry in range(max_retries):
            retry_suffix = f" (attempt {retry + 1}/{max_retries})" if max_retries > 1 else ""
            print(f"\n[{url_id}] === METHOD {attempt_num}/{total_methods}: {method_name}{retry_suffix} ===")

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
                          "discountPercentage", "currency", "availability", "product_image_url",
                          "seller", "shippingInfo", "shippingCost", "deliveryTime",
                          "review_score", "installmentOptions", "kit", "unitMeasurement",
                          "outOfStockReason", "marketplaceWebsite", "sku", "ean",
                          "stockQuantity", "otherPaymentMethods", "promotionDetails"]:
                setattr(result, field, product_data.get(field))

            print(f"[{url_id}] Completed in {time.time() - start_time:.2f}s")

            # Return result with initial data merged (initial data takes priority)
            return merge_with_initial_data(result.to_dict(), initial_data)

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
    print(f"\n[{url_id}] ALL {total_methods} METHOD(S) FAILED")
    result.status = "error"
    result.method = "erro"  # Mark method as "erro" when all methods fail
    result.errorMessage = last_error or "All methods failed"
    result.screenshotUrl = last_screenshot_url
    # Keep errors array (don't set to None for failed results)

    print(f"[{url_id}] Failed after {time.time() - start_time:.2f}s")

    # Return result with initial data merged (initial data takes priority)
    return merge_with_initial_data(result.to_dict(), initial_data)


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
    retries_config = get_retries_config()

    # Build priority string based on enabled methods
    enabled_methods = [(m, r) for m, r in [
        (METHOD_FIRECRAWL, retries_config[METHOD_FIRECRAWL]),
        (METHOD_BRIGHTDATA, retries_config[METHOD_BRIGHTDATA]),
        (METHOD_PLAYWRIGHT, retries_config[METHOD_PLAYWRIGHT]),
    ] if r > 0]

    # Reorder based on primary
    if primary == PRIMARY_BRIGHTDATA:
        order = [METHOD_BRIGHTDATA, METHOD_FIRECRAWL, METHOD_PLAYWRIGHT]
    else:
        order = [METHOD_FIRECRAWL, METHOD_BRIGHTDATA, METHOD_PLAYWRIGHT]

    ordered_enabled = [(m, retries_config[m]) for m in order if retries_config[m] > 0]
    priority_str = " -> ".join(f"{m.title()}({r})" for m, r in ordered_enabled) if ordered_enabled else "NONE"

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
    print(f"METHOD RETRIES (0 = disabled):")
    print(f"  Firecrawl:   {retries_config[METHOD_FIRECRAWL]} {'(disabled)' if retries_config[METHOD_FIRECRAWL] == 0 else ''}")
    print(f"  Bright Data: {retries_config[METHOD_BRIGHTDATA]} {'(disabled)' if retries_config[METHOD_BRIGHTDATA] == 0 else ''}")
    print(f"  Playwright:  {retries_config[METHOD_PLAYWRIGHT]} {'(disabled)' if retries_config[METHOD_PLAYWRIGHT] == 0 else ''}")
    print(f"{'='*70}")
    print(f"Tinybird Batching: {TINYBIRD_BATCH_SIZE} records/batch, {TINYBIRD_FLUSH_TIMEOUT}s timeout")
    print(f"{'='*70}")
    print(f"Scraping Priority: {priority_str}")
    print(f"{'='*70}\n")

    start_time = time.time()

    # Use semaphore to control concurrency (respects API limits)
    semaphore = asyncio.Semaphore(config["max_concurrency"])

    async def scrape_with_semaphore(url_id: str, url: str, method: Optional[str] = None, initial_data: Optional[dict] = None) -> dict:
        async with semaphore:
            return await scrape_url.remote.aio(url_id, url, method, initial_data)

    # Pre-process items: extract URL, method, and initial_data from each item
    # This allows flexible input format (url or productUrl for the URL to scrape)
    processed_items = []
    for item in urls_data:
        # Extract URL (try 'url' first, then 'productUrl')
        url = extract_url_from_item(item)
        if not url:
            print(f"[WARNING] Skipping item without URL: {item.get('urlId', 'unknown')}")
            continue

        # Get urlId (required)
        url_id = item.get("urlId")
        if not url_id:
            print(f"[WARNING] Skipping item without urlId: {url[:50]}...")
            continue

        # Extract method preference
        method = item.get("method")

        # Extract initial data (fields that will override scraped data)
        initial_data = extract_initial_data(item)

        processed_items.append({
            "url_id": url_id,
            "url": url,
            "method": method,
            "initial_data": initial_data,
            "original_item": item,  # Keep reference for error handling
        })

    print(f"Processed {len(processed_items)} valid items from {len(urls_data)} input items")

    # Create all tasks
    tasks = [
        scrape_with_semaphore(
            item["url_id"],
            item["url"],
            item["method"],
            item["initial_data"]
        )
        for item in processed_items
    ]

    # Run all tasks concurrently (semaphore limits actual concurrency)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle any exceptions
    processed_results = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            # Build error result with initial_data merged
            item = processed_items[i]
            error_result = {
                "urlId": item["url_id"],
                "productUrl": item["url"],
                "status": "error",
                "errorMessage": str(r)[:200],
                "scrapedAt": int(time.time() * 1000)
            }
            # Merge with initial_data so metadata is preserved even on errors
            error_result = merge_with_initial_data(error_result, item["initial_data"])
            processed_results.append(error_result)
        else:
            processed_results.append(r)

    scrape_elapsed = time.time() - start_time

    # Stats
    successful = sum(1 for r in processed_results if r.get("status") == "completed")
    failed = len(processed_results) - successful
    with_screenshots = sum(1 for r in processed_results if r.get("screenshotUrl"))
    via_fc = sum(1 for r in processed_results if r.get("method") == METHOD_FIRECRAWL)
    via_bd = sum(1 for r in processed_results if r.get("method") == METHOD_BRIGHTDATA)
    via_pw = sum(1 for r in processed_results if r.get("method") == METHOD_PLAYWRIGHT)

    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Total: {len(processed_results)} | Success: {successful} | Failed: {failed}")
    print(f"Screenshots: {with_screenshots}")
    print(f"Methods: Firecrawl={via_fc}, BrightData={via_bd}, Playwright={via_pw}")
    print(f"Scrape Time: {scrape_elapsed:.2f}s | Rate: {len(processed_results)/scrape_elapsed:.2f} URLs/sec")
    print(f"{'='*60}\n")

    # Send all results to Tinybird using the batcher
    print(f"\n{'='*60}")
    print(f"SENDING TO TINYBIRD (batched)")
    print(f"{'='*60}")

    tinybird_start = time.time()

    async with TinybirdBatcher() as batcher:
        for result in processed_results:
            await batcher.add(result)
        # Batcher auto-flushes remaining records on context exit

    tinybird_elapsed = time.time() - tinybird_start
    total_elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"BATCH COMPLETE")
    print(f"{'='*60}")
    print(f"Scrape Time: {scrape_elapsed:.2f}s")
    print(f"Tinybird Time: {tinybird_elapsed:.2f}s")
    print(f"Total Time: {total_elapsed:.2f}s")
    print(f"{'='*60}\n")

    return processed_results


# =============================================================================
# Local Entrypoint
# =============================================================================

@app.local_entrypoint()
def main(input_file: str = None, input_json: str = None):
    """
    Main entrypoint.

    Accepts JSON input with flexible field names:
    - URL can be specified as 'url' or 'productUrl'
    - Method can be specified as 'method' (firecrawl, brightdata, playwright)
    - Additional fields (companyName, alertId, productTitle, seller, etc.)
      will override any scraped data in the final output

    Example input:
    {
        "urls": [
            {
                "urlId": "test_001",
                "url": "https://www.amazon.com.br/dp/B0BDHWDR12",
                "companyName": "My Company",
                "alertId": "alert_123",
                "method": "brightdata"
            }
        ]
    }
    """
    if input_file:
        with open(input_file, 'r') as f:
            data = json.load(f)
    elif input_json:
        data = json.loads(input_json)
    else:
        # Default test data with example of initial data fields
        data = {"urls": [{
            "urlId": "test_001",
            "url": "https://www.amazon.com.br/dp/B0BDHWDR12",
            "companyName": "Test Company"  # This will override any scraped company name
        }]}

    urls_data = data.get("urls", [])
    if not urls_data:
        print("No URLs!")
        return

    print(f"\n{'='*60}")
    print(f"INPUT: {len(urls_data)} URLs")
    print(f"{'='*60}")

    # Show sample of input data with initial fields
    for i, item in enumerate(urls_data[:3]):
        url = extract_url_from_item(item)
        initial_data = extract_initial_data(item)
        override_fields = [k for k, v in initial_data.items() if v is not None and k != "urlId"]
        print(f"  [{i+1}] {item.get('urlId', 'unknown')}: {url[:50] if url else 'NO URL'}...")
        if override_fields:
            print(f"      Override fields: {', '.join(override_fields)}")
    if len(urls_data) > 3:
        print(f"  ... and {len(urls_data) - 3} more")
    print()

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


# =============================================================================
# Tinybird Job Manager
# =============================================================================

TINYBIRD_JOBS_DATASOURCE = "scrape_jobs"


@dataclass
class JobRecord:
    """Represents a scrape job record for Tinybird."""
    jobId: str
    status: str  # pending, processing, completed, failed, partial
    totalUrls: int = 0
    completedUrls: int = 0
    failedUrls: int = 0
    withScreenshots: int = 0
    companyName: Optional[str] = None
    createdAt: Optional[str] = None
    startedAt: Optional[str] = None
    completedAt: Optional[str] = None
    updatedAt: Optional[str] = None
    durationMs: Optional[int] = None
    errorMessage: Optional[str] = None
    webhookUrl: Optional[str] = None
    webhookSent: int = 0
    webhookSentAt: Optional[str] = None
    primaryScraper: Optional[str] = None
    methodStats: Optional[str] = None  # JSON string
    requestIp: Optional[str] = None
    userAgent: Optional[str] = None
    metadata: Optional[str] = None  # JSON string

    def to_dict(self) -> dict:
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        result = {
            "jobId": self.jobId,
            "status": self.status,
            "totalUrls": self.totalUrls,
            "completedUrls": self.completedUrls,
            "failedUrls": self.failedUrls,
            "withScreenshots": self.withScreenshots,
            "webhookSent": self.webhookSent,
            "updatedAt": self.updatedAt or now,
        }
        # Add optional fields
        for field_name in [
            "companyName", "createdAt", "startedAt", "completedAt",
            "durationMs", "errorMessage", "webhookUrl", "webhookSentAt",
            "primaryScraper", "methodStats", "requestIp", "userAgent", "metadata"
        ]:
            value = getattr(self, field_name)
            if value is not None:
                result[field_name] = value
        return result


class TinybirdJobManager:
    """
    Manages scrape job records in Tinybird.

    Uses ReplacingMergeTree pattern: insert new rows to update status.
    The latest row (by updatedAt) is kept for each jobId.
    """

    def __init__(self):
        self._client = None

    async def _get_client(self):
        """Get or create async HTTP client."""
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(30.0))
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def _get_token(self) -> Optional[str]:
        """Get Tinybird token from environment."""
        return os.environ.get("TINYBIRD_TOKEN")

    async def insert_job(self, job: JobRecord) -> bool:
        """Insert or update a job record in Tinybird."""
        token = self._get_token()
        if not token:
            print("[TinybirdJobManager] ERROR: TINYBIRD_TOKEN not configured")
            return False

        client = await self._get_client()
        record = job.to_dict()

        try:
            response = await client.post(
                f"{TINYBIRD_HOST}/v0/events?name={TINYBIRD_JOBS_DATASOURCE}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                content=json.dumps(record, ensure_ascii=False),
            )

            if response.status_code in (200, 202):
                print(f"[TinybirdJobManager] Job {job.jobId} -> {job.status}")
                return True
            else:
                print(f"[TinybirdJobManager] ERROR: {response.status_code} - {response.text[:200]}")
                return False

        except Exception as e:
            print(f"[TinybirdJobManager] ERROR: {str(e)[:200]}")
            return False

    async def create_job(
        self,
        job_id: str,
        total_urls: int,
        company_name: Optional[str] = None,
        webhook_url: Optional[str] = None,
        request_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> JobRecord:
        """Create a new pending job."""
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        job = JobRecord(
            jobId=job_id,
            status="pending",
            totalUrls=total_urls,
            companyName=company_name,
            createdAt=now,
            updatedAt=now,
            webhookUrl=webhook_url,
            requestIp=request_ip,
            userAgent=user_agent,
            primaryScraper=get_primary_scraper(),
            metadata=json.dumps(metadata) if metadata else None,
        )
        await self.insert_job(job)
        return job

    async def start_job(self, job: JobRecord) -> JobRecord:
        """Mark job as processing."""
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        job.status = "processing"
        job.startedAt = now
        job.updatedAt = now
        await self.insert_job(job)
        return job

    async def complete_job(
        self,
        job: JobRecord,
        completed_urls: int,
        failed_urls: int,
        with_screenshots: int,
        method_stats: dict,
        error_message: Optional[str] = None,
    ) -> JobRecord:
        """Mark job as completed or partial."""
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        job.completedUrls = completed_urls
        job.failedUrls = failed_urls
        job.withScreenshots = with_screenshots
        job.completedAt = now
        job.updatedAt = now
        job.methodStats = json.dumps(method_stats)

        # Calculate duration
        if job.startedAt:
            try:
                start = datetime.strptime(job.startedAt, '%Y-%m-%d %H:%M:%S.%f')
                end = datetime.strptime(now, '%Y-%m-%d %H:%M:%S.%f')
                job.durationMs = int((end - start).total_seconds() * 1000)
            except Exception:
                pass

        # Determine final status
        if failed_urls == job.totalUrls:
            job.status = "failed"
            job.errorMessage = error_message or "All URLs failed"
        elif failed_urls > 0:
            job.status = "partial"
        else:
            job.status = "completed"

        await self.insert_job(job)
        return job

    async def fail_job(self, job: JobRecord, error_message: str) -> JobRecord:
        """Mark job as failed."""
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        job.status = "failed"
        job.errorMessage = error_message[:500]
        job.completedAt = now
        job.updatedAt = now

        if job.startedAt:
            try:
                start = datetime.strptime(job.startedAt, '%Y-%m-%d %H:%M:%S.%f')
                end = datetime.strptime(now, '%Y-%m-%d %H:%M:%S.%f')
                job.durationMs = int((end - start).total_seconds() * 1000)
            except Exception:
                pass

        await self.insert_job(job)
        return job

    async def send_webhook(self, job: JobRecord, results_summary: dict) -> bool:
        """Send webhook notification when job completes."""
        if not job.webhookUrl:
            return False

        client = await self._get_client()
        payload = {
            "jobId": job.jobId,
            "status": job.status,
            "totalUrls": job.totalUrls,
            "completedUrls": job.completedUrls,
            "failedUrls": job.failedUrls,
            "withScreenshots": job.withScreenshots,
            "durationMs": job.durationMs,
            "completedAt": job.completedAt,
            "methodStats": json.loads(job.methodStats) if job.methodStats else None,
            # Include results and alertsTriggered at top level
            "results": results_summary.get("results", []),
            "alertsTriggered": results_summary.get("alertsTriggered", []),
        }

        try:
            response = await client.post(
                job.webhookUrl,
                json=payload,
                timeout=10.0,
            )

            now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            job.webhookSent = 1 if response.status_code < 400 else 0
            job.webhookSentAt = now
            job.updatedAt = now
            await self.insert_job(job)

            if response.status_code < 400:
                print(f"[TinybirdJobManager] Webhook sent for job {job.jobId}")
                return True
            else:
                print(f"[TinybirdJobManager] Webhook failed: {response.status_code}")
                return False

        except Exception as e:
            print(f"[TinybirdJobManager] Webhook error: {str(e)[:100]}")
            return False


# =============================================================================
# Process Batch with Job Tracking
# =============================================================================

@app.function(
    image=scraper_image,
    timeout=7200,  # 2 hours for large batches
    secrets=[modal.Secret.from_name("bausch")],
)
async def process_batch_with_job(job_id: str, urls_data: List[dict], webhook_url: Optional[str] = None) -> dict:
    """
    Process batch with job tracking in Tinybird.

    This is the main entry point for the API. It:
    1. Updates job status to 'processing'
    2. Runs the scraping
    3. Updates job status to 'completed'/'partial'/'failed'
    4. Sends webhook if configured
    5. Returns summary (not full results - those go to product_scrapes)
    """
    job_manager = TinybirdJobManager()

    # Extract company name from first URL if available
    company_name = None
    if urls_data:
        company_name = urls_data[0].get("companyName")

    # Create job record
    job = JobRecord(
        jobId=job_id,
        status="pending",
        totalUrls=len(urls_data),
        companyName=company_name,
        webhookUrl=webhook_url,
        primaryScraper=get_primary_scraper(),
        createdAt=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
    )

    try:
        # Mark as processing
        job = await job_manager.start_job(job)

        # Run the actual batch processing
        results = await process_batch.local(urls_data)

        # Calculate stats
        completed = sum(1 for r in results if r.get("status") == "completed")
        failed = len(results) - completed
        with_screenshots = sum(1 for r in results if r.get("screenshotUrl"))

        method_stats = {
            "firecrawl": sum(1 for r in results if r.get("method") == METHOD_FIRECRAWL),
            "brightdata": sum(1 for r in results if r.get("method") == METHOD_BRIGHTDATA),
            "playwright": sum(1 for r in results if r.get("method") == METHOD_PLAYWRIGHT),
        }

        # Complete the job
        job = await job_manager.complete_job(
            job=job,
            completed_urls=completed,
            failed_urls=failed,
            with_screenshots=with_screenshots,
            method_stats=method_stats,
        )

        # Send webhook if configured
        if webhook_url:
            # Build detailed results for each URL
            webhook_results = []
            alerts_triggered = []

            for r in results:
                url_id = r.get("urlId")

                # Build result entry for this URL
                webhook_results.append({
                    "urlId": url_id,
                    "status": r.get("status"),
                    "hasPrice": bool(r.get("currentPrice")),
                    "hasScreenshot": bool(r.get("screenshotUrl")),
                    "currentPrice": r.get("currentPrice"),
                    "scrapedAt": r.get("scrapedAt"),
                    "errorMessage": r.get("errorMessage") if r.get("status") == "error" else None,
                })

                # Check for price alerts
                alert = check_price_alert(r)
                if alert:
                    alert["urlId"] = url_id
                    alerts_triggered.append(alert)

            results_summary = {
                "results": webhook_results,
                "alertsTriggered": alerts_triggered,
            }
            await job_manager.send_webhook(job, results_summary)

        await job_manager.close()

        return {
            "jobId": job_id,
            "status": job.status,
            "totalUrls": job.totalUrls,
            "completedUrls": job.completedUrls,
            "failedUrls": job.failedUrls,
            "withScreenshots": job.withScreenshots,
            "durationMs": job.durationMs,
            "methodStats": method_stats,
        }

    except Exception as e:
        # Mark job as failed
        await job_manager.fail_job(job, str(e))
        await job_manager.close()
        raise


# =============================================================================
# FastAPI Web Endpoints
# =============================================================================

# Image for the web API (lighter than scraper image)
api_image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi[standard]>=0.115.0",
        "httpx>=0.27.0",
        "pydantic>=2.0.0",
    )
)


@app.function(
    image=api_image,
    secrets=[modal.Secret.from_name("bausch")],
)
@modal.concurrent(max_inputs=100)
@modal.asgi_app(label="scraper-api")
def scraper_api():
    """
    FastAPI application for the web scraper API.

    Endpoints:
    - POST /api/v1/scrape - Submit a new scrape job
    - GET /api/v1/scrape/{job_id} - Get job status
    - GET /api/v1/scrape - List recent jobs
    - GET /health - Health check
    """
    from fastapi import FastAPI, HTTPException, Request, Query
    from fastapi.responses import JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
    from typing import List, Optional
    import httpx

    api = FastAPI(
        title="Modal Web Scraper API",
        description="Async web scraping API with job tracking",
        version="1.0.0",
    )

    # CORS middleware
    api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # =========================================================================
    # Pydantic Models
    # =========================================================================

    class UrlItem(BaseModel):
        urlId: str = Field(..., description="Unique identifier for this URL")
        url: Optional[str] = Field(None, description="URL to scrape")
        productUrl: Optional[str] = Field(None, description="Alternative field for URL")
        method: Optional[str] = Field(None, description="Preferred scraping method")
        companyName: Optional[str] = Field(None, description="Company name")
        alertId: Optional[str] = Field(None, description="Alert ID")
        # Categorization fields (Business, Channel, Family)
        businessId: Optional[str] = Field(None, description="Business ID for categorization")
        businessName: Optional[str] = Field(None, description="Business name for categorization")
        channelId: Optional[str] = Field(None, description="Channel ID for categorization")
        channelName: Optional[str] = Field(None, description="Channel name for categorization")
        familyId: Optional[str] = Field(None, description="Family ID for categorization")
        familyName: Optional[str] = Field(None, description="Family name for categorization")
        # Allow any additional fields
        model_config = {"extra": "allow"}

    class ScrapeRequest(BaseModel):
        urls: List[UrlItem] = Field(..., min_length=1, max_length=10000)
        webhookUrl: Optional[str] = Field(None, description="Webhook URL for completion notification")
        companyName: Optional[str] = Field(None, description="Company name for all URLs")

    class ScrapeResponse(BaseModel):
        jobId: str
        status: str
        totalUrls: int
        message: str

    class JobStatusResponse(BaseModel):
        jobId: str
        status: str
        totalUrls: int
        completedUrls: int
        failedUrls: int
        withScreenshots: int
        createdAt: Optional[str]
        startedAt: Optional[str]
        completedAt: Optional[str]
        durationMs: Optional[int]
        errorMessage: Optional[str]
        methodStats: Optional[dict]

    # =========================================================================
    # Helper Functions
    # =========================================================================

    async def query_tinybird(endpoint: str, params: dict) -> dict:
        """Query Tinybird API endpoint."""
        token = os.environ.get("TINYBIRD_TOKEN")
        if not token:
            raise HTTPException(500, "Tinybird not configured")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"{TINYBIRD_HOST}/v0/pipes/{endpoint}.json",
                params=params,
                headers={"Authorization": f"Bearer {token}"},
            )

            if response.status_code != 200:
                raise HTTPException(502, f"Tinybird query failed: {response.text[:200]}")

            return response.json()

    # =========================================================================
    # Endpoints
    # =========================================================================

    @api.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "service": "modal-web-scraper",
            "version": "3.3",
            "primaryScraper": os.environ.get("PRIMARY_SCRAPER", "firecrawl"),
        }

    @api.post("/api/v1/scrape", response_model=ScrapeResponse)
    async def submit_scrape(request: Request, body: ScrapeRequest):
        """
        Submit a new scrape job.

        The job runs asynchronously. Use GET /api/v1/scrape/{job_id} to check status.
        Results are stored in Tinybird product_scrapes table.
        """
        # Generate job ID
        job_id = f"job_{uuid.uuid4().hex[:12]}"

        # Prepare URLs data
        urls_data = []
        for item in body.urls:
            url_dict = item.model_dump(exclude_none=True)
            # Apply company name from request if not set per-URL
            if body.companyName and not url_dict.get("companyName"):
                url_dict["companyName"] = body.companyName
            urls_data.append(url_dict)

        # Get client info
        client_ip = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")

        # Create initial job record in Tinybird
        job_manager = TinybirdJobManager()
        job = await job_manager.create_job(
            job_id=job_id,
            total_urls=len(urls_data),
            company_name=body.companyName or (urls_data[0].get("companyName") if urls_data else None),
            webhook_url=body.webhookUrl,
            request_ip=client_ip,
            user_agent=user_agent,
        )
        await job_manager.close()

        # Spawn the batch processing (non-blocking)
        process_batch_with_job.spawn(job_id, urls_data, body.webhookUrl)

        return ScrapeResponse(
            jobId=job_id,
            status="pending",
            totalUrls=len(urls_data),
            message=f"Job submitted. Query GET /api/v1/scrape/{job_id} for status.",
        )

    @api.get("/api/v1/scrape/{job_id}", response_model=JobStatusResponse)
    async def get_job_status(job_id: str):
        """
        Get the status of a scrape job.

        Returns job metadata and progress. For full results, query the
        product_scrapes table in Tinybird filtered by time range.
        """
        try:
            result = await query_tinybird("get_job", {"job_id": job_id})
            data = result.get("data", [])

            if not data:
                raise HTTPException(404, f"Job {job_id} not found")

            job = data[0]

            # Parse methodStats if present
            method_stats = None
            if job.get("methodStats"):
                try:
                    method_stats = json.loads(job["methodStats"])
                except Exception:
                    pass

            return JobStatusResponse(
                jobId=job["jobId"],
                status=job["status"],
                totalUrls=job["totalUrls"],
                completedUrls=job["completedUrls"],
                failedUrls=job["failedUrls"],
                withScreenshots=job["withScreenshots"],
                createdAt=job.get("createdAt"),
                startedAt=job.get("startedAt"),
                completedAt=job.get("completedAt"),
                durationMs=job.get("durationMs"),
                errorMessage=job.get("errorMessage"),
                methodStats=method_stats,
            )

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(500, f"Error querying job: {str(e)[:200]}")

    @api.get("/api/v1/scrape")
    async def list_jobs(
        status: Optional[str] = Query(None, description="Filter by status"),
        company_name: Optional[str] = Query(None, description="Filter by company"),
        limit: int = Query(50, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """
        List recent scrape jobs with optional filtering.
        """
        params = {"limit": limit, "offset": offset}
        if status:
            params["status"] = status
        if company_name:
            params["company_name"] = company_name

        try:
            result = await query_tinybird("list_jobs", params)
            return {
                "jobs": result.get("data", []),
                "count": len(result.get("data", [])),
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            raise HTTPException(500, f"Error listing jobs: {str(e)[:200]}")

    @api.get("/api/v1/stats")
    async def get_stats(
        company_name: Optional[str] = Query(None),
        from_date: Optional[str] = Query(None, description="ISO date string"),
        to_date: Optional[str] = Query(None, description="ISO date string"),
    ):
        """
        Get aggregated job statistics.
        """
        params = {}
        if company_name:
            params["company_name"] = company_name
        if from_date:
            params["from_date"] = from_date
        if to_date:
            params["to_date"] = to_date

        try:
            result = await query_tinybird("job_stats", params)
            return {"stats": result.get("data", [])}
        except Exception as e:
            raise HTTPException(500, f"Error getting stats: {str(e)[:200]}")

    return api
