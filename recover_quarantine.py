#!/usr/bin/env python3
"""
Recover quarantined rows from Tinybird product_scrapes_quarantine.
Fixes type mismatches and re-inserts into product_scrapes.
"""

import json
import os
import re
import sys
import requests

# Tinybird config
TINYBIRD_URL = "https://api.us-east.tinybird.co"

# Valid columns in the product_scrapes schema (must match datasource definition)
VALID_COLUMNS = {
    "urlId", "productUrl", "status", "scrapedAt", "errorMessage", "screenshotUrl",
    "productTitle", "productName", "brandName", "currentPrice", "originalPrice",
    "discountPercentage", "currency", "availability", "imageUrl", "sellerName",
    "seller", "shippingInfo", "shippingCost", "deliveryTime", "review_score",
    "installmentOptions", "kit", "unitMeasurement", "outOfStockReason",
    "marketplaceWebsite", "sku", "ean", "stockQuantity", "otherPaymentMethods",
    "promotionDetails", "method", "attempts", "errors", "alertId", "companyName",
    "hasAlert", "screenshotId", "productId", "sellerId", "minPrice", "maxPrice",
    "alertsEnabled", "businessId", "businessName", "channelId", "channelName",
    "familyId", "familyName",
}

# Quarantine metadata columns to strip
QUARANTINE_COLUMNS = {"c__error", "c__error_column", "c__import_id", "insertion_date"}


def tb_sql(token, query):
    """Execute a Tinybird SQL query and return parsed JSON result."""
    resp = requests.get(
        f"{TINYBIRD_URL}/v0/sql",
        params={"q": f"{query} FORMAT JSON"},
        headers={"Authorization": f"Bearer {token}"},
    )
    if resp.status_code != 200:
        raise Exception(f"Tinybird SQL error {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def to_string(value):
    """Convert a value to string for Nullable(String) columns."""
    if value is None:
        return None
    if isinstance(value, list):
        return " | ".join(str(item) for item in value) if value else None
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def to_float(value):
    """Convert a value to float for Float64 columns."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", ".").strip()
        match = re.search(r'[\d]+\.?[\d]*', cleaned)
        if match:
            try:
                return float(match.group())
            except (ValueError, TypeError):
                return None
    return None


def to_int(value):
    """Convert a value to int for Int32 columns."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        match = re.search(r'[\d]+', value)
        if match:
            try:
                return int(match.group())
            except (ValueError, TypeError):
                return None
    return None


def fix_row(row):
    """Apply type coercions to a quarantined row and strip metadata columns."""
    fixed = {}
    for key, value in row.items():
        # Skip quarantine metadata and unknown columns
        if key in QUARANTINE_COLUMNS:
            continue
        if key not in VALID_COLUMNS:
            continue
        # Skip null/empty values
        if value is None or value == "":
            continue

        # Apply type-specific fixes
        if key in ("promotionDetails", "installmentOptions", "otherPaymentMethods", "sku"):
            fixed[key] = to_string(value)
        elif key in ("shippingCost", "currentPrice", "originalPrice", "discountPercentage", "minPrice", "maxPrice"):
            fixed[key] = to_float(value)
        elif key in ("stockQuantity",):
            fixed[key] = to_int(value)
        elif key in ("availability", "kit", "hasAlert", "alertsEnabled"):
            if isinstance(value, str):
                if value.lower() in ("true", "1", "yes"):
                    fixed[key] = 1
                elif value.lower() in ("false", "0", "no"):
                    fixed[key] = 0
                else:
                    fixed[key] = None
            else:
                fixed[key] = 1 if value else 0
        else:
            fixed[key] = value

    # Remove None values
    return {k: v for k, v in fixed.items() if v is not None}


def get_tinybird_token():
    """Get Tinybird token from environment or .tinyb config."""
    token = os.environ.get("TINYBIRD_TOKEN")
    if token:
        return token

    # Try reading from .tinyb (JSON format)
    tinyb_path = os.path.join(os.path.dirname(__file__), ".tinyb")
    if os.path.exists(tinyb_path):
        with open(tinyb_path) as f:
            config = json.load(f)
            return config.get("token")

    return None


def main():
    token = get_tinybird_token()
    if not token:
        print("ERROR: No Tinybird token found")
        sys.exit(1)

    print(f"Using Tinybird API at {TINYBIRD_URL}\n")

    # --- Before counts ---
    result = tb_sql(token, "SELECT count() as cnt FROM product_scrapes")
    before_count = result["data"][0]["cnt"]
    print(f"Before: {before_count} rows in product_scrapes")

    result = tb_sql(token, "SELECT count() as cnt FROM product_scrapes_quarantine")
    quarantine_before = result["data"][0]["cnt"]
    print(f"Before: {quarantine_before} rows in quarantine\n")

    # --- Step 1: Export ---
    print("[1/3] Exporting quarantined rows...")
    all_rows = []
    offset = 0
    batch_size = 500

    while True:
        result = tb_sql(token, f"SELECT * FROM product_scrapes_quarantine ORDER BY insertion_date LIMIT {batch_size} OFFSET {offset}")
        rows = result.get("data", [])
        all_rows.extend(rows)
        print(f"  Exported {len(all_rows)} rows...")
        if len(rows) < batch_size:
            break
        offset += batch_size

    print(f"  Total: {len(all_rows)} rows exported\n")

    if not all_rows:
        print("No quarantined rows found!")
        return

    # --- Step 2: Fix and insert ---
    print(f"[2/3] Fixing and inserting {len(all_rows)} rows...")

    fixed_rows = []
    skipped = 0
    for row in all_rows:
        try:
            fixed = fix_row(row)
            if fixed.get("urlId") and fixed.get("scrapedAt"):
                fixed_rows.append(fixed)
            else:
                skipped += 1
        except Exception as e:
            skipped += 1
            print(f"  ERROR: {e}")

    if skipped:
        print(f"  {skipped} rows skipped (missing required fields)")
    print(f"  {len(fixed_rows)} rows ready to insert")

    # Insert in batches
    insert_batch = 200
    total_inserted = 0
    total_quarantined = 0

    for i in range(0, len(fixed_rows), insert_batch):
        batch = fixed_rows[i:i + insert_batch]
        ndjson = "\n".join(json.dumps(row, ensure_ascii=False) for row in batch)

        resp = requests.post(
            f"{TINYBIRD_URL}/v0/events?name=product_scrapes&format=ndjson",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/x-ndjson",
            },
            data=ndjson.encode("utf-8"),
        )

        if resp.status_code not in (200, 202):
            print(f"  ERROR batch {i//insert_batch + 1}: {resp.status_code} - {resp.text[:200]}")
            continue

        result = resp.json()
        inserted = result.get("successful_rows", 0)
        quarantined = result.get("quarantined_rows", 0)
        total_inserted += inserted
        total_quarantined += quarantined
        print(f"  Batch {i//insert_batch + 1}: {inserted} inserted, {quarantined} quarantined")

    print(f"  TOTAL: {total_inserted} inserted, {total_quarantined} re-quarantined\n")

    # --- Step 3: Verify ---
    print("[3/3] Verifying...")
    result = tb_sql(token, "SELECT count() as cnt FROM product_scrapes")
    after_count = result["data"][0]["cnt"]

    result = tb_sql(token, "SELECT count() as cnt FROM product_scrapes_quarantine")
    quarantine_after = result["data"][0]["cnt"]

    print(f"  Normal table: {before_count} -> {after_count} (+{after_count - before_count})")
    print(f"  Quarantine:   {quarantine_before} -> {quarantine_after} (+{quarantine_after - quarantine_before})\n")

    # --- Summary ---
    print("=" * 50)
    print("RECOVERY SUMMARY")
    print("=" * 50)
    print(f"  Exported from quarantine:  {len(all_rows)}")
    print(f"  Successfully re-inserted:  {total_inserted}")
    print(f"  Re-quarantined (bad data): {total_quarantined}")
    print(f"  Skipped (missing fields):  {skipped}")
    print(f"  Normal table: {before_count} -> {after_count}")
    print(f"  Net new rows: {after_count - before_count}")
    if total_quarantined > 0:
        print(f"\n  WARNING: {total_quarantined} rows were quarantined again!")
    if after_count - before_count == total_inserted:
        print(f"\n  SUCCESS: All {total_inserted} rows verified in normal table!")


if __name__ == "__main__":
    main()
