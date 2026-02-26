# TinyBird Dimension Tables - Manual Setup Instructions

Since this is a Tinybird Forward workspace that requires deployments, and the deployment mechanism isn't fully configured, you'll need to create the datasources and pipes manually through the TinyBird UI.

## NEW: Additional Dimension Tables for Companies, Families, Channels, and Business

The following 4 new dimension tables need to be created:

### Create companies_dimension Data Source

1. Go to TinyBird UI → Data Sources → Create Data Source
2. Choose "Create from scratch"
3. Name: `companies_dimension`
4. Paste this schema:

```sql
CREATE TABLE companies_dimension
(
    `companyId` String,
    `companyName` String,
    `website` String,
    `description` String,
    `updatedAt` DateTime
)
ENGINE = ReplacingMergeTree(updatedAt)
PRIMARY KEY (companyId, updatedAt)
ORDER BY (companyId, updatedAt)
```

### Create families_dimension Data Source

1. Go to TinyBird UI → Data Sources → Create Data Source
2. Choose "Create from scratch"
3. Name: `families_dimension`
4. Paste this schema:

```sql
CREATE TABLE families_dimension
(
    `familyId` String,
    `familyName` String,
    `channelId` String,
    `channelName` String,
    `description` String,
    `updatedAt` DateTime
)
ENGINE = ReplacingMergeTree(updatedAt)
PRIMARY KEY (familyId, updatedAt)
ORDER BY (familyId, updatedAt)
```

### Create channels_dimension Data Source

1. Go to TinyBird UI → Data Sources → Create Data Source
2. Choose "Create from scratch"
3. Name: `channels_dimension`
4. Paste this schema:

```sql
CREATE TABLE channels_dimension
(
    `channelId` String,
    `channelName` String,
    `businessId` String,
    `businessName` String,
    `description` String,
    `updatedAt` DateTime
)
ENGINE = ReplacingMergeTree(updatedAt)
PRIMARY KEY (channelId, updatedAt)
ORDER BY (channelId, updatedAt)
```

### Create business_dimension Data Source

1. Go to TinyBird UI → Data Sources → Create Data Source
2. Choose "Create from scratch"
3. Name: `business_dimension`
4. Paste this schema:

```sql
CREATE TABLE business_dimension
(
    `businessId` String,
    `businessName` String,
    `description` String,
    `updatedAt` DateTime
)
ENGINE = ReplacingMergeTree(updatedAt)
PRIMARY KEY (businessId, updatedAt)
ORDER BY (businessId, updatedAt)
```

---

## Step 1: Create products_dimension Data Source

1. Go to TinyBird UI → Data Sources → Create Data Source
2. Choose "Create from scratch"
3. Name: `products_dimension`
4. Paste this schema:

```sql
CREATE TABLE products_dimension
(
    `productId` String,
    `productName` String,
    `brandId` String,
    `brandName` String,
    `updatedAt` DateTime
)
ENGINE = ReplacingMergeTree(updatedAt)
PRIMARY KEY (productId, updatedAt)
ORDER BY (productId, updatedAt)
```

## Step 2: Create sellers_dimension Data Source

1. Go to TinyBird UI → Data Sources → Create Data Source
2. Choose "Create from scratch"
3. Name: `sellers_dimension`
4. Paste this schema:

```sql
CREATE TABLE sellers_dimension
(
    `sellerId` String,
    `sellerName` String,
    `country` String,
    `updatedAt` DateTime
)
ENGINE = ReplacingMergeTree(updatedAt)
PRIMARY KEY (sellerId, updatedAt)
ORDER BY (sellerId, updatedAt)
```

## Step 3: Create scrapes_by_product_id Pipe

1. Go to TinyBird UI → Pipes → Create Pipe
2. Name: `scrapes_by_product_id`
3. Paste this SQL:

```sql
SELECT
    s.*,
    p.productName as currentProductName
FROM product_scrapes s
LEFT JOIN (
    SELECT
        productId,
        argMax(productName, updatedAt) as productName
    FROM products_dimension
    GROUP BY productId
) p ON s.productId = p.productId
WHERE s.productId = {{ String(product_id, '') }}
ORDER BY s.scrapedAt DESC
LIMIT {{ Int32(limit, 1000) }}
```

4. Publish as API Endpoint

## Step 4: Create scrapes_with_current_names Pipe (Optional but Recommended)

1. Go to TinyBird UI → Pipes → Create Pipe
2. Name: `scrapes_with_current_names`
3. Paste this SQL:

```sql
SELECT
    s.urlId,
    s.scrapedAt,
    s.status,
    s.productId,
    s.sellerId,
    s.productName as historicalProductName,
    s.sellerName as historicalSellerName,
    p.productName as currentProductName,
    sel.sellerName as currentSellerName,
    if(s.productName != p.productName, 1, 0) as productNameChanged,
    if(s.sellerName != sel.sellerName, 1, 0) as sellerNameChanged,
    s.currentPrice,
    s.originalPrice,
    s.discountPercentage,
    s.currency,
    s.availability,
    s.imageUrl,
    s.screenshotUrl,
    s.productTitle,
    s.brandName,
    s.companyName,
    s.marketplaceWebsite,
    s.shippingInfo,
    s.shippingCost,
    s.deliveryTime,
    s.review_score,
    s.installmentOptions,
    s.kit,
    s.sku,
    s.ean,
    s.stockQuantity,
    s.errorMessage,
    s.minPrice,
    s.maxPrice,
    s.alertsEnabled,
    s.alertTriggered,
    s.alertType,
    s.productUrl,
    s.method
FROM product_scrapes s
LEFT JOIN (
    SELECT productId, argMax(productName, updatedAt) as productName
    FROM products_dimension GROUP BY productId
) p ON s.productId = p.productId
LEFT JOIN (
    SELECT sellerId, argMax(sellerName, updatedAt) as sellerName, argMax(country, updatedAt) as country
    FROM sellers_dimension GROUP BY sellerId
) sel ON s.sellerId = sel.sellerId
WHERE
    {% if defined(url_id) %}
        s.urlId = {{ String(url_id) }}
    {% end %}
    {% if defined(product_id) %}
        AND s.productId = {{ String(product_id) }}
    {% end %}
ORDER BY s.scrapedAt DESC
LIMIT {{ Int32(limit, 100) }}
```

4. Publish as API Endpoint

## Step 5: Run Backfill Scripts

After creating the datasources and pipes, populate them with existing data:

### From the project root directory:

```bash
# Development - Original dimension tables
npx convex run tinybirdDimensions:runProductsBackfill
npx convex run tinybirdDimensions:runSellersBackfill

# Development - NEW dimension tables
npx convex run tinybirdDimensions:runCompaniesBackfill
npx convex run tinybirdDimensions:runFamiliesBackfill
npx convex run tinybirdDimensions:runChannelsBackfill
npx convex run tinybirdDimensions:runBusinessBackfill

# Production - Original dimension tables
npx convex run tinybirdDimensions:runProductsBackfill --prod
npx convex run tinybirdDimensions:runSellersBackfill --prod

# Production - NEW dimension tables
npx convex run tinybirdDimensions:runCompaniesBackfill --prod
npx convex run tinybirdDimensions:runFamiliesBackfill --prod
npx convex run tinybirdDimensions:runChannelsBackfill --prod
npx convex run tinybirdDimensions:runBusinessBackfill --prod
```

## Step 6: Test the System

1. Create a new product in the app → Check if it appears in `products_dimension`
2. Update a product name → Check if dimension table is updated
3. Open ProductAnalytics for a product → Verify it shows scrapes correctly
4. Change a product name again → Verify analytics still shows ALL scrapes (old + new)

## Verification Queries

Run these in TinyBird SQL Console to verify everything is working:

```sql
-- Check products_dimension has data
SELECT count() FROM products_dimension;

-- Check sellers_dimension has data
SELECT count() FROM sellers_dimension;

-- Check NEW dimension tables have data
SELECT count() FROM companies_dimension;
SELECT count() FROM families_dimension;
SELECT count() FROM channels_dimension;
SELECT count() FROM business_dimension;

-- Test the JOIN (should show both historical and current names)
SELECT
    s.productId,
    s.productName as historical,
    p.productName as current,
    if(s.productName != p.productName, 'CHANGED', 'SAME') as status
FROM product_scrapes s
LEFT JOIN (
    SELECT productId, argMax(productName, updatedAt) as productName
    FROM products_dimension GROUP BY productId
) p ON s.productId = p.productId
LIMIT 10;
```

## What This Achieves

✅ **Historical data preserved**: Scrapes keep original product/seller names
✅ **Current names available**: Dimension tables have latest names
✅ **Analytics work perfectly**: ProductAnalytics finds scrapes by ID, not name
✅ **No data duplication**: Only dimension tables need updates, not millions of scrapes
✅ **Scalable**: Works efficiently even with millions of scrapes

## Troubleshooting

**If pipes fail to create:**
- Make sure datasources are created first
- Check that field names match exactly (case-sensitive)
- Verify product_scrapes table has productId and sellerId columns

**If backfill fails:**
- Check Convex environment variables are set (TINYBIRD_ADMIN_TOKEN)
- Verify datasources exist in TinyBird
- Check Convex logs for detailed error messages
