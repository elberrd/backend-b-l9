# Integração Tinybird: Arquitetura CQRS para Analytics

## Visão Geral

Esta documentação descreve a integração entre o sistema atual (Convex) e o Tinybird para analytics de scraping. A arquitetura segue o padrão CQRS (Command Query Responsibility Segregation), separando o banco transacional (Convex) do banco analítico (Tinybird).

---

## Por Que Separar?

| Problema Atual | Solução |
|----------------|---------|
| Convex fica lento com milhares de scrapes | Tinybird usa ClickHouse, otimizado para milhões de rows |
| Agregações pesadas travam o sistema | Tinybird processa queries analíticas em milissegundos |
| Custo de storage no Convex | Tinybird é mais barato para grandes volumes |

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        APLICATIVO                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  CONVEX (Transacional)              TINYBIRD (Analytics)        │
│  ────────────────────               ──────────────────          │
│  • URLs                             • productScrapes            │
│  • Products                         • Histórico de preços       │
│  • Sellers                          • Agregações                │
│  • Users/Auth                       • Dashboards                │
│  • scrapeJobs (status)              • Tendências                │
│  • Alertas (config)                 • Relatórios                │
│                                                                 │
│         ↓                                    ↑                  │
│    Reatividade                          API REST                │
│    (real-time)                         (polling)                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## O Que Fica em Cada Banco

### Convex (Dados Transacionais e Estado)

Mantém tudo que precisa de reatividade e relacionamentos:

- **users** - Autenticação e perfis
- **products** - Catálogo de produtos
- **sellers** - Vendedores
- **brands** - Marcas
- **urls** - URLs para scraping (metadados)
- **tags** - Sistema de tags
- **alerts** - Configuração de alertas de preço
- **scrapeJobs** - Estado dos jobs de scraping (loading, completed, error)

### Tinybird (Dados Analíticos)

Armazena todos os dados de scraping para analytics:

- **productScrapes** - Dados completos de cada scrape
- Histórico de preços por período
- Agregações pré-calculadas
- Dados para dashboards e relatórios

---

## Nova Tabela no Convex: scrapeJobs

Esta tabela leve controla apenas o estado dos jobs, permitindo reatividade no frontend:

```typescript
// convex/schema.ts
scrapeJobs: defineTable({
  urlId: v.id("urls"),
  status: v.union(
    v.literal("pending"),
    v.literal("running"),
    v.literal("completed"),
    v.literal("error")
  ),
  startedAt: v.number(),
  completedAt: v.optional(v.number()),

  // Metadados leves (não os dados completos)
  hasPrice: v.optional(v.boolean()),
  hasScreenshot: v.optional(v.boolean()),
  errorMessage: v.optional(v.string()),

  // Para bulk operations
  batchId: v.optional(v.string()),
})
.index("by_urlId", ["urlId"])
.index("by_status", ["status"])
.index("by_batchId", ["batchId"])
```

---

## Tabela no Tinybird: productScrapes

Armazena todos os dados de scraping:

```sql
-- Schema Tinybird (ClickHouse)
CREATE TABLE productScrapes (
  -- Identificadores (vêm do Convex)
  urlId String,
  scrapeJobId String,

  -- Timestamps
  scrapedAt DateTime64(3),

  -- Dados do produto
  productTitle String,
  brand String,
  currentPrice Float64,
  originalPrice Float64,
  discountPercentage Float64,
  currency String,
  availability UInt8,

  -- Imagens
  imageUrl String,
  screenshotUrl String,

  -- Vendedor e entrega
  seller String,
  sellerRating String,
  shippingInfo String,
  shippingCost Float64,
  deliveryTime String,
  freightDelay String,

  -- Avaliações
  review_score String,
  reviewCount UInt32,

  -- Pagamento
  installmentOptions String,
  otherPaymentMethods String,
  promotionDetails String,

  -- Identificadores do produto
  sku String,
  ean String,
  productUrl String,
  marketplaceWebsite String,

  -- Outros
  kit UInt8,
  stockQuantity UInt32,
  outOfStockReason String,

  -- Metadados
  errorMessage String,
  metadata String
)
ENGINE = MergeTree()
ORDER BY (urlId, scrapedAt)
```

---

## Fluxo Completo de Scraping

### Passo 1: Usuário Inicia Scraping

```
Frontend                          Convex
────────                          ──────
Clica "Scrape"
      │
      ▼
      ───────────────────────────► mutation: createScrapeJob
                                        │
                                        ▼
                                  Cria registro:
                                  {
                                    _id: "j1abc123",
                                    urlId: "k57xyz789",
                                    status: "running",
                                    startedAt: Date.now()
                                  }
                                        │
      ◄───────────────────────────────────
      │
Recebe via reatividade
      │
      ▼
Mostra "Loading..."
```

### Passo 2: Convex Dispara para Backend

```
Convex                            Backend Python
──────                            ──────────────
action: triggerBulkScrape
      │
      ▼
Envia para backend:
{
  "urls": [
    {
      "urlId": "k57xyz789",
      "scrapeJobId": "j1abc123",
      "url": "https://amazon.com.br/..."
    }
  ],
  "callbackUrl": "https://app.convex.site/api/scrapes/callback",
  "callbackToken": "secret_token"
}
      │
      ───────────────────────────────────►
                                          │
                                          ▼
                                    Processa scraping
                                    (Playwright + IA)
```

### Passo 3: Backend Salva no Tinybird

```
Backend Python                    Tinybird
──────────────                    ────────
Após processar:
      │
      ▼
POST Events API:
{
  "urlId": "k57xyz789",
  "scrapeJobId": "j1abc123",
  "scrapedAt": 1705320000000,
  "productTitle": "Echo Dot 5ª Geração",
  "currentPrice": 284.05,
  "originalPrice": 399.00,
  "screenshotUrl": "https://r2.../abc.webp",
  ...
}
      │
      ───────────────────────────────────►
                                          │
                                          ▼
                                    Armazena dados
                                    (sem retornar ID)
```

### Passo 4: Backend Notifica Convex

```
Backend Python                    Convex
──────────────                    ──────
      │
      ▼
POST callback:
{
  "scrapeJobId": "j1abc123",
  "status": "completed",
  "hasPrice": true,
  "hasScreenshot": true
}
      │
      ───────────────────────────────────►
                                          │
                                          ▼
                                    HTTP Action recebe
                                          │
                                          ▼
                                    Atualiza scrapeJob:
                                    {
                                      status: "completed",
                                      completedAt: Date.now(),
                                      hasPrice: true,
                                      hasScreenshot: true
                                    }
```

### Passo 5: Frontend Atualiza e Busca Dados

```
Frontend                          Convex              Tinybird
────────                          ──────              ────────
      │
      ◄─────────────────────────── Reatividade:
      │                           status = "completed"
      │
      ▼
Para de mostrar loading
      │
      ▼
Precisa ver dados?
      │
      ───────────────────────────────────────────────►
      │                                               │
      │                           GET /pipes/get_scrape.json
      │                               ?scrapeJobId=j1abc123
      │                                               │
      ◄───────────────────────────────────────────────
      │
      ▼
Exibe dados completos
```

---

## Identificação de Dados (Sem Row ID)

O Tinybird/ClickHouse não tem IDs de linha auto-incrementados. Usamos nossos próprios identificadores:

| Campo | Origem | Uso |
|-------|--------|-----|
| urlId | Convex | Identifica a URL sendo scraped |
| scrapeJobId | Convex | Identifica o job específico |
| scrapedAt | Backend | Timestamp do scrape |

### Buscas Comuns

```sql
-- Último scrape de uma URL
SELECT * FROM productScrapes
WHERE urlId = 'k57xyz789'
ORDER BY scrapedAt DESC
LIMIT 1

-- Scrape específico por job
SELECT * FROM productScrapes
WHERE scrapeJobId = 'j1abc123'

-- Histórico de preços
SELECT scrapedAt, currentPrice, originalPrice
FROM productScrapes
WHERE urlId = 'k57xyz789'
ORDER BY scrapedAt ASC

-- Agregação por período
SELECT
  toDate(scrapedAt) as date,
  avg(currentPrice) as avgPrice,
  min(currentPrice) as minPrice,
  max(currentPrice) as maxPrice
FROM productScrapes
WHERE urlId = 'k57xyz789'
GROUP BY date
ORDER BY date
```

---

## APIs do Tinybird (Pipes)

Cada query SQL vira uma API REST automaticamente:

### Pipe: get_latest_scrape

```sql
SELECT *
FROM productScrapes
WHERE urlId = {{ String(urlId) }}
ORDER BY scrapedAt DESC
LIMIT 1
```

**Uso:**
```
GET https://api.tinybird.co/v0/pipes/get_latest_scrape.json?urlId=k57xyz789
Authorization: Bearer TB_TOKEN
```

### Pipe: get_scrape_by_job

```sql
SELECT *
FROM productScrapes
WHERE scrapeJobId = {{ String(scrapeJobId) }}
LIMIT 1
```

**Uso:**
```
GET https://api.tinybird.co/v0/pipes/get_scrape_by_job.json?scrapeJobId=j1abc123
Authorization: Bearer TB_TOKEN
```

### Pipe: get_price_history

```sql
SELECT
  scrapedAt,
  currentPrice,
  originalPrice,
  discountPercentage
FROM productScrapes
WHERE urlId = {{ String(urlId) }}
ORDER BY scrapedAt ASC
```

**Uso:**
```
GET https://api.tinybird.co/v0/pipes/get_price_history.json?urlId=k57xyz789
Authorization: Bearer TB_TOKEN
```

### Pipe: get_scrapes_by_batch

```sql
SELECT *
FROM productScrapes
WHERE scrapeJobId IN (
  SELECT scrapeJobId
  FROM productScrapes
  WHERE urlId IN {{ Array(urlIds, 'String') }}
)
ORDER BY scrapedAt DESC
```

---

## Integração no Frontend

### Hook para Buscar Dados do Tinybird

```typescript
// hooks/useTinybirdScrape.ts
import { useQuery } from "convex/react";
import { api } from "@/convex/_generated/api";
import { useState, useEffect } from "react";

export function useScrapeData(urlId: string) {
  const [scrapeData, setScrapeData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  // Estado do job vem do Convex (reativo)
  const scrapeJob = useQuery(api.scrapeJobs.getLatestByUrlId, { urlId });

  // Quando job completa, busca dados do Tinybird
  useEffect(() => {
    if (scrapeJob?.status === "completed") {
      fetchFromTinybird(urlId)
        .then(data => {
          setScrapeData(data);
          setIsLoading(false);
        });
    } else if (scrapeJob?.status === "error") {
      setIsLoading(false);
    } else if (scrapeJob?.status === "running") {
      setIsLoading(true);
    }
  }, [scrapeJob?.status, urlId]);

  return {
    isLoading: scrapeJob?.status === "running",
    isError: scrapeJob?.status === "error",
    errorMessage: scrapeJob?.errorMessage,
    data: scrapeData,
    hasPrice: scrapeJob?.hasPrice,
    hasScreenshot: scrapeJob?.hasScreenshot,
  };
}

async function fetchFromTinybird(urlId: string) {
  const response = await fetch(
    `${process.env.NEXT_PUBLIC_TINYBIRD_URL}/v0/pipes/get_latest_scrape.json?urlId=${urlId}`,
    {
      headers: {
        Authorization: `Bearer ${process.env.NEXT_PUBLIC_TINYBIRD_TOKEN}`,
      },
    }
  );
  const json = await response.json();
  return json.data[0];
}
```

### Componente de Exibição

```typescript
// components/ScrapeResult.tsx
import { useScrapeData } from "@/hooks/useTinybirdScrape";

export function ScrapeResult({ urlId }: { urlId: string }) {
  const { isLoading, isError, errorMessage, data } = useScrapeData(urlId);

  if (isLoading) {
    return <div className="animate-pulse">Processando scrape...</div>;
  }

  if (isError) {
    return <div className="text-red-500">Erro: {errorMessage}</div>;
  }

  if (!data) {
    return <div>Nenhum dado disponível</div>;
  }

  return (
    <div>
      <h2>{data.productTitle}</h2>
      <p>Preço: {data.currency} {data.currentPrice}</p>
      {data.screenshotUrl && (
        <img src={data.screenshotUrl} alt="Screenshot" />
      )}
    </div>
  );
}
```

---

## Formato do Callback (Backend → Convex)

O backend Python envia para o Convex apenas metadados leves:

```json
{
  "scrapeJobId": "j1abc123",
  "status": "completed",
  "completedAt": 1705320000000,
  "hasPrice": true,
  "hasScreenshot": true,
  "errorMessage": null
}
```

**OU em caso de erro:**

```json
{
  "scrapeJobId": "j1abc123",
  "status": "error",
  "completedAt": 1705320000000,
  "hasPrice": false,
  "hasScreenshot": true,
  "errorMessage": "Could not find the price"
}
```

---

## Variáveis de Ambiente

### Convex (.env.local)

```env
# Tinybird (para o frontend buscar dados)
NEXT_PUBLIC_TINYBIRD_URL=https://api.tinybird.co
NEXT_PUBLIC_TINYBIRD_TOKEN=p.eyJ...

# Backend Python
BULK_SCRAPER_URL=https://seu-backend.com/api/scrape
BULK_SCRAPER_TOKEN=token_secreto

# Webhook secret (backend → Convex)
CONVEX_WEBHOOK_SECRET=whsec_...
```

### Backend Python (.env)

```env
# Tinybird (para inserir dados)
TINYBIRD_URL=https://api.tinybird.co
TINYBIRD_TOKEN=p.eyJ...
TINYBIRD_DATASOURCE=productScrapes

# Convex (para callback)
CONVEX_CALLBACK_URL=https://app.convex.site/api/scrapes/callback
CONVEX_CALLBACK_TOKEN=whsec_...

# Cloudflare R2
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET_NAME=screenshots
R2_PUBLIC_URL=https://pub-xxx.r2.dev
```

---

## Limites do Tinybird

| Limite | Valor |
|--------|-------|
| Tamanho por request (Events API) | 10 MB (Free) / 100 MB (Pago) |
| Requests por segundo | 100 req/s |
| Rows por request | Sem limite (só MB) |
| Query timeout | 10 segundos |
| Resposta máxima | 100 MB |

### Estimativa para Scrapes

Se cada scrape tem ~10 KB:
- 10 MB = ~1.000 scrapes por request
- 100 req/s = ~100.000 scrapes por segundo (teórico)

Na prática, com batches de 50:
- 50 scrapes × 10 KB = 500 KB por request
- Bem dentro dos limites

---

## Migração de Dados Existentes

Para migrar scrapes antigos do Convex para Tinybird:

1. Exportar dados do Convex via query
2. Transformar para formato NDJSON
3. Upload via Tinybird Data Sources API
4. Validar contagem de registros
5. Manter dados antigos no Convex como backup (opcional)

---

## Vantagens da Arquitetura

1. **Reatividade mantida**: Loading states via Convex
2. **Analytics rápido**: Milhões de rows no Tinybird
3. **Custo otimizado**: Convex leve, Tinybird escala
4. **Separação clara**: Transacional vs Analítico
5. **Flexibilidade**: Queries SQL complexas no Tinybird
6. **Escalabilidade**: ClickHouse aguenta petabytes
