# Especificação Técnica: Backend de Scraping em Massa (Versão Final)

## Visão Geral

Este backend em Python recebe uma lista de URLs de produtos, processa cada uma utilizando Playwright como método principal (extraindo dados via IA e capturando screenshot simultaneamente), com fallback para Bright Data quando necessário. Os screenshots são salvos no Cloudflare R2 e todos os resultados são enviados via callback em lotes de até 50 itens.

---

## Arquivos de Referência

O sistema será implementado em um **único arquivo Python** (`main.py`) que incorpora a lógica dos seguintes arquivos de referência:

| Operação | Primeira Tentativa (Playwright) | Segunda Tentativa (Bright Data) |
|----------|--------------------------------|--------------------------------|
| **Web Scraping** | `ref/url-pw.py` | `ref/url-bd.py` |
| **Screenshot** | `ref/screen-pw.py` | `ref/screen-bd.py` |

### Descrição dos Arquivos de Referência

**`ref/url-pw.py`** - Extração de dados via Playwright
- Usa Playwright headless Chromium com proxy Webshare
- Limpa HTML preservando scripts importantes (ld+json, __NEXT_DATA__, vtex, etc.)
- Envia HTML limpo para Gemini AI (`gemini-2.5-flash`) para extração de dados
- Timeout: **1 minuto e meio (90s)**

**`ref/screen-pw.py`** - Screenshot via Playwright
- Usa Playwright headless Chromium com proxy Webshare
- Captura screenshot full-page em PNG
- Comprime para JPEG com qualidade configurável (padrão 85)
- Timeout: **3 minutos (180s)**

**`ref/url-bd.py`** - Extração de dados via Bright Data (fallback)
- Usa Bright Data API (`/request` endpoint) para fetch de HTML
- Mesma lógica de limpeza de HTML do Playwright
- Envia HTML limpo para Gemini AI para extração de dados
- Timeout: **1 minuto e meio (90s)**

**`ref/screen-bd.py`** - Screenshot via Bright Data (fallback)
- Usa Bright Data API com `data_format: "screenshot"`
- Retry com backoff exponencial e jitter (até 4 tentativas)
- Comprime para JPEG com qualidade configurável
- Timeout: **3 minutos (180s)**

---

## 1. Requisição de Entrada

O sistema recebe uma requisição POST com a lista de URLs e informações de callback.

**Exemplo:**

```json
{
  "urls": [
    {
      "urlId": "k57abc123def456",
      "url": "https://www.amazon.com.br/dp/B08XYZ123"
    },
    {
      "urlId": "k57xyz789ghi012",
      "url": "https://www.mercadolivre.com.br/p/MLB12345"
    }
  ],
  "callbackUrl": "https://meu-app.convex.site/api/scrapes/bulk",
  "callbackToken": "sk_live_abc123xyz789"
}
```

---

## 2. Resposta Imediata

O sistema confirma o recebimento e inicia o processamento em background.

```json
{
  "success": true,
  "jobId": "job_2024_01_15_abc123",
  "message": "Processing started for 2 URLs"
}
```

---

## 3. Processamento de Cada URL

Para cada URL, o sistema executa um processo completo com duas tentativas possíveis.

### Primeira Tentativa: Playwright (Principal)

O Playwright abre a página e executa duas operações em paralelo dentro do mesmo contexto de navegador:

**Operação A: Extração de Dados** (baseado em `ref/url-pw.py`)
- Aguarda a página carregar completamente (domcontentloaded + networkidle)
- Limpa HTML preservando scripts importantes (ld+json, __NEXT_DATA__, vtex)
- Envia HTML limpo para Gemini AI (`gemini-2.5-flash`)
- Verifica se encontrou currentPrice ou originalPrice
- Timeout: **1 minuto e meio (90s)**

**Operação B: Captura de Screenshot** (baseado em `ref/screen-pw.py`)
- Bloqueia pop-ups, modais e banners
- Captura screenshot full-page em PNG
- Comprime para JPEG (qualidade 85)
- Faz upload para o Cloudflare R2
- Timeout: **3 minutos (180s)**

Ambas as operações rodam em paralelo, aproveitando que a página já está aberta no Playwright.

### Resultados Possíveis da Primeira Tentativa

| Dados | Screenshot | Ação |
|-------|------------|------|
| OK | OK | Finalizado. Envia para fila de callback. |
| OK | Falhou | Guarda dados. Tenta screenshot via Bright Data (`screen-bd.py`). |
| Falhou | OK | Guarda screenshot. Tenta dados via Bright Data (`url-bd.py`). |
| Falhou | Falhou | Tenta ambos via Bright Data. |

### Segunda Tentativa: Bright Data (Fallback)

Apenas para as operações que falharam na primeira tentativa:

**Para dados (se necessário):** (baseado em `ref/url-bd.py`)
- Chama Bright Data API para fetch de HTML
- Limpa HTML e envia para Gemini AI
- Timeout: **1 minuto e meio (90s)**
- Se não encontrar preço, marca como erro "Could not find the price"

**Para screenshot (se necessário):** (baseado em `ref/screen-bd.py`)
- Chama Bright Data API com `data_format: "screenshot"`
- Retry com backoff exponencial (até 4 tentativas)
- Comprime para JPEG e faz upload para o Cloudflare R2
- Timeout: **3 minutos (180s)**

---

## 4. Diagrama do Fluxo por URL

```
URL recebida
    │
    ▼
┌─────────────────────────────────────────┐
│ PRIMEIRA TENTATIVA (Playwright)         │
│                                         │
│ Abre página no navegador (Webshare)     │
│         │                               │
│    ┌────┴────┐                          │
│    ▼         ▼                          │
│ [Dados]   [Screenshot]   ← em paralelo  │
│ url-pw.py  screen-pw.py                 │
│    │         │                          │
│    ▼         ▼                          │
│ Resultado  Resultado                    │
└─────────────────────────────────────────┘
    │
    ▼
Avalia resultados
    │
    ├── Ambos OK ──────────────────────────────► Envia para fila
    │
    ├── Dados OK + Screenshot falhou ──────────┐
    │                                          │
    ├── Dados falhou + Screenshot OK ──────────┤
    │                                          │
    └── Ambos falharam ────────────────────────┤
                                               │
                                               ▼
                        ┌─────────────────────────────────────────┐
                        │ SEGUNDA TENTATIVA (Bright Data)         │
                        │                                         │
                        │ Apenas para operações que falharam:     │
                        │                                         │
                        │ • Se dados falhou → url-bd.py           │
                        │ • Se screen falhou → screen-bd.py       │
                        └─────────────────────────────────────────┘
                                               │
                                               ▼
                                       Combina resultados
                                       (usa o que funcionou)
                                               │
                                               ▼
                                       Envia para fila
```

---

## 5. Fila de Resultados e Batching

Os resultados são acumulados antes de serem enviados ao callback.

**Regras:**
- Acumula até 50 resultados antes de enviar
- Se passar 5 segundos sem atingir 50, envia o que tiver
- Retry com intervalos crescentes em caso de falha (1s, 5s, 30s)

---

## 6. Formato do Callback

**Headers:**
```
Authorization: Bearer sk_live_abc123xyz789
Content-Type: application/json
```

**Exemplo de callback com sucesso total:**

```json
{
  "batchId": "batch_001",
  "processedAt": 1705320000000,
  "scrapes": [
    {
      "urlId": "k57abc123def456",
      "status": "completed",
      "screenshotUrl": "https://pub-xxx.r2.dev/screenshots/2024/01/abc123.webp",
      "scrapedAt": 1705319950000,
      "productTitle": "Echo Dot 5ª Geração",
      "brand": "Amazon",
      "currentPrice": 284.05,
      "originalPrice": 399.00,
      "discountPercentage": 29,
      "currency": "BRL",
      "availability": true,
      "imageUrl": "https://m.media-amazon.com/images/I/xxx.jpg",
      "seller": "Amazon.com.br",
      "shippingInfo": "Frete GRÁTIS",
      "deliveryTime": "Receba sexta-feira",
      "review_score": "4.7",
      "installmentOptions": "5x de R$ 56,81 sem juros",
      "productUrl": "https://www.amazon.com.br/dp/B08XYZ123"
    }
  ]
}
```

**Exemplo de callback com screenshot OK mas dados falharam:**

```json
{
  "batchId": "batch_002",
  "processedAt": 1705320060000,
  "scrapes": [
    {
      "urlId": "k57xyz789ghi012",
      "status": "error",
      "errorMessage": "Could not find the price",
      "screenshotUrl": "https://pub-xxx.r2.dev/screenshots/2024/01/xyz789.webp",
      "scrapedAt": 1705320055000,
      "productUrl": "https://www.mercadolivre.com.br/p/MLB12345"
    }
  ]
}
```

**Exemplo de callback com falha total:**

```json
{
  "batchId": "batch_003",
  "processedAt": 1705320120000,
  "scrapes": [
    {
      "urlId": "k57mno456pqr789",
      "status": "error",
      "errorMessage": "Could not find the price",
      "screenshotError": "Screenshot capture failed after all attempts",
      "scrapedAt": 1705320115000,
      "productUrl": "https://www.site.com.br/produto/abc"
    }
  ]
}
```

---

## 7. Campos Retornados

| Campo | Tipo | Descrição |
|-------|------|-----------|
| urlId | string | Identificador da URL no sistema de origem |
| status | string | "completed" ou "error" |
| errorMessage | string | Mensagem de erro na extração de dados (se houver) |
| screenshotUrl | string | URL pública do screenshot no R2 (se capturado) |
| screenshotError | string | Mensagem de erro no screenshot (se falhou) |
| scrapedAt | number | Timestamp do processamento |
| productTitle | string | Nome do produto |
| brand | string | Marca |
| currentPrice | number | Preço atual ou com desconto |
| originalPrice | number | Preço original |
| discountPercentage | number | Percentual de desconto |
| currency | string | Moeda |
| availability | boolean | Disponibilidade |
| imageUrl | string | URL da imagem do produto |
| seller | string | Nome do vendedor |
| shippingInfo | string | Informações de frete |
| shippingCost | number | Custo do frete |
| deliveryTime | string | Prazo de entrega |
| review_score | string | Nota de avaliação |
| installmentOptions | string | Opções de parcelamento |
| productUrl | string | URL do produto |

---

## 8. Timeouts

| Operação | Timeout |
|----------|---------|
| **Extração de Dados** (qualquer etapa) | **1 minuto e meio (90s)** |
| **Screenshot** (qualquer etapa) | **3 minutos (180s)** |
| Upload para R2 | 30 segundos |
| Envio de callback | 30 segundos |

---

## 9. Limites

| Limite | Valor |
|--------|-------|
| Workers paralelos | 50 simultâneos |
| Resultados por batch de callback | 50 máximo |
| Retries no callback | 3 tentativas |
| Tempo máximo de espera para formar batch | 5 segundos |

---

## 10. Bloqueio de Pop-ups no Playwright

Antes de capturar o screenshot, o sistema deve:
- Remover elementos com classes comuns de modais (modal, popup, overlay, cookie-banner)
- Fechar diálogos JavaScript automaticamente
- Remover elementos com z-index muito alto que cobrem o conteúdo
- Scrollar para o topo da página
- Aguardar 1 segundo para animações terminarem

---

## 11. Estrutura do Código Python

O sistema será um **único arquivo Python** (`main.py`) contendo:

1. **Servidor FastAPI** - Recebe requisições e retorna resposta imediata
2. **Worker Pool** - Gerencia até 50 workers processando URLs em paralelo
3. **Processador de URL** - Lógica de primeira tentativa (Playwright) e segunda tentativa (Bright Data)
4. **Cliente Playwright** (baseado em `url-pw.py` e `screen-pw.py`)
   - Gerencia instâncias do navegador headless Chromium
   - Usa proxy Webshare
   - Executa extração de dados e screenshot em paralelo
5. **Cliente Bright Data** (baseado em `url-bd.py` e `screen-bd.py`)
   - Fallback para quando Playwright falha
   - Retry com backoff exponencial para screenshots
6. **Cliente Gemini AI** - Envia HTML limpo para extração de dados do produto
7. **Limpador de HTML** - Remove scripts desnecessários, preserva ld+json, __NEXT_DATA__, etc.
8. **Compressor de Imagem** - Converte PNG para JPEG otimizado (Pillow)
9. **Cliente R2** - Faz upload de screenshots para Cloudflare R2
10. **Fila de Callback** - Acumula resultados e envia em batches de até 50

---

## 12. Variáveis de Ambiente Necessárias

```bash
# Playwright / Webshare Proxy (primeira tentativa)
WEBSHARE_PROXY_URL=http://user:pass@proxy.webshare.io:80

# Bright Data (fallback/segunda tentativa)
BRIGHT_DATA_API=seu_api_token
BRIGHT_DATA_ZONE=sua_zone

# Gemini AI (extração de dados)
GEMINI_API_KEY=sua_chave_gemini

# Cloudflare R2 (armazenamento de screenshots)
R2_ACCOUNT_ID=sua_account_id
R2_ACCESS_KEY_ID=sua_access_key
R2_SECRET_ACCESS_KEY=sua_secret_key
R2_BUCKET_NAME=screenshots
R2_PUBLIC_URL=https://pub-xxx.r2.dev
```

---

## 13. Dependências Python

```txt
fastapi
uvicorn
playwright
google-genai
beautifulsoup4
pillow
requests
boto3  # para R2 (compatível com S3)
```
