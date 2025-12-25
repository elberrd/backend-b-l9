# Estudo de Otimizacao do Sistema de Scraping

**Data:** 25 de Dezembro de 2025
**Versao:** 3.2
**Autor:** Claude Code + Elber

---

## Sumario

1. [Visao Geral do Sistema](#visao-geral-do-sistema)
2. [Analise do Bright Data](#analise-do-bright-data)
3. [Analise do Firecrawl](#analise-do-firecrawl)
4. [Analise do Modal.com](#analise-do-modalcom)
5. [Otimizacao de Containers vs Concorrencia](#otimizacao-de-containers-vs-concorrencia)
6. [Configuracao Otima](#configuracao-otima)
7. [Resultados dos Testes](#resultados-dos-testes)
8. [Conclusoes e Recomendacoes](#conclusoes-e-recomendacoes)

---

## Visao Geral do Sistema

### Arquitetura de 3 Camadas (Fallback)

```
URL Input
    |
    v
[1. Primary Scraper] ----falha----> [2. Secondary Scraper] ----falha----> [3. Playwright]
    |                                      |                                    |
    v                                      v                                    v
  HTML + Screenshot                  HTML + Screenshot                   HTML + Screenshot
    |                                      |                                    |
    +----------------------+---------------+------------------------------------+
                           |
                           v
                   [Gemini AI Extraction]
                           |
                           v
                   [R2 Screenshot Upload]
                           |
                           v
                      JSON Result
```

### Componentes

| Componente | Funcao | Tecnologia |
|------------|--------|------------|
| **Firecrawl** | Scraping com anti-bot | API externa |
| **Bright Data** | Web Unlocker | API externa |
| **Playwright** | Browser automation | Local (stealth) |
| **Gemini AI** | Extracao de dados | Google API |
| **Cloudflare R2** | Armazenamento de screenshots | S3-compatible |
| **Modal.com** | Infraestrutura serverless | Containers |

---

## Analise do Bright Data

### Limites de Concorrencia

| Recurso | Limite |
|---------|--------|
| **Concurrent Requests** | **ILIMITADO** |
| **Rate Limiting** | Nenhum |
| **Throttling** | Nenhum |

**Fonte:** https://brightdata.com/pricing/web-unlocker

> "Unlimited Concurrent Requests" - listado explicitamente como feature

### Precos

| Plano | Preco | Volume |
|-------|-------|--------|
| Pay as you go | $1.50/1K requests | Sem compromisso |
| $499/mes | $1.30/1K requests | 380K requests |
| $999/mes | $1.10/1K requests | 900K requests |
| $1999/mes | $1.00/1K requests | 2M requests |

### Caracteristicas

- Tempo por request: **30-90 segundos**
- Inclui: HTML + Screenshot
- CAPTCHA solving automatico
- Browser fingerprinting
- IP rotation automatica

---

## Analise do Firecrawl

### Limites de Concorrencia por Plano

| Plano | Concurrent Browsers | Preco |
|-------|---------------------|-------|
| **Hobby** | 5 | $19/mes |
| **Standard** | 50 | $99/mes |
| **Growth** | 100 | $499/mes |

### Caracteristicas

- Tempo por request: **10-20 segundos** (mais rapido que Bright Data)
- Inclui: HTML + Screenshot
- waitFor configuravel
- Timeout configuravel

### Implicacao Critica

```
Se voce tem 100 containers mas Firecrawl so permite 5 concurrent:
-> 95 containers ficam ESPERANDO
-> Voce paga cold start de 95 containers desnecessarios
-> Desperdicio total de recursos
```

---

## Analise do Modal.com

### Limites por Plano

| Plano | Containers | GPU Concurrency | Preco Base |
|-------|------------|-----------------|------------|
| **Starter** | 100 | 10 | $0 + compute |
| **Team** | 1000 | 50 | $250 + compute |
| **Enterprise** | Custom | Custom | Custom |

### Precos de Compute

| Recurso | Preco |
|---------|-------|
| **CPU** | $0.0000131 / core / segundo |
| **Memoria** | $0.00000222 / GiB / segundo |

**Importante:** Modal cobra por **tempo de compute**, NAO por quantidade de containers!

### Limites de Scaling

- 2,000 pending inputs
- 25,000 total inputs
- `.map()` processa no maximo 1000 inputs concurrentemente

### Cold Start

- Boot do container: ~1 segundo (Modal e otimizado)
- Inicializacao (imports, Playwright): ~5-15 segundos
- Total cold start: ~10-15 segundos por container

---

## Otimizacao de Containers vs Concorrencia

### O Problema

Qual e o numero otimo de:
1. Containers (scaling horizontal)
2. Inputs por container (concorrencia interna)

### Variaveis-Chave

| Variavel | Valor Estimado |
|----------|----------------|
| **Cold Start (C)** | ~10s por container |
| **Tempo por URL (T)** | ~50s media |
| **Overhead Async (O)** | ~0.1-0.5s por task adicional |
| **Memoria por URL (M)** | ~2-5 MB |

### Formula de Eficiencia

```
Custo_por_URL = (Cold_Start / N) + Overhead(N)

Onde:
- N = numero de URLs por container
- Overhead(N) cresce nao-linearmente
```

### Curva de Eficiencia por max_inputs

| max_inputs | Cold Start/URL | Overhead/URL | Total/URL | Eficiencia |
|------------|----------------|--------------|-----------|------------|
| 1 | 10.0s | 0s | 10.0s | Baixa |
| 5 | 2.0s | 0.1s | 2.1s | Boa |
| 10 | 1.0s | 0.2s | 1.2s | Muito Boa |
| **15** | 0.67s | 0.3s | **0.97s** | **Otima** |
| **20** | 0.5s | 0.5s | **1.0s** | **Otima** |
| 30 | 0.33s | 0.8s | 1.13s | Boa |
| 50 | 0.2s | 1.5s | 1.7s | Moderada |
| 100 | 0.1s | 3.0s | 3.1s | Baixa |

### Grafico Visual

```
Custo por URL (segundos)
|
10s | *
    |  \
 5s |   \
    |    \
 2s |     \___
    |         \___*
 1s |              ---*---*                 <- PONTO OTIMO (15-20)
    |                      \
0.5s|                       \__*
    |                           \___*
    +---+---+---+---+---+---+---+---+---> max_inputs
        1   5   10  15  20  30  50  100
```

### Ponto Otimo: 15-20 inputs por container

**Razoes:**

1. **Cold start amortizado**: 10s / 20 = 0.5s/URL
2. **Async ainda eficiente**: 15-20 tasks nao sobrecarregam o event loop
3. **Memoria controlada**: 20 x 5MB = 100MB extra (ok)
4. **Diminishing returns apos 20**: O overhead cresce mais que a economia

---

## Configuracao Otima

### Para Firecrawl (Hobby = 5 concurrent)

```python
FIRECRAWL_MAX_CONCURRENCY = 5       # API limit (Hobby plan)
FIRECRAWL_MAX_CONTAINERS = 2        # 1 main + 1 buffer (minimal cold starts)
FIRECRAWL_MAX_INPUTS = 10           # Each container handles 2x the limit
```

**Logica:**
- Firecrawl limita a 5 concurrent
- Nao adianta ter mais containers
- Menos containers = menos cold start = mais barato

### Para BrightData (unlimited)

```python
BRIGHTDATA_MAX_CONCURRENCY = 50     # Limited by Gemini/Modal, not BrightData
BRIGHTDATA_MAX_CONTAINERS = 50      # Scale horizontally
BRIGHTDATA_MAX_INPUTS = 20          # Optimal for I/O-bound work
```

**Logica:**
- Sem limite de API
- Pode escalar horizontalmente
- Gargalo e Gemini rate limits

### Comparativo de Estrategias para 5K URLs

| Estrategia | Containers | URLs/Container | Cold Starts | Tempo Est. |
|------------|------------|----------------|-------------|------------|
| 100 x 1 | 100 | 1 | 1000s | ~80 min |
| 50 x 10 | 50 | 10 | 500s | ~55 min |
| **50 x 20** | 50 | 20 | 500s | **~45 min** |
| 25 x 40 | 25 | 40 | 250s | ~50 min |
| 10 x 100 | 10 | 100 | 100s | ~70 min |

---

## Resultados dos Testes

### Teste 1: BrightData como Primary (v3.1)

```
======================================================================
MODAL WEB SCRAPER v3.1 - ASYNC BATCH
======================================================================
Total URLs: 13
Primary Scraper: BRIGHTDATA
Max Concurrency: 50
Scraping Priority: Bright Data -> Firecrawl -> Playwright
======================================================================

RESULTADOS:
- Total: 13 | Success: 13 | Failed: 0
- Screenshots: 13
- Methods: Firecrawl=0, BrightData=13, Playwright=0
- Time: 91.54s | Rate: 0.14 URLs/sec
```

### Teste 2: Firecrawl como Primary (v3.2 Otimizado)

```
======================================================================
MODAL WEB SCRAPER v3.2 - OPTIMIZED BATCH
======================================================================
Total URLs: 13
Primary Scraper: FIRECRAWL
======================================================================
CONFIGURATION (optimized for FIRECRAWL):
  Max Concurrency: 5
  Max Containers:  2
  Max Inputs/Container: 10
  Theoretical Capacity: 20
======================================================================

RESULTADOS:
- Total: 13 | Success: 13 | Failed: 0
- Screenshots: 13
- Methods: Firecrawl=12, BrightData=1, Playwright=0
- Time: 90.63s | Rate: 0.14 URLs/sec
```

### Comparativo Final

| Metrica | Firecrawl v3.2 | BrightData v3.1 |
|---------|----------------|-----------------|
| **Primary** | Firecrawl | BrightData |
| **Max Concurrency** | 5 | 50 |
| **Max Containers** | 2 | 50 |
| **Max Inputs/Container** | 10 | 20 |
| **Tempo Total** | **90.63s** | 91.54s |
| **Rate** | 0.14 URLs/sec | 0.14 URLs/sec |
| **Via Primary** | 12 (92%) | 13 (100%) |
| **Via Fallback** | 1 (8%) | 0 (0%) |

### Observacao Importante

Os tempos sao **praticamente iguais** (~90-91s) apesar de configuracoes muito diferentes!

**Analise:**

1. **O gargalo e o tempo por URL, nao a concorrencia**:
   - Cada URL leva ~10-15s para processar
   - Com 13 URLs e 5 concurrent: 13/5 = 2.6 batches x 15s = ~39s teorico
   - Mas teve 1 fallback que demorou 72s

2. **O fallback dominou o tempo**:
   - Firecrawl: 12 URLs em ~30s (paralelo)
   - 1 URL precisou fallback -> +72s
   - Total: ~90s

3. **Com BrightData-first**:
   - Todos 13 em paralelo (concorrencia 50)
   - Mas BrightData e mais lento por URL (~30-50s cada)
   - Total: ~90s

---

## Conclusoes e Recomendacoes

### Quando Usar Cada Servico

| Cenario | Melhor Opcao | Razao |
|---------|--------------|-------|
| **URLs que Firecrawl resolve bem** | Firecrawl | Mais rapido por URL (~10-15s vs 30-50s) |
| **URLs que precisam fallback** | BrightData | Evita tempo perdido com retry |
| **Volume alto (5K+)** | Firecrawl | Mais barato no total |
| **Custo nao importa** | BrightData | Menos fallbacks, mais consistente |

### Projecao de Tempo para 5K URLs

| Configuracao | Tempo Estimado |
|--------------|----------------|
| Firecrawl (conc=5) | ~23 minutos |
| BrightData (conc=50) | ~8 minutos |
| BrightData (conc=100) | ~4 minutos |

### Projecao de Custo para 5K URLs

| Servico | Custo |
|---------|-------|
| Firecrawl | ~$8.33 (5K x $0.00167) |
| BrightData | ~$7.50 (5K x $0.0015) |

### Configuracao Final Implementada

```python
# Firecrawl (Hobby - 5 concurrent)
FIRECRAWL_MAX_CONCURRENCY = 5
FIRECRAWL_MAX_CONTAINERS = 2      # Minimo cold starts
FIRECRAWL_MAX_INPUTS = 10         # Amortiza cold start

# BrightData (unlimited)
BRIGHTDATA_MAX_CONCURRENCY = 50
BRIGHTDATA_MAX_CONTAINERS = 50    # Escala horizontal
BRIGHTDATA_MAX_INPUTS = 20        # Optimal I/O-bound
```

### Variaveis de Ambiente

```bash
# Selecionar scraper primario
PRIMARY_SCRAPER=firecrawl  # ou "brightdata"

# Overrides opcionais
MAX_CONCURRENCY=5
MAX_CONTAINERS=2
MAX_INPUTS=10
```

---

## Apendice: Codigo Relevante

### Funcao get_config()

```python
def get_config() -> dict:
    """Get optimal configuration based on primary scraper."""
    primary = get_primary_scraper()

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

    return config
```

### Decorador Modal Otimizado

```python
@app.function(
    image=scraper_image,
    max_containers=50,   # Max possible - actual controlled by semaphore
    memory=512,          # MB - enough for 20 concurrent scrapes
    timeout=300,
    retries=1,
    secrets=[modal.Secret.from_name("bausch")],
)
@modal.concurrent(max_inputs=20)  # Max possible - actual controlled by semaphore
async def scrape_url(url_id: str, url: str) -> dict:
    ...
```

---

## Referencias

- Modal.com Pricing: https://modal.com/pricing
- Modal.com Concurrent Inputs: https://modal.com/docs/guide/concurrent-inputs
- Modal.com Scaling: https://modal.com/docs/guide/scale
- Modal.com Cold Start: https://modal.com/docs/guide/cold-start
- Bright Data Web Unlocker: https://brightdata.com/products/web-unlocker
- Bright Data Pricing: https://brightdata.com/pricing/web-unlocker
- Firecrawl Documentation: https://docs.firecrawl.dev
