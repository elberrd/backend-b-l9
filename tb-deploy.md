# TinyBird Deployment Guide - Forward Workspaces

Este documento explica como fazer deploy de datasources e pipes no TinyBird quando o workspace está configurado como **Tinybird Forward** (não Classic).

## O Problema

O workspace `iai_baush` é um **TinyBird Forward workspace**, que tem restrições de deployment diferentes da versão Classic. Ao tentar fazer deploy usando métodos tradicionais, você encontrará erros como:

### Erro 1: `tb push` bloqueado
```bash
tb push datasources/companies_dimension.datasource

# ❌ Erro:
# Failed creating Data Source: Forbidden: Adding or modifying data sources
# to this workspace can only be done via deployments.
```

### Erro 2: `tb deploy` não funciona em Forward workspaces
```bash
tb deploy --yes

# ❌ Erro:
# This is a Tinybird Forward workspace, and this operation is only
# available for Tinybird Classic workspaces
```

### Erro 3: API POST /v0/datasources também bloqueado
```bash
curl -X POST "https://api.us-east.tinybird.co/v0/datasources" \
  -H "Authorization: Bearer $TOKEN" \
  -d "name=companies_dimension" \
  # ... outros parâmetros

# ❌ Erro:
# {"error": "Adding or modifying data sources to this workspace can only
# be done via deployments."}
```

### Erro 4: API POST /v0/sql só aceita SELECT
```bash
curl -X POST "https://api.us-east.tinybird.co/v0/sql" \
  -H "Authorization: Bearer $TOKEN" \
  -d 'q=CREATE TABLE companies_dimension (...)'

# ❌ Erro:
# {"error": "DB::Exception: Only SELECT or DESCRIBE queries are supported.
# Got: CreateQuery"}
```

### Erro 5: Events API requer datasource existente
```bash
curl -X POST "https://api.us-east.tinybird.co/v0/events?name=companies_dimension" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"companyId": "test", ...}'

# ❌ Erro:
# Datasource companies_dimension not found in workspace iai_baush.
```

## A Solução: Nova CLI do TinyBird Forward

### Por que a CLI antiga não funciona?

A CLI versão 5.x (Classic) **NÃO suporta** TinyBird Forward workspaces. Os comandos de deployment (`tb deploy`, `tb push`) foram completamente redesenhados para a arquitetura Forward.

### Identificando a CLI antiga vs. nova

**CLI Antiga (Classic):**
```bash
tb --version
# Output: tb, version 5.22.2 (rev 35eaae2)
```

**CLI Nova (Forward):**
```bash
tb --version
# Output: tb, version 1.1.7 (rev d8fef0d)
```

## Passo a Passo: Como Fazer Deploy Corretamente

### 1. Instalar/Atualizar para a CLI Forward

```bash
# Instalar a nova CLI do TinyBird Forward
curl https://tinybird.co | sh

# A CLI será instalada em ~/.local/bin/tb
```

### 2. Atualizar o PATH (se necessário)

```bash
# Adicionar ao PATH
export PATH="/Users/$USER/.local/bin:$PATH"

# Ou adicionar permanentemente ao ~/.zshrc ou ~/.bashrc
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### 3. Verificar a instalação

```bash
tb --version
# Deve mostrar: tb, version 1.1.7 (ou superior)
```

### 4. Navegar para o diretório do projeto backend

```bash
cd /path/to/your/backend
# Exemplo: cd /Users/elberrd/Documents/Development/clientes/bel9/backend
```

### 5. Fazer Deploy no TinyBird Cloud

**Comando principal:**
```bash
tb --cloud deploy --wait
```

**Opções do comando:**
- `--cloud`: Deploy para Tinybird Cloud (ambiente produção)
- `--wait`: Aguarda o deployment completar antes de retornar
- `--check`: Dry-run para validar antes de fazer deploy

**Exemplo de output bem-sucedido:**
```
Running against Tinybird Cloud: Workspace iai_baush

* Changes to be deployed:
------------------------------------------------
status: new
name: companies_dimension
type: datasource
path: datasources/companies_dimension.datasource
------------------------------------------------
status: new
name: families_dimension
type: datasource
path: datasources/families_dimension.datasource
------------------------------------------------

* Deployment submitted
» Waiting for deployment to be ready...
✓ Deployment is ready
» Waiting for deployment to be promoted...
✓ Deployment promoted
✓ Deployment #15 is live!
```

### 6. Verificar Datasources Criadas

```bash
# Listar datasources via API
TOKEN='your_admin_token_here'
curl -s "https://api.us-east.tinybird.co/v0/datasources" \
  -H "Authorization: Bearer $TOKEN" | \
  python3 -c "import sys, json; data = json.load(sys.stdin); print('\n'.join([ds['name'] for ds in data['datasources'] if 'dimension' in ds['name']]))"
```

## Configuração do Convex para Sincronização

Após criar as datasources no TinyBird, configure as variáveis de ambiente no Convex:

### 1. Configurar Token de Admin

```bash
# No diretório raiz do projeto (onde está convex/)
npx convex env set TINYBIRD_ADMIN_TOKEN 'p.eyJ1IjogImE4ZmNmN2M5LWU2NjMtNGRlYi05ZmIzLWI5MDI3YzRlZWFmMSIsICJpZCI6ICJhNDAwN2I2ZS04NzQyLTRhMDMtYjczZC1kNmU2YWEyZDE3OTciLCAiaG9zdCI6ICJ1c19lYXN0In0.6MTCcY111SMuv6B4IY4MVsYHulb0wATDkwAII06NYK0'
```

### 2. Configurar URL da API

```bash
npx convex env set TINYBIRD_URL 'https://api.us-east.tinybird.co'
```

**IMPORTANTE:** Se você não configurar a `TINYBIRD_URL`, o código usará o default `https://api.tinybird.co` (sem região), o que causará erros 404.

## Executar Backfills Iniciais

Após deployment e configuração das variáveis de ambiente, popule as dimension tables com dados existentes:

```bash
# Companies
npx convex run tinybirdDimensions:runCompaniesBackfill

# Families
npx convex run tinybirdDimensions:runFamiliesBackfill

# Channels
npx convex run tinybirdDimensions:runChannelsBackfill

# Business
npx convex run tinybirdDimensions:runBusinessBackfill
```

**Output esperado:**
```json
{
  "errorCount": 0,
  "successCount": 9,
  "total": 9
}
```

## Verificar Dados no TinyBird

```bash
TOKEN='your_admin_token_here'

# Contar registros em companies_dimension
curl -s "https://api.us-east.tinybird.co/v0/sql?q=SELECT%20count()%20FROM%20companies_dimension" \
  -H "Authorization: Bearer $TOKEN"

# Ver primeiros registros
curl -s "https://api.us-east.tinybird.co/v0/sql?q=SELECT%20*%20FROM%20companies_dimension%20LIMIT%205" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

## Workflow de Deploy Futuro

Quando adicionar novos datasources ou pipes:

1. **Criar o arquivo `.datasource` ou `.pipe`** no diretório apropriado
2. **Commit no git** (recomendado para rastreabilidade)
3. **Deploy:**
   ```bash
   cd backend
   tb --cloud deploy --wait
   ```
4. **Verificar deployment:**
   - URL do deployment: https://cloud.tinybird.co/gcp/us-east4/iai_baush/deployments
   - Ou via CLI: `tb --cloud deployment ls`

## Comandos Úteis da Nova CLI

```bash
# Listar deployments
tb --cloud deployment ls

# Ver status do deployment
tb --cloud deployment status

# Abrir projeto no TinyBird Cloud
tb --cloud open

# Verificar diferenças antes de deploy
tb --cloud deployment create --check

# Deploy com promote automático
tb --cloud deploy

# Deploy apenas para staging (sem promover)
tb --cloud deployment create --wait

# Promover staging para live
tb --cloud deployment promote
```

## Troubleshooting

### "No such option: --cloud"
- **Causa:** CLI antiga (Classic) instalada
- **Solução:** Instalar nova CLI Forward conforme passo 1

### "TINYBIRD_ADMIN_TOKEN not configured"
- **Causa:** Variável de ambiente não configurada no Convex
- **Solução:** Executar `npx convex env set TINYBIRD_ADMIN_TOKEN '...'`

### Backfill retorna erros (errorCount > 0)
- **Causa:** URL incorreta (usando default ao invés de us-east)
- **Solução:** Configurar `TINYBIRD_URL` conforme passo 2 da configuração Convex

### "Datasource not found" no Events API
- **Causa:** Tentando enviar eventos para datasource que não existe
- **Solução:** Fazer deploy primeiro com `tb --cloud deploy`, depois enviar eventos

## Arquitetura de Sincronização

```
Convex (Source of Truth)              TinyBird (Analytics)
    └─ companies                      └─ companies_dimension
       - companyName                      - companyId
       - website                          - companyName
       - description                      - website
                                          - description
       [Mutation]                         - updatedAt
          ↓
    scheduler.runAfter(0)
          ↓
    syncCompanyToDimension  ──→  POST /v0/events?name=companies_dimension
    (internalAction)                     (ReplacingMergeTree auto-dedupe)
```

**Fluxo:**
1. Usuário cria/atualiza Company no Convex
2. Mutation dispara scheduler após commit
3. `syncCompanyToDimension` envia evento para TinyBird
4. TinyBird insere/atualiza registro na dimension table
5. ReplacingMergeTree mantém apenas versão mais recente (por `updatedAt`)

## Diferenças: CLI Classic vs. CLI Forward

| Aspecto | Classic (5.x) | Forward (1.x) |
|---------|---------------|---------------|
| Comando deploy | `tb deploy` | `tb --cloud deploy` |
| Push direto | `tb push` | ❌ Não suportado |
| Git integration | Opcional | Recomendado |
| Workspace type | Classic only | Forward only |
| Local development | Limitado | `tb local start` |
| Agent mode | ❌ | ✅ `tb` (natural language) |

## Recursos Adicionais

- **Documentação Forward:** https://www.tinybird.co/docs/forward
- **Quick Start:** https://www.tinybird.co/docs/forward/get-started/quick-start
- **CLI Commands:** https://www.tinybird.co/docs/forward/dev-reference/commands
- **Deployments:** https://www.tinybird.co/docs/forward/test-and-deploy/deployments/cli

## Resumo TL;DR

```bash
# 1. Instalar nova CLI
curl https://tinybird.co | sh
export PATH="$HOME/.local/bin:$PATH"

# 2. Deploy datasources
cd backend
tb --cloud deploy --wait

# 3. Configurar Convex
npx convex env set TINYBIRD_ADMIN_TOKEN 'your_token'
npx convex env set TINYBIRD_URL 'https://api.us-east.tinybird.co'

# 4. Backfill dados
npx convex run tinybirdDimensions:runCompaniesBackfill
npx convex run tinybirdDimensions:runFamiliesBackfill
npx convex run tinybirdDimensions:runChannelsBackfill
npx convex run tinybirdDimensions:runBusinessBackfill
```

**Pronto!** ✅ Suas dimension tables estão deployadas e sincronizando automaticamente.
