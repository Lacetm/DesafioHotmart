# ETL — GMV Diário por Subsidiária

Pipeline PySpark que consolida eventos CDC das tabelas `purchase`, `product_item` e `purchase_extra_info` para calcular o GMV diário por subsidiária, gravando snapshots históricos e imutáveis em Delta Lake.

---

## Pré-requisitos

- Python 3.9+
- Java 8 ou 11 (obrigatório para o PySpark)
- pip

### Verificar versão do Java

```bash
java -version
```

Caso não tenha o Java instalado:

```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS (Homebrew)
brew install openjdk@11

# Windows
# Baixe e instale o JDK 11 em: https://adoptium.net
```

---

## Instalação

### 1. Clone o repositório e entre na pasta

```bash
git clone <https://github.com/Lacetm/DesafioHotmart.git>
cd <DesafioHotmart>
```

### 2. Crie um ambiente virtual (recomendado)

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

---

## Configuração

Antes de rodar, edite as variáveis de caminho no topo do arquivo `etl_gmv_spark.py`:

```python
# Caminhos no data lake — substitua pelos seus paths reais ex: s3://bucket/purchase
PATH_PURCHASE        = "data/purchase"
PATH_PRODUCT_ITEM    = "data/product_item"    
PATH_EXTRA_INFO      = "data/purchase_extra_info"
PATH_GMV_SNAPSHOT    = "data/gmv_daily_snapshot"
```

Os paths suportam qualquer storage compatível com Spark:

| Ambiente | Exemplo de path |
|---|---|
| Local | `data/purchase` |
| AWS S3 | `s3://meu-bucket/purchase` |
---

## Execução

```bash
python etl_gmv_spark.py
```

O pipeline executa as seguintes etapas:

1. Cria a tabela `gmv_daily_snapshot` (se não existir)
2. Lê os dados das três fontes CDC
3. Deduplica os eventos mantendo o mais recente por `purchase_id`
4. Consolida as fontes e calcula o GMV (apenas transações com `release_date` preenchida)
5. Grava o snapshot diário de forma imutável (D-1)
6. Exibe o GMV corrente ao final

---

## Consultas SQL

### GMV corrente (registros ativos)

```sql
SELECT
    reference_date,
    subsidiary,
    gmv
FROM gmv_daily_snapshot
WHERE is_current = TRUE
ORDER BY reference_date, subsidiary;
```

### Navegação histórica — GMV como estava em uma data específica

```sql
SELECT
    snapshot_date,
    reference_date,
    subsidiary,
    gmv
FROM gmv_daily_snapshot
WHERE reference_date BETWEEN '2023-01-01' AND '2023-01-31'
  AND snapshot_date = '2023-03-31'
ORDER BY reference_date, subsidiary;
```

---

## Modelo da tabela final

| Coluna | Tipo | Descrição |
|---|---|---|
| `snapshot_date` | DATE | Dia em que o snapshot foi gerado (D-1) |
| `reference_date` | DATE | Dia do GMV (order_date da compra) |
| `subsidiary` | STRING | Subsidiária (`desconhecida` se ainda não chegou) |
| `gmv` | DECIMAL(18,2) | Soma do purchase_value das transações liberadas |
| `is_current` | BOOLEAN | `TRUE` = registro corrente, `FALSE` = histórico invalidado |
| `created_at` | TIMESTAMP | Timestamp de inserção do registro |

---

## Tech Stack

| Tecnologia | Função |
|---|---|
| **PySpark 3.x** | Processamento distribuído dos eventos CDC |
| **Delta Lake** | Formato ACID com suporte a upsert, time travel e reprocessamento seguro |
| **Apache Spark** | Engine de execução (compatível com Databricks, EMR, GCP Dataproc) |
