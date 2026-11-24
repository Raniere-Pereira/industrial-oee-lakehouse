# Industrial OEE Lakehouse

Pipeline analítico end-to-end de **Overall Equipment Effectiveness (OEE)** para manufatura industrial, construído sobre **Databricks Free Edition**, **Unity Catalog**, **Delta Lake**, **PySpark**, **dbt-databricks** e **GitHub Actions**.

O projeto demonstra uma arquitetura **Medallion** completa em produção — da inspeção e ingestão idempotente de arquivos brutos em Unity Catalog Volumes até a modelagem dimensional (Star Schema) aderente à norma **ISO 22400**, reconstrução de contadores industriais acumulados (*High-Water Mark* por ciclo), **43 testes automatizados de qualidade de dados** e CI/CD contínuo.

---

## Visão Geral da Arquitetura

```mermaid
flowchart LR
    subgraph LZ["Unity Catalog Volume (Landing Zone)"]
        CSV["Arquivos CSV Brutos (;)<br/>Machine.csv | Products.csv<br/>Target Speed.csv | Total Report.csv"]
    end

    subgraph BR["Camada Bronze (PySpark + Delta Lake)"]
        NB1["01_inspect_raw.py<br/>Descoberta e Profiling"]
        NB2["02_ingest_bronze.py<br/>Ingestão Idempotente (SHA-256 + MERGE)"]
        T_B1["raw_oee_machines"]
        T_B2["raw_oee_products"]
        T_B3["raw_oee_target_speed"]
        T_B4["raw_oee_total_report"]
    end

    subgraph SV["Camada Silver (dbt-databricks)"]
        STG["stg_oee_events<br/>Incremental MERGE + High-Water Mark por Ciclo<br/>Reset Real (< 50%) vs. Jitter + time_bucket (ISO 22400)"]
    end

    subgraph GD["Camada Gold (Star Schema + Data Quality)"]
        DM["dim_machine<br/>Surrogate Key SHA-256"]
        DP["dim_product<br/>Surrogate Key SHA-256"]
        FCT["fct_oee_daily<br/>Decomposição Aditiva + OEE Honesto (ISO 22400)"]
        TST["43 Testes Automatizados dbt<br/>(37 Genéricos + 6 Singulares)"]
    end

    subgraph CI["CI/CD (GitHub Actions)"]
        GHA["dbt_ci.yml<br/>dbt debug + dbt build<br/>(PR -> main | workflow_dispatch)"]
    end

    CSV --> NB1
    CSV --> NB2
    NB2 --> T_B1 & T_B2 & T_B3 & T_B4
    T_B4 --> STG
    T_B1 --> DM
    T_B2 --> DP
    STG --> FCT
    DM --> FCT
    DP --> FCT
    T_B3 --> FCT
    STG & DM & DP & FCT --> TST
    GHA -.->|"Validação Contínua"| SV
    GHA -.->|"Validação Contínua"| GD
```

---

## Stack Tecnológica

| Camada / Responsabilidade | Tecnologia |
| --- | --- |
| **Plataforma & Governança** | Databricks Free Edition + Unity Catalog (`industrial_lakehouse.{bronze,silver,gold}`) |
| **Armazenamento** | Delta Lake com **Liquid Clustering** |
| **Ingestão (Raw → Bronze)** | PySpark + `DeltaTable` (`MERGE INTO`) |
| **Transformação (Bronze → Silver → Gold)** | `dbt-databricks` (`dbt-core 1.10`) |
| **Qualidade de Dados** | 43 dbt Data Tests (37 genéricos + 6 singulares de reconciliação e invariantes) e Source Freshness |
| **CI/CD** | GitHub Actions com GitHub Secrets |

---

## Estrutura do Repositório

```text
industrial-oee-lakehouse/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml                                  # Pipeline CI/CD (dbt debug + dbt build)
├── databricks/
│   └── notebooks/
│       ├── 01_inspect_raw.py                           # Inspeção dos CSVs no Unity Catalog Volume
│       └── 02_ingest_bronze.py                         # Ingestão 1:1 idempotente (SHA-256 + MERGE)
├── dbt_project/
│   ├── dbt_project.yml                                 # Configuração do projeto dbt (Silver/Gold Delta)
│   ├── profiles.yml                                    # Profile Databricks via env_var()
│   ├── macros/
│   │   └── generate_schema_name.sql                    # Controle explícito de schemas (silver/gold)
│   ├── models/
│   │   ├── bronze/
│   │   │   └── sources.yml                             # Fontes Bronze + Source Freshness (ingested_at)
│   │   ├── silver/
│   │   │   ├── stg_oee_events.sql                      # Incremental MERGE, High-Water Mark por ciclo e time_bucket
│   │   │   └── schema.yml                              # 13 testes genéricos da camada Silver
│   │   └── gold/
│   │       ├── dim_machine.sql                         # Dimensão de máquinas (10 máquinas)
│   │       ├── dim_product.sql                         # Dimensão de produtos (18 produtos)
│   │       ├── fct_oee_daily.sql                       # Fato diária OEE (grão data + máquina + produto)
│   │       └── schema.yml                              # 24 testes genéricos da camada Gold
│   └── tests/
│       ├── assert_valid_oee_bounds.sql                 # Pilares e oee_score em [0, 1] quando não nulos
│       ├── assert_non_negative_delta_biscuits.sql      # delta_total_biscuits_made >= 0 (Silver e Gold)
│       ├── assert_non_negative_delta_good_biscuits.sql # delta_good_biscuits_made >= 0 (Silver e Gold)
│       ├── assert_gold_time_conservation.sql           # Conservação aditiva dos 5 buckets de tempo (< 0,01 min)
│       ├── assert_quality_null_without_telemetry.sql   # quality_rate NULL sem telemetria de contador total
│       └── assert_silver_gold_reconciliation.sql       # Gate de reconciliação (eventos, duração e deltas Silver = Gold)
├── .env.example                                        # Template de variáveis de ambiente (sem segredos)
└── .gitignore                                          # Proteção de .env, target/, logs/ e .user.yml
```

---

## Auditoria de Data Quality & Decisões de Engenharia

Uma auditoria forense sobre os **8.043 eventos** da camada Bronze identificou **3 problemas estruturais na telemetria de origem** (além da ausência de sensor de total em 8 máquinas). Todas as correções foram implementadas com rigor semântico (ISO 22400), preservando **100% de reconciliação entre Silver e Gold** (`8.043` eventos, `179.775,96 min`, `11.790.588` `delta_total_biscuits_made` e `10.689.705` `delta_good_biscuits_made`):

1. **Comportamento de odômetro em `total` e `good` (resolvido via High-Water Mark por ciclo)**
   - **Achado:** `totalbiscuitsmade` na `Biscuit Filling Machine` e na `Biscuit Sprinkling Machine` é um **contador acumulado (odômetro)** — somá-lo diretamente inflava a produção para `1,31 bilhão` de biscoitos. A auditoria por máquina também comprovou objetivamente dois modos distintos para `good_biscuits_made`:
     - **`CUMULATIVE_ODOMETER`** exclusivamente na `Biscuit Filling Machine` (`max = 480.646`, `avg = 115.183,3`), crescendo em sincronia com `raw_total_biscuits_made`;
     - **`PER_EVENT_INCREMENTAL`** nas outras 9 máquinas (`min = 1..47`, `max = 406..500`, `avg ≈ 250`).
   - **Solução (`stg_oee_events.sql`):** introduzimos `good_counter_mode`, `total_counter_cycle_id` / `good_counter_cycle_id` e o cálculo de `delta_total_biscuits_made` e `delta_good_biscuits_made` via **High-Water Mark (`running_max` dentro de cada ciclo entre resets reais)**, com âncoras incrementais (`existing_total_anchor` e `existing_good_anchor`) por máquina para garantir idempotência em execuções incrementais futuras. Na `Biscuit Filling Machine`, eventos sem telemetria total (`raw_total = 0` e `good <= 500`) são isolados da janela do odômetro de bons biscoitos para evitar falsos resets. O volume líquido reconstruído caiu de `1,31 bilhão` bruto para **`11,79 milhões`** (`9,50M` na Filling e `2,29M` na Sprinkling), em escala industrial realista frente à velocidade nominal (`43.200–51.840 biscoitos/h`).

2. **Jitter de sensor + truncamento de timestamp no minuto (`HH:mm:00`)**
   - **Achado:** os timestamps do CSV têm resolução apenas de **minuto** (sem segundos), causando empates em eventos sub-minuto, somados a micro-oscilações de leitura (`-32` a `-171` unidades, `< 0,15%`). Uma regra ingênua de reset (`raw < prev`) gerava **1.486 falsos resets** (`1.019` na Filling e `467` na Sprinkling), injetando centenas de milhões de biscoitos fantasmas.
   - **Solução (`stg_oee_events.sql`):**
     - **Desempate determinístico:** janelas ordenadas por `start_timestamp, end_timestamp, raw_total_biscuits_made, good_biscuits_made, row_hash`.
     - **Separação reset real vs. jitter:** `is_counter_reset = TRUE` apenas para quedas acentuadas (`raw < prev * 0.5`), identificando apenas **38 resets reais** (`23` na Filling + `15` na Sprinkling); micro-quedas (`prev * 0.5 <= raw < prev`) são marcadas como `is_counter_jitter = TRUE` (**1.371 eventos** filtrados com `delta = 0`), e o *High-Water Mark* elimina qualquer dupla contagem quando a leitura volta a subir.

3. **Produção ativa durante registros rotulados como `NO` / `CC` e ausência de telemetria em 8 máquinas**
   - **Achado:** durante janelas rotuladas como `NO (No Order)` (`137.694,36 min`, 76,6% do tempo) e `CC (Changeover Cleaning)` (`33.679,23 min`), os odômetros da Filling e da Sprinkling sobem milhares de biscoitos (evidência de falha de apontamento de status no sistema coletor/SCADA de origem), enquanto apenas **9 registros** (`239,87 min`, todos na `Biscuit Filling Machine`) trazem `oee_category = 'Run Time'` explícito. Além disso, **8 das 10 máquinas** reportam `raw_total_biscuits_made = 0` em 100% dos eventos (`1.937` linhas).
   - **Solução (`fct_oee_daily.sql`):** seguimos estritamente o padrão **OEE clássico / ISO 22400**: `NO_ORDER` é excluído do `planned_production_minutes` (`42.081,59 min`), `CC` e `NO` **não** são imputados como tempo produtivo, e os KPIs (`performance_rate`, `quality_rate`, `oee_score`) retornam **`NULL`** sempre que não houver telemetria confiável (sem forçar `100%` ou `0%` artificiais).

### Outras Decisões Técnicas Consolidadas
- **Ingestão Bronze 1:1 Idempotente (`02_ingest_bronze.py`):** leitura com `.option("sep", ";")` e `inferSchema=false`, `row_hash` (`SHA-256`) removendo 1 duplicata exata do `Total Report.csv` (`8.043` registros únicos) e `DeltaTable.merge()`.
- **Higienização e Governança:** `TRIM()` em `machine`/`product` (`0` órfãos nos joins), `TRY_TO_TIMESTAMP`, `REPLACE(..., ',', '.')` para decimais europeus, correção `bsicuits_per_pallet` → `biscuits_per_pallet` em `dim_product` (preservando `cases_per_pallet`) e **Delta Lake Liquid Clustering** em Bronze, Silver e Gold.
- **43 Testes Automatizados (`dbt build`):** 37 genéricos + 6 singulares (bounds `[0, 1]`, deltas `>= 0` em total e good, conservação aditiva de tempo `< 0,01 min`, honestidade de `quality_rate` e reconciliação Silver = Gold).

---

## Resultados Validados (Camada Gold)

### Reconciliação Silver vs. Gold

| Métrica | Silver (`stg_oee_events`) | Gold (`fct_oee_daily`) | Status |
| --- | ---: | ---: | :---: |
| Total de eventos | **8.043** | **8.043** *(193 linhas agregadas)* | OK |
| Duração total (`minutes`) | **179.775,96** | **179.775,96** | OK |
| `SUM(delta_total_biscuits_made)` | **11.790.588** | **11.790.588** | OK |
| `SUM(delta_good_biscuits_made)` | **10.689.705** | **10.689.705** | OK |
| Resets reais (`< 50%`) | **38** | **38** | OK |
| Eventos de jitter (`50%..100%`) | **1.371** | **1.371** | OK |

### Resumo por Máquina — Odômetros, Tempos ISO 22400 e KPIs Ponderados

Resultado obtido via `dbt show` sobre `industrial_lakehouse.gold.fct_oee_daily` × `industrial_lakehouse.gold.dim_machine`:

| machine_name | good_counter_mode | planned_min | no_order_min | changeover_min | maint_min | run_time_min | unclass_min | delta_total | delta_good | real_resets | jitter | avail_% | perf_% | qual_% | oee_% |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Biscuit Filling Machine | `CUMULATIVE_ODOMETER` | 19.360,64 | 22.848,37 | 10.958,47 | 211,68 | 239,87 | 7.950,63 | 9.498.937 | 9.658.809 | 23 | 962 | 1,24 | 100,00 | 100,00 | 1,24 |
| Biscuit Sprinkling Machine | `PER_EVENT_INCREMENTAL` | 3.624,60 | 2.948,61 | 3.624,59 | 0,00 | 0,00 | 0,02 | 2.291.651 | 537.444 | 15 | 409 | 0,00 | `NULL` | 23,45 | `NULL` |
| Biscuit Boxing Machine | `PER_EVENT_INCREMENTAL` | 5.230,35 | 41.509,58 | 5.230,27 | 0,08 | 0,00 | 0,00 | `NULL` | 57.152 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Biscuit Forming Machine | `PER_EVENT_INCREMENTAL` | 4.027,76 | 16.289,09 | 4.027,68 | 0,08 | 0,00 | 0,00 | `NULL` | 14.121 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Biscuit Heating Machine | `PER_EVENT_INCREMENTAL` | 3.821,04 | 7.032,35 | 3.821,04 | 0,00 | 0,00 | 0,00 | `NULL` | 1.995 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Biscuit Mixing Machine | `PER_EVENT_INCREMENTAL` | 3.142,07 | 29.525,17 | 3.142,07 | 0,00 | 0,00 | 0,00 | `NULL` | 52.358 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Biscuit Pressing Machine | `PER_EVENT_INCREMENTAL` | 2.366,76 | 4.295,16 | 2.366,76 | 0,00 | 0,00 | 0,00 | `NULL` | 292.149 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Biscuit Jam Machine | `PER_EVENT_INCREMENTAL` | 290,97 | 4.338,80 | 290,97 | 0,00 | 0,00 | 0,00 | `NULL` | 23.748 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Packaging Heat Machine | `PER_EVENT_INCREMENTAL` | 207,17 | 7.427,98 | 207,17 | 0,00 | 0,00 | 0,00 | `NULL` | 48.067 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |
| Biscuit Topping Machine | `PER_EVENT_INCREMENTAL` | 10,23 | 1.479,26 | 10,23 | 0,00 | 0,00 | 0,00 | `NULL` | 3.862 | 0 | 0 | 0,00 | `NULL` | `NULL` | `NULL` |

---

## Como Executar Localmente

### Pré-requisitos
- Python 3.9+ (recomendado 3.11)
- Workspace Databricks com Unity Catalog habilitado, SQL Warehouse ativo e catálogo `industrial_lakehouse` (`bronze`, `silver`, `gold`)
- Arquivos CSV carregados no Volume `/Volumes/industrial_lakehouse/bronze/landing_zone/oee/`

### 1. Ingestão no Databricks (Raw → Bronze)
Execute os notebooks no Databricks Workspace (conectados ao catálogo `industrial_lakehouse`):
1. `databricks/notebooks/01_inspect_raw.py` — valida os arquivos `.csv`, tamanho, schema bruto e amostra.
2. `databricks/notebooks/02_ingest_bronze.py` — cria/atualiza de forma idempotente as 4 tabelas Delta na camada Bronze.

### 2. Configuração do Ambiente Local
Clone o repositório e instale o `dbt-databricks`:

```powershell
git clone https://github.com/Raniere-Pereira/industrial-oee-lakehouse.git
cd industrial-oee-lakehouse
pip install dbt-databricks
```

Copie o arquivo `.env.example` para `.env` na raiz do repositório e preencha suas credenciais do Databricks SQL Warehouse (`.env` está listado no `.gitignore` e nunca deve ser versionado):

```dotenv
DATABRICKS_HOST=seu-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/seu-warehouse-id
DATABRICKS_TOKEN=dapi...
```

### 3. Execução do dbt (PowerShell)
Carregue as variáveis do `.env` na sessão atual e rode `dbt debug` e `dbt build`:

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+?)\s*=\s*(.*)\s*$') {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
    }
}

cd dbt_project
dbt debug --profiles-dir .
dbt build --profiles-dir .
```

Para executar backfill completo da Silver e Gold (e validar o caminho incremental em seguida):

```powershell
dbt build --profiles-dir . --select stg_oee_events+ --full-refresh
dbt run --profiles-dir . --select stg_oee_events
```

Para consultar o resumo por máquina via CLI:

```powershell
dbt show --profiles-dir . --limit 20 --inline "
WITH base AS (
    SELECT
        m.machine_name,
        MAX(f.good_counter_mode) AS good_counter_mode,
        SUM(f.planned_production_minutes) AS planned_min,
        SUM(f.explicit_run_time_minutes) AS run_min,
        SUM(f.delta_total_biscuits_made) AS sum_delta_total_biscuits,
        SUM(f.delta_good_biscuits_made) AS sum_delta_good_biscuits,
        SUM(f.expected_biscuits) AS exp_tot,
        SUM(f.counter_reset_count) AS real_resets,
        SUM(f.counter_jitter_count) AS jitter_events
    FROM industrial_lakehouse.gold.fct_oee_daily f
    JOIN industrial_lakehouse.gold.dim_machine m
      ON f.machine_sk = m.machine_sk
    GROUP BY 1
),
metrics AS (
    SELECT
        *,
        CASE WHEN planned_min > 0 THEN run_min / planned_min ELSE NULL END AS avail,
        CASE WHEN exp_tot > 0 AND sum_delta_total_biscuits IS NOT NULL THEN LEAST(1.0, CAST(sum_delta_total_biscuits AS DOUBLE) / exp_tot) ELSE NULL END AS perf,
        CASE WHEN sum_delta_total_biscuits IS NOT NULL AND sum_delta_total_biscuits > 0 AND sum_delta_good_biscuits IS NOT NULL THEN LEAST(1.0, CAST(sum_delta_good_biscuits AS DOUBLE) / CAST(sum_delta_total_biscuits AS DOUBLE)) ELSE NULL END AS qual
    FROM base
)
SELECT
    machine_name,
    good_counter_mode,
    sum_delta_total_biscuits,
    sum_delta_good_biscuits,
    real_resets,
    jitter_events,
    ROUND(avail * 100, 2) AS avail_pct,
    ROUND(perf * 100, 2) AS perf_pct,
    ROUND(qual * 100, 2) AS qual_pct,
    CASE
        WHEN avail IS NOT NULL AND perf IS NOT NULL AND qual IS NOT NULL
        THEN ROUND((avail * perf * qual) * 100, 2)
        ELSE NULL
    END AS oee_pct
FROM metrics
ORDER BY oee_pct DESC NULLS LAST, sum_delta_total_biscuits DESC NULLS LAST
"
```

---

## Execução via CI/CD (GitHub Actions)

O workflow [.github/workflows/dbt_ci.yml](.github/workflows/dbt_ci.yml) automatiza a validação completa (`dbt debug` + `dbt build`) em:
- **Pull Requests** direcionados à branch `main`;
- **Disparo manual** (`workflow_dispatch`) pela aba **Actions** no GitHub ou via GitHub CLI.

### Configuração dos Secrets no GitHub
Em **Settings → Secrets and variables → Actions → New repository secret**, configure:
- `DATABRICKS_HOST`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`

### Disparo manual via GitHub CLI (opcional)
```powershell
gh workflow run dbt_ci.yml
gh run list --limit 3
```
