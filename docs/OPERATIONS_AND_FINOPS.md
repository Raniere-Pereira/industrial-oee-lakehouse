# Operations, Safe Backfill & FinOps Runbook

This runbook defines production operational standards for the **Industrial OEE Lakehouse** across Databricks Unity Catalog, Delta Lake, and `dbt-databricks`.

## 1. Daily Incremental vs. Safe Partitioned Backfill

`fct_oee_daily` uses `materialized='incremental'` with `incremental_strategy='merge'`, `unique_key=['event_date', 'machine_sk', 'product_sk']`, and `on_schema_change='fail'`.

* **Daily Incremental Run (Default):** Reprocesses a configurable lookback window (`oee_lookback_days`, default `3` days) to absorb late-arriving MES/PLC records without full-table rebuilds:
  ```bash
  dbt build --profiles-dir . --select fct_oee_daily
  ```
* **Safe Partitioned Backfill Protocol:** Never overwrite the full history without validating a single date slice first:
  1. **Single-Day Slice Test:** Run one `event_date` slice first:
     ```bash
     dbt run --profiles-dir . --select fct_oee_daily \
       --vars '{"backfill_start_date": "2024-05-01", "backfill_end_date": "2024-05-01"}'
     ```
  2. **Reconciliation Gate (0.00% Tolerance):** Verify parity between `industrial_lakehouse.silver.stg_oee_events` and `industrial_lakehouse.gold.fct_oee_daily` for that slice (event count, `SUM(delta_total_biscuits_made)`, `SUM(delta_good_biscuits_made)`, and duplicate check on `(event_date, machine_sk, product_sk)`).
  3. **Window Expansion:** Only after passing the reconciliation gate and `dbt test --select fct_oee_daily`, expand the backfill in controlled monthly slices.

## 2. Five Pillars of Data Observability

| Pillar | Implementation in This Repository |
| :--- | :--- |
| **Freshness** | Configured in `dbt_project/models/bronze/sources.yml` (`warn_after: 24h`, `error_after: 48h` on `ingested_at`) and checked via `dbt source freshness` in CI. |
| **Volume** | Row counts logged per table in `02_ingest_bronze.py` and event count reconciliation between Silver (`stg_oee_events`) and Gold (`fct_oee_daily`). |
| **Schema** | `on_schema_change='fail'` on `fct_oee_daily` and `inferSchema='false'` + column sanitization on Bronze ingestion to prevent silent schema drift. |
| **Distribution** | Automated dbt bounds and domain tests (`accepted_values`, `not_null`, `unique`, referential integrity, and OEE `[0, 1]` range assertions). |
| **Lineage** | Declarative DAG via `source()` and `ref()` in dbt + Unity Catalog table lineage (`bronze` -> `silver` -> `gold`). |

## 3. Delta Lake FinOps & Storage Maintenance

To prevent small-file degradation and control storage overhead across Delta tables, run scheduled maintenance (weekly or after large backfills):

```sql
-- Compact files and apply Liquid Clustering layout
OPTIMIZE industrial_lakehouse.bronze.raw_oee_total_report;
OPTIMIZE industrial_lakehouse.silver.stg_oee_events;
OPTIMIZE industrial_lakehouse.gold.fct_oee_daily;

-- Remove obsolete files older than 7 days (respecting Delta time-travel safety window)
VACUUM industrial_lakehouse.bronze.raw_oee_total_report RETAIN 168 HOURS;
VACUUM industrial_lakehouse.silver.stg_oee_events RETAIN 168 HOURS;
VACUUM industrial_lakehouse.gold.fct_oee_daily RETAIN 168 HOURS;
```

In `02_ingest_bronze.py`, pass widget parameter `run_optimize="true"` to trigger `OPTIMIZE` automatically after Bronze merges, or `dry_run="true"` to validate staging row counts without writing.
