# Databricks notebook source
import logging
import os
import re
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("oee_bronze_ingestion")


def get_param(name: str, default: str) -> str:
    """Read parameter from Databricks widgets with fallback to env vars/default."""
    try:
        dbutils.widgets.text(name, default)  # type: ignore[name-defined]
        value = dbutils.widgets.get(name).strip()
        return value or default
    except NameError:
        return os.getenv(name.upper(), default)


CATALOG = get_param("catalog", "industrial_lakehouse")
SCHEMA = get_param("schema", "bronze")
VOLUME_PATH = get_param("volume_path", f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/oee")
DRY_RUN = get_param("dry_run", "false").lower() == "true"
RUN_OPTIMIZE = get_param("run_optimize", "false").lower() == "true"

# 1:1 mapping between raw CSV file and isolated Bronze Delta table
FILES_TO_TABLES = {
    "Machine.csv": "raw_oee_machines",
    "Products.csv": "raw_oee_products",
    "Target Speed.csv": "raw_oee_target_speed",
    "Total Report.csv": "raw_oee_total_report",
}


def sanitize_column_name(col_name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", col_name.strip()).strip("_").lower()


def ingest_csv_to_bronze(file_name: str, table_name: str) -> None:
    source_path = f"{VOLUME_PATH}/{file_name}"
    target_table = f"{CATALOG}.{SCHEMA}.{table_name}"
    logger.info("Starting Bronze ingestion: source=%s target=%s dry_run=%s", source_path, target_table, DRY_RUN)

    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .load(source_path)
    )

    # Normalize column names only to ensure Delta Lake compatibility
    renamed_cols = [sanitize_column_name(c) for c in df_raw.columns]
    df_clean_cols = df_raw.toDF(*renamed_cols)

    # Deterministic SHA-256 hash across all business columns for idempotency
    business_cols = df_clean_cols.columns
    df_staged = (
        df_clean_cols
        .withColumn(
            "row_hash",
            F.sha2(
                F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols]),
                256,
            ),
        )
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_date", F.current_date())
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["row_hash"])
    )

    staged_count = df_staged.count()
    if DRY_RUN:
        logger.info("[DRY_RUN] %s -> %d deduplicated rows ready (no write executed)", target_table, staged_count)
        return

    if not spark.catalog.tableExists(target_table):
        (
            df_staged.write.format("delta")
            .clusterBy("ingestion_date")
            .saveAsTable(target_table)
        )
        action = "CREATED"
    else:
        delta_target = DeltaTable.forName(spark, target_table)
        (
            delta_target.alias("t")
            .merge(df_staged.alias("s"), "t.row_hash = s.row_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
        action = "MERGED"

    if RUN_OPTIMIZE:
        spark.sql(f"OPTIMIZE {target_table}")
        logger.info("[OPTIMIZED] %s", target_table)

    total_rows = spark.table(target_table).count()
    logger.info("[%s] %s -> staged=%d total_rows=%d", action, target_table, staged_count, total_rows)


failed_tables = []
for csv_file, bronze_table in FILES_TO_TABLES.items():
    try:
        ingest_csv_to_bronze(csv_file, bronze_table)
    except Exception as exc:
        logger.exception("Failed to ingest %s into %s.%s.%s: %s", csv_file, CATALOG, SCHEMA, bronze_table, exc)
        failed_tables.append((bronze_table, str(exc)))

if failed_tables:
    raise RuntimeError(f"Bronze ingestion failed for tables: {failed_tables}")
    
