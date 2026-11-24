# Databricks notebook source
import re
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "industrial_lakehouse"
SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/oee"

# Mapeamento 1:1 entre arquivo bruto e tabela Delta isolada
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

    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("inferSchema", "false")
        .load(source_path)
    )

    # Normaliza apenas nomes de colunas para compatibilidade com Delta Lake
    renamed_cols = [sanitize_column_name(c) for c in df_raw.columns]
    df_clean_cols = df_raw.toDF(*renamed_cols)

    # Hash determinístico sobre todas as colunas de negócio (idempotência)
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

    total_rows = spark.table(target_table).count()
    print(f"[{action}] {target_table} -> {total_rows} linhas")


for csv_file, bronze_table in FILES_TO_TABLES.items():
    ingest_csv_to_bronze(csv_file, bronze_table)
