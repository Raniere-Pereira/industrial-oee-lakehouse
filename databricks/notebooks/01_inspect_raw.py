# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Inspeção dos Arquivos Raw (OEE)
# MAGIC
# MAGIC Lista e inspeciona todos os arquivos `.csv` disponíveis na landing zone do Unity Catalog Volume
# MAGIC `/Volumes/industrial_lakehouse/bronze/landing_zone/oee/`.

# COMMAND ----------

from pathlib import Path

try:
    spark  # type: ignore[name-defined]
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("01_inspect_raw_oee").getOrCreate()

LANDING_ZONE_PATH = "/Volumes/industrial_lakehouse/bronze/landing_zone/oee/"


def format_size(num_bytes: int) -> str:
    """Formata tamanho em bytes para leitura humana."""
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{num_bytes} B"


def list_csv_files(directory_path: str) -> list[dict]:
    """Lista arquivos .csv no diretório usando dbutils (Databricks) ou pathlib (fallback)."""
    if "dbutils" in globals():
        entries = dbutils.fs.ls(directory_path)  # type: ignore[name-defined]
        files = [
            {"name": entry.name, "path": entry.path, "size": entry.size}
            for entry in entries
            if not entry.isDir() and entry.name.lower().endswith(".csv")
        ]
    else:
        base_path = Path(directory_path)
        files = [
            {"name": entry.name, "path": str(entry), "size": entry.stat().st_size}
            for entry in base_path.iterdir()
            if entry.is_file() and entry.name.lower().endswith(".csv")
        ]

    return sorted(files, key=lambda item: item["name"])


# COMMAND ----------

csv_files = list_csv_files(LANDING_ZONE_PATH)

print(f"Diretório inspecionado: {LANDING_ZONE_PATH}")
print(f"Total de arquivos .csv encontrados: {len(csv_files)}\n")

if not csv_files:
    print("Nenhum arquivo .csv encontrado na landing zone.")
else:
    for idx, file_info in enumerate(csv_files, start=1):
        file_name = file_info["name"]
        file_path = file_info["path"]
        file_size = file_info["size"]

        print("=" * 80)
        print(f"[{idx}/{len(csv_files)}] Nome: {file_name}")
        print(f"Tamanho: {file_size:,} bytes ({format_size(file_size)})")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(file_path)
        )

        columns = df.columns
        total_rows = df.count()

        print(f"Colunas ({len(columns)}): {columns}")
        print(f"Total de linhas: {total_rows:,}")
        print("Amostra (5 linhas):")
        df.show(5, truncate=False)
