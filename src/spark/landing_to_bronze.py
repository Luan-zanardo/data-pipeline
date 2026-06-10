"""Etapa 4 - Transformação Spark: Landing -> Bronze (Delta Lake).

Lê os arquivos brutos da camada Landing (CSV) com Apache Spark e grava
na camada Bronze no formato Delta Lake, sem transformações de negócio,
apenas adicionando metadados de rastreabilidade da origem.

Refs: issue #6
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LANDING_PATH = "data/landing"
BRONZE_PATH = "data/bronze"


def get_spark() -> SparkSession:
    """Cria a SparkSession com suporte a Delta Lake."""
    return (
        SparkSession.builder.appName("landing_to_bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = get_spark()

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{LANDING_PATH}/*.csv")
    )

    # Rastreabilidade da origem
    df = df.withColumn("_arquivo_origem", F.input_file_name()).withColumn(
        "_data_ingestao", F.current_timestamp()
    )

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
    )

    print(f"Bronze gravada em {BRONZE_PATH} ({df.count()} linhas)")
    spark.stop()


if __name__ == "__main__":
    main()
