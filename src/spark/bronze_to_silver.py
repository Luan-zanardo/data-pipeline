"""Etapa 4 - Transformação Spark: Bronze -> Silver (Delta Lake).

Lê a camada Bronze (Delta Lake) com Apache Spark, aplica limpeza e
tratamento (padronização de colunas, remoção de duplicatas e de registros
nulos) e grava o resultado na camada Silver no formato Delta Lake.

Refs: issue #6
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"


def get_spark() -> SparkSession:
    """Cria a SparkSession com suporte a Delta Lake."""
    return (
        SparkSession.builder.appName("bronze_to_silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def padronizar_colunas(df: DataFrame) -> DataFrame:
    """Normaliza nomes de colunas para snake_case minúsculo."""
    for antigo in df.columns:
        novo = re.sub(r"[^0-9a-zA-Z]+", "_", antigo.strip().lower()).strip("_")
        if novo != antigo:
            df = df.withColumnRenamed(antigo, novo)
    return df


def main() -> None:
    spark = get_spark()

    df = spark.read.format("delta").load(BRONZE_PATH)

    # Limpeza e tratamento
    df = padronizar_colunas(df)
    df = df.dropDuplicates()
    df = df.na.drop(how="all")

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )

    print(f"Silver gravada em {SILVER_PATH} ({df.count()} linhas)")
    spark.stop()


if __name__ == "__main__":
    main()
