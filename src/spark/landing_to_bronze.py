"""Landing to Bronze.

Lê os dados em formato CSV bruto da camada Landing (via S3A) e grava na
camada Bronze em formato Delta Lake. A lista de tabelas a serem processadas
é lida a partir do arquivo de manifesto da ingestão.
"""

import argparse
import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("landing_to_bronze")


def get_tables_from_manifest(spark: SparkSession, manifest_path: str) -> list[str]:
    """Lê o arquivo de manifesto e extrai os nomes das tabelas."""
    try:
        manifest_rdd = spark.sparkContext.wholeTextFiles(manifest_path).take(1)
        if not manifest_rdd:
            logger.error(f"Arquivo de manifesto não encontrado em: {manifest_path}")
            return []

        manifest_content = json.loads(manifest_rdd[0][1])
        # A chave 'tabelas' contém uma lista de dicionários, cada um com uma chave 'tabela'
        tables = [item["tabela"] for item in manifest_content.get("tabelas", [])]

        unique_tables = sorted(list(set(tables)))
        logger.info(f"Tabelas encontradas no manifesto: {unique_tables}")
        return unique_tables

    except Exception as e:
        logger.error(f"Erro ao ler ou processar o manifesto em {manifest_path}: {e}", exc_info=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Landing to Bronze")
    parser.add_argument("--date", required=True, type=str, help="Data de ingestão no formato YYYY-MM-DD.")
    args = parser.parse_args()

    spark = get_spark_session("LandingToBronze")

    # Define o caminho base do Data Lake no S3 (MinIO)
    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"
    # Corrigido: caminho e nome do arquivo de manifesto
    manifest_path = f"{base_path}/landing/_manifests/ingestion_{args.date}.json"

    try:
        tabelas = get_tables_from_manifest(spark, manifest_path)
        if not tabelas:
            raise RuntimeError("Nenhuma tabela encontrada no manifesto para processar.")

        for tabela in tabelas:
            path_landing = f"{base_path}/landing/{tabela}/ingestion_date={args.date}/*.csv"
            path_bronze = f"{base_path}/bronze/{tabela}"

            logger.info(f"Processando tabela [{tabela}] de {path_landing} para {path_bronze}")

            try:
                df_landing = spark.read.option("header", "true").option("inferSchema", "true").csv(path_landing)

                if df_landing.rdd.isEmpty():
                    logger.warning(f"Nenhum dado encontrado para a tabela {tabela} em {path_landing}.")
                    continue

                df_bronze = (
                    df_landing
                    .withColumn("_input_file_name", input_file_name())
                    .withColumn("_loaded_at", current_timestamp())
                    .withColumn("ingestion_date", lit(args.date))
                )

                (df_bronze.write
                    .format("delta")
                    .mode("append")
                    .partitionBy("ingestion_date")
                    .option("mergeSchema", "true")
                    .save(path_bronze))

                logger.info(f"Tabela [{tabela}] gravada na Bronze com sucesso.")

            except Exception as e:
                # Se o Spark não encontrar arquivos, ele lança uma exceção.
                if "Path does not exist" in str(e):
                    logger.warning(f"Caminho não encontrado para a tabela {tabela}: {path_landing}")
                else:
                    logger.error(f"Erro ao processar tabela {tabela}: {str(e)}", exc_info=True)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
