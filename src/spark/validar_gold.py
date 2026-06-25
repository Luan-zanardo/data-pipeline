"""Validação da camada Gold.

Lê as tabelas Delta da camada Gold (via S3A) e imprime métricas de contagem.
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validar_gold")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validação Spark da camada Gold")
    parser.add_argument("--date", required=True, type=str, help="Data de ingestão no formato YYYY-MM-DD.")
    args = parser.parse_args()

    spark = get_spark_session("ValidarGold")

    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"

    erros = []
    metricas = {"counts": {}}

    try:
        tabelas_gold = ["dim_cliente", "dim_produto", "dim_data", "fato_vendas"]
        logger.info(f"Iniciando validação para as tabelas: {tabelas_gold}")

        for tabela in tabelas_gold:
            path_gold = f"{base_path}/gold/{tabela}"
            try:
                if not DeltaTable.isDeltaTable(spark, path_gold):
                    erros.append(f"Tabela Gold [{tabela}] não é uma tabela Delta ou não existe em {path_gold}")
                    metricas["counts"][tabela] = 0
                else:
                    df = spark.read.format("delta").load(path_gold)
                    count = df.count()
                    metricas["counts"][tabela] = count
                    if count == 0:
                        erros.append(f"Alerta: Tabela Gold [{tabela}] está vazia.")
            except Exception as e:
                erros.append(f"Erro ao ler a tabela Gold [{tabela}]: {str(e)}")
                metricas["counts"][tabela] = None

        logger.info("--- Métricas da Camada Gold ---")
        for tabela, count in metricas["counts"].items():
            logger.info(f"Tabela [{tabela}]: {count} registros.")
        logger.info("-----------------------------")

        if erros:
            for erro in erros:
                logger.error(erro)
            raise RuntimeError("Validação da camada Gold falhou.")

        logger.info("Validação da camada Gold concluída com sucesso.")

    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
