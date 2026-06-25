"""
Gold to PostgreSQL (Serving Layer).

Lê as tabelas dimensionais da camada Gold (Delta Lake) e as grava em um
banco de dados PostgreSQL, que servirá como a camada de consumo (Serving Layer)
para as ferramentas de BI.
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold_to_postgres")

def get_db_properties():
    """Retorna as propriedades de conexão JDBC para o banco de destino."""
    return {
        "user": os.environ.get("DEST_DB_USER", "postgres"),
        "password": os.environ.get("DEST_DB_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver"
    }

def main() -> None:
    """Função principal para carregar dados da Gold para o PostgreSQL."""
    spark = get_spark_session("GoldToPostgres")

    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"
    db_url = f"jdbc:postgresql://{os.environ.get('DEST_DB_HOST', 'postgres-serving')}:{os.environ.get('DEST_DB_PORT', '5432')}/{os.environ.get('DEST_DB_NAME', 'postgres')}"
    db_properties = get_db_properties()

    # Lista de tabelas na camada Gold a serem carregadas
    tabelas_gold = ["dim_cliente", "dim_produto", "dim_data", "fato_vendas"]

    logger.info(f"Iniciando carga da camada Gold para o banco de dados de destino em {db_url}")

    try:
        for tabela in tabelas_gold:
            path_gold = f"{base_path}/gold/{tabela}"
            logger.info(f"Lendo tabela '{tabela}' de {path_gold}...")

            df_gold = spark.read.format("delta").load(path_gold)

            # Para as dimensões SCD-2, carregamos apenas a visão atual dos dados
            if "is_current" in df_gold.columns:
                df_gold = df_gold.filter(col("is_current") == True)

            logger.info(f"Gravando tabela '{tabela}' no PostgreSQL...")
            
            # Grava os dados no PostgreSQL, sobrescrevendo a tabela
            (df_gold.write
                .jdbc(url=db_url,
                      table=tabela,
                      mode="overwrite",
                      properties=db_properties))
            
            logger.info(f"Tabela '{tabela}' carregada com sucesso no PostgreSQL.")

    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()

if __name__ == "__main__":
    main()
