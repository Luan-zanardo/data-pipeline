"""Gold to Postgres.

Lê as tabelas Delta da camada Gold e grava em um Postgres de destino via JDBC.
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Importa a função centralizada
from spark.utils import get_spark_session

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold_to_postgres")


def montar_config_postgres() -> dict[str, str]:
    """Monta a configuração JDBC do Postgres de destino a partir de variáveis de ambiente."""
    host = os.environ.get("DEST_DB_HOST")
    port = os.environ.get("DEST_DB_PORT", "5432")
    database = os.environ.get("DEST_DB_NAME")
    user = os.environ.get("DEST_DB_USER")
    password = os.environ.get("DEST_DB_PASSWORD")
    sslmode = os.environ.get("DEST_DB_SSLMODE", "require")

    if not all([host, database, user, password]):
        raise ValueError("Variáveis de ambiente do banco de destino (DEST_DB_*) não estão configuradas.")

    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}?sslmode={sslmode}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def processar_tabela_gold(spark: SparkSession, tabela: str, base_path: str, jdbc_config: dict[str, str]) -> None:
    """Lê uma tabela Gold em Delta e grava no Postgres de destino."""
    path_gold = f"{base_path}/gold/{tabela}"
    logger.info(f"Lendo tabela Gold [{tabela}] em: {path_gold}")
    df_gold = spark.read.format("delta").load(path_gold)

    if tabela in ["dim_cliente", "dim_produto"]:
        logger.info(f"Filtrando somente registros atuais da dimensao [{tabela}]...")
        df_gold = df_gold.filter(col("is_current") == lit(True))

    total_registros = df_gold.count()
    logger.info(f"Gravando tabela [{tabela}] no Postgres de destino ({total_registros} registros)...")

    (df_gold.write
        .format("jdbc")
        .option("url", jdbc_config["url"])
        .option("dbtable", tabela)
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("driver", jdbc_config["driver"])
        .option("truncate", "true")
        .mode("overwrite")
        .save())

    logger.info(f"Tabela [{tabela}] gravada no Postgres com sucesso.")


def main() -> None:
    base_datalake = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"

    spark = get_spark_session("GoldToPostgres")

    try:
        jdbc_config = montar_config_postgres()
        tabelas = ["dim_cliente", "dim_produto", "dim_data", "fato_vendas"]
        logger.info(f"Tabelas Gold planejadas para envio ao Postgres: {tabelas}")

        for tabela in tabelas:
            try:
                processar_tabela_gold(spark, tabela, base_datalake, jdbc_config)
            except Exception as e:
                logger.error(f"Erro ao enviar tabela Gold {tabela} para o Postgres: {str(e)}", exc_info=True)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
