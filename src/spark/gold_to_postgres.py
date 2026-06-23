"""Gold to Postgres.

Le as tabelas Delta da camada Gold e grava em um Postgres de destino via JDBC,
disponibilizando as tabelas dimensionais e a fato para consumo analitico.
"""

import argparse
import logging
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Configuracao de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gold_to_postgres")


def get_spark_session() -> SparkSession:
    """Cria ou recupera a SparkSession configurada com suporte ao Delta Lake e JDBC Postgres."""
    logger.info("Inicializando SparkSession com suporte a Delta Lake e JDBC Postgres...")
    return SparkSession.builder \
        .appName("GoldToPostgres") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4") \
        .config("spark.sql.sources.default", "delta") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .getOrCreate()


def tabela_delta_existe(spark: SparkSession, path_tabela: Path) -> bool:
    """Verifica se um caminho existe e contem uma tabela Delta."""
    return path_tabela.exists() and DeltaTable.isDeltaTable(spark, str(path_tabela))


def montar_config_postgres() -> dict[str, str]:
    """Monta a configuracao JDBC do Postgres de destino a partir de variaveis de ambiente."""
    host = os.environ.get("DEST_DB_HOST")
    port = os.environ.get("DEST_DB_PORT", "5432")
    database = os.environ.get("DEST_DB_NAME")
    user = os.environ.get("DEST_DB_USER")
    password = os.environ.get("DEST_DB_PASSWORD")
    sslmode = os.environ.get("DEST_DB_SSLMODE", "require")

    variaveis_obrigatorias = {
        "DEST_DB_HOST": host,
        "DEST_DB_NAME": database,
        "DEST_DB_USER": user,
        "DEST_DB_PASSWORD": password,
    }
    faltantes = [nome for nome, valor in variaveis_obrigatorias.items() if not valor]
    if faltantes:
        raise ValueError(f"Variaveis de ambiente obrigatorias ausentes: {faltantes}")

    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}?sslmode={sslmode}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def processar_tabela_gold(spark: SparkSession, tabela: str, base_path: Path, jdbc_config: dict[str, str]) -> None:
    """Le uma tabela Gold em Delta e grava no Postgres de destino."""
    path_gold = base_path / "gold" / tabela

    if not tabela_delta_existe(spark, path_gold):
        logger.warning(f"Tabela Gold [{tabela}] nao encontrada em: {path_gold}")
        return

    logger.info(f"Lendo tabela Gold [{tabela}] em: {path_gold}")
    df_gold = spark.read.format("delta").load(str(path_gold))

    if tabela in ["dim_cliente", "dim_produto"]:
        logger.info(f"Filtrando somente registros atuais da dimensao [{tabela}]...")
        df_gold = df_gold.filter(col("is_current") == lit(True))

    logger.info(f"Gravando tabela [{tabela}] no Postgres de destino...")
    df_gold.write \
        .format("jdbc") \
        .option("url", jdbc_config["url"]) \
        .option("dbtable", tabela) \
        .option("user", jdbc_config["user"]) \
        .option("password", jdbc_config["password"]) \
        .option("driver", jdbc_config["driver"]) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    logger.info(f"Tabela [{tabela}] gravada no Postgres com sucesso. Registros enviados: {df_gold.count()}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Gold to Postgres")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data especifica no formato YYYY-MM-DD. Mantido para compatibilidade com os scripts Spark."
    )
    args = parser.parse_args()

    if args.date:
        logger.info(f"Parametro --date recebido ({args.date}); exportacao Gold para Postgres envia o estado atual.")

    # Caminho do Data Lake
    base_datalake = Path(os.environ.get("DATALAKE_PATH", "datalake"))

    spark = get_spark_session()

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
