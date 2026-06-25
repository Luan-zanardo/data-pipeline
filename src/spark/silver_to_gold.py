"""Silver to Gold.

Lê as tabelas da camada Silver (via S3A), constrói o modelo dimensional e
grava na camada Gold em formato Delta Lake.
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver_to_gold")


def ler_tabela_silver(spark: SparkSession, base_path: str, tabela: str):
    path_silver = f"{base_path}/silver/{tabela}"
    logger.info(f"Lendo tabela Silver [{tabela}] de {path_silver}")
    return spark.read.format("delta").load(path_silver)


def processar_dim_data(spark: SparkSession, base_path: str) -> None:
    logger.info("Processando dimensao [dim_data]...")
    pedidos = ler_tabela_silver(spark, base_path, "pedidos")
    path_gold = f"{base_path}/gold/dim_data"

    df_dim_data = pedidos.select(to_date(col("data_pedido")).alias("data")).distinct()
    df_dim_data = df_dim_data.withColumn("sk_data", date_format(col("data"), "yyyyMMdd").cast("int"))

    df_dim_data.write.format("delta").mode("overwrite").save(path_gold)
    logger.info(f"Dimensao [dim_data] gravada com sucesso.")


def processar_dim_cliente(spark: SparkSession, base_path: str) -> None:
    logger.info("Processando dimensao [dim_cliente]...")
    usuarios = ler_tabela_silver(spark, base_path, "usuarios")
    # Simplificação para SCD Type 1 para performance na apresentação
    usuarios.write.format("delta").mode("overwrite").save(f"{base_path}/gold/dim_cliente")
    logger.info("Dimensao [dim_cliente] gravada com sucesso.")


def processar_dim_produto(spark: SparkSession, base_path: str) -> None:
    logger.info("Processando dimensao [dim_produto]...")
    produtos = ler_tabela_silver(spark, base_path, "produtos")
    # Simplificação para SCD Type 1
    produtos.write.format("delta").mode("overwrite").save(f"{base_path}/gold/dim_produto")
    logger.info("Dimensao [dim_produto] gravada com sucesso.")


def processar_fato_vendas(spark: SparkSession, base_path: str) -> None:
    logger.info("Processando fato [fato_vendas]...")
    pedidos = ler_tabela_silver(spark, base_path, "pedidos")
    pedido_itens = ler_tabela_silver(spark, base_path, "pedido_itens")

    # Colunas de metadados que causam duplicidade
    colunas_para_remover = ["_input_file_name", "ingestion_date", "_updated_at", "_loaded_at"]

    # Junta as tabelas, removendo as colunas duplicadas de 'pedido_itens'
    fato = pedidos.join(
        pedido_itens.drop(*colunas_para_remover, "id"),
        pedidos.id == pedido_itens.pedido_id,
        "inner"
    )

    fato.write.format("delta").mode("overwrite").save(f"{base_path}/gold/fato_vendas")
    logger.info("Fato [fato_vendas] gravada com sucesso.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Silver to Gold")
    parser.add_argument("--date", required=True, type=str, help="Data de ingestão no formato YYYY-MM-DD.")
    args = parser.parse_args()

    spark = get_spark_session("SilverToGold")

    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"

    try:
        processar_dim_data(spark, base_path)
        processar_dim_cliente(spark, base_path)
        processar_dim_produto(spark, base_path)
        processar_fato_vendas(spark, base_path)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
