"""Bronze to Silver.

Lê as tabelas da camada Bronze, realiza a limpeza, tipagem, padronização e
deduplicação dos dados (mantendo o registro mais recente) e grava na camada Silver
usando a operação MERGE (upsert) do Delta Lake para manter o estado atualizado.
"""

import argparse
import logging
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, desc, lower, row_number, trim, upper, to_timestamp
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("bronze_to_silver")


def get_spark_session() -> SparkSession:
    """Cria ou recupera a SparkSession configurada com suporte ao Delta Lake."""
    logger.info("Inicializando SparkSession com suporte a Delta Lake...")
    return SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.sources.default", "delta") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .getOrCreate()


def aplicar_transformacoes(df, tabela):
    """Aplica tipagem, padronização de campos e limpezas específicas por tabela."""
    logger.info(f"Aplicando transformações específicas para a tabela {tabela}...")

    # Primeiro, deduplica os registros da Bronze baseando-se no ID e pegando o mais recente (_loaded_at)
    window_spec = Window.partitionBy("id").orderBy(desc("_loaded_at"))
    df_dedup = df.withColumn("rn", row_number().over(window_spec)) \
                 .filter("rn = 1") \
                 .drop("rn")

    # Regras de transformação e limpeza para cada uma das 10 tabelas
    if tabela == "usuarios":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("nome", trim(col("nome"))) \
            .withColumn("email", lower(trim(col("email")))) \
            .filter(col("id").isNotNull() & (col("nome") != ""))

    elif tabela == "produtos":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("nome", trim(col("nome"))) \
            .withColumn("descricao", trim(col("descricao"))) \
            .withColumn("preco", col("preco").cast("decimal(10,2)")) \
            .withColumn("estoque", col("estoque").cast("integer")) \
            .withColumn("categoria_id", col("categoria_id").cast("integer")) \
            .filter(col("id").isNotNull() & (col("nome") != "") & (col("preco") >= 0))

    elif tabela == "pedidos":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("usuario_id", col("usuario_id").cast("integer")) \
            .withColumn("data_pedido", to_timestamp(col("data_pedido"))) \
            .withColumn("status", upper(trim(col("status")))) \
            .filter(col("id").isNotNull() & col("usuario_id").isNotNull() & col("data_pedido").isNotNull())

    elif tabela == "enderecos":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("usuario_id", col("usuario_id").cast("integer")) \
            .withColumn("rua", trim(col("rua"))) \
            .withColumn("cidade", trim(col("cidade"))) \
            .withColumn("estado", upper(trim(col("estado")))) \
            .withColumn("zip_code", trim(col("zip_code"))) \
            .withColumn("pais", trim(col("pais"))) \
            .filter(col("id").isNotNull() & col("usuario_id").isNotNull())

    elif tabela == "categorias":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("nome", trim(col("nome"))) \
            .withColumn("descricao", trim(col("descricao"))) \
            .filter(col("id").isNotNull() & (col("nome") != ""))

    elif tabela == "pedido_itens":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("pedido_id", col("pedido_id").cast("integer")) \
            .withColumn("produto_id", col("produto_id").cast("integer")) \
            .withColumn("quantidade", col("quantidade").cast("integer")) \
            .withColumn("preco", col("preco").cast("decimal(10,2)")) \
            .filter(col("id").isNotNull() & col("pedido_id").isNotNull() & col("produto_id").isNotNull() & (col("quantidade") > 0))

    elif tabela == "pagamentos":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("pedido_id", col("pedido_id").cast("integer")) \
            .withColumn("forma_pagamento", upper(trim(col("forma_pagamento")))) \
            .withColumn("quantia", col("quantia").cast("decimal(10,2)")) \
            .withColumn("data_pagamento", to_timestamp(col("data_pagamento"))) \
            .filter(col("id").isNotNull() & col("pedido_id").isNotNull() & (col("quantia") >= 0))

    elif tabela == "envio":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("pedido_id", col("pedido_id").cast("integer")) \
            .withColumn("endereco_id", col("endereco_id").cast("integer")) \
            .withColumn("data_envio", to_timestamp(col("data_envio"))) \
            .withColumn("data_entrega", to_timestamp(col("data_entrega"))) \
            .withColumn("status", upper(trim(col("status")))) \
            .filter(col("id").isNotNull() & col("pedido_id").isNotNull() & col("endereco_id").isNotNull())

    elif tabela == "avaliacoes":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("usuario_id", col("usuario_id").cast("integer")) \
            .withColumn("produto_id", col("produto_id").cast("integer")) \
            .withColumn("avaliacao", col("avaliacao").cast("integer")) \
            .withColumn("comentario", trim(col("comentario"))) \
            .withColumn("data_avaliacao", to_timestamp(col("data_avaliacao"))) \
            .filter(col("id").isNotNull() & col("usuario_id").isNotNull() & col("produto_id").isNotNull() & col("avaliacao").between(1, 5))

    elif tabela == "carrinho":
        df_clean = df_dedup \
            .withColumn("id", col("id").cast("integer")) \
            .withColumn("usuario_id", col("usuario_id").cast("integer")) \
            .withColumn("produto_id", col("produto_id").cast("integer")) \
            .withColumn("quantidade", col("quantidade").cast("integer")) \
            .filter(col("id").isNotNull() & col("usuario_id").isNotNull() & col("produto_id").isNotNull() & (col("quantidade") > 0))

    else:
        df_clean = df_dedup

    # Adiciona metadado de atualização na Silver
    df_clean = df_clean.withColumn("_updated_at", current_timestamp())

    return df_clean


def processar_tabela_bronze(spark: SparkSession, tabela: str, data_especifica: str | None, base_path: Path) -> None:
    """Lê a tabela da camada Bronze, limpa/deduplica e escreve na Silver via MERGE."""
    path_bronze = base_path / "bronze" / tabela
    path_silver = base_path / "silver" / tabela

    if not path_bronze.exists():
        logger.warning(f"Diretório da tabela {tabela} não encontrado na Bronze: {path_bronze}")
        return

    logger.info(f"Lendo tabela [{tabela}] da Bronze: {path_bronze}")

    # Ler dados em Delta da Bronze
    df_bronze = spark.read.format("delta").load(str(path_bronze))

    # Filtrar por data lógica específica de processamento, se fornecida
    if data_especifica:
        df_bronze = df_bronze.filter(col("ingestion_date") == data_especifica)
        if df_bronze.limit(1).count() == 0:
            logger.info(f"Nenhum registro encontrado na Bronze para a tabela {tabela} na data {data_especifica}.")
            return

    # Aplicar transformações e deduplicação
    df_silver_source = aplicar_transformacoes(df_bronze, tabela)

    # Escrever na Silver usando MERGE (upsert)
    if not DeltaTable.isDeltaTable(spark, str(path_silver)):
        logger.info(f"Tabela Silver [{tabela}] não existe. Criando pela primeira vez em: {path_silver}")
        # Criar a tabela Silver inicial
        df_silver_source.write \
            .format("delta") \
            .mode("overwrite") \
            .save(str(path_silver))
    else:
        logger.info(f"Mesclando dados na tabela Silver [{tabela}] em: {path_silver}")
        delta_table = DeltaTable.forPath(spark, str(path_silver))

        # Realiza MERGE baseado no campo ID
        delta_table.alias("target") \
            .merge(
                df_silver_source.alias("source"),
                "target.id = source.id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    logger.info(f"Tabela [{tabela}] processada com sucesso para a Silver.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Bronze to Silver")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data específica de processamento no formato YYYY-MM-DD. Se omitida, processa todas as datas da Bronze."
    )
    args = parser.parse_args()

    # Caminho do Data Lake
    base_datalake = Path(os.environ.get("DATALAKE_PATH", "datalake"))

    spark = get_spark_session()

    try:
        # Descobrir tabelas dinamicamente na Bronze
        path_bronze_base = base_datalake / "bronze"
        if not path_bronze_base.exists():
            logger.error(f"Diretório base da Bronze não existe: {path_bronze_base}")
            return

        tabelas = [
            d.name for d in path_bronze_base.iterdir()
            if d.is_dir() and not d.name.startswith("_")
        ]

        if not tabelas:
            logger.info("Nenhuma tabela encontrada na camada Bronze para processamento.")
            return

        logger.info(f"Tabelas identificadas na Bronze: {tabelas}")

        for tabela in tabelas:
            try:
                processar_tabela_bronze(spark, tabela, args.date, base_datalake)
            except Exception as e:
                logger.error(f"Erro ao processar tabela {tabela}: {str(e)}", exc_info=True)

    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
