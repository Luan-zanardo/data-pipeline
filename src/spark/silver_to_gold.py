"""Silver to Gold.

Lê as tabelas da camada Silver (via S3A), constrói o modelo dimensional e
grava na camada Gold em formato Delta Lake, aplicando a lógica de carga
incremental (SCD Type 2 para dimensões e checkpoint para fatos).
"""

import argparse
import logging
import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, lit, max, to_date

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver_to_gold")


def ler_tabela_silver(spark: SparkSession, base_path: str, tabela: str):
    path_silver = f"{base_path}/silver/{tabela}"
    logger.info(f"Lendo tabela Silver [{tabela}] de {path_silver}")
    return spark.read.format("delta").load(path_silver)


def processar_dim_data(spark: SparkSession, base_path: str) -> None:
    """Gera a dimensão de data. É sempre recriada (overwrite) pois é uma dimensão derivada."""
    logger.info("Processando dimensao [dim_data]...")
    pedidos = ler_tabela_silver(spark, base_path, "pedidos")
    path_gold = f"{base_path}/gold/dim_data"

    df_dim_data = pedidos.select(to_date(col("data_pedido")).alias("data")).distinct()
    df_dim_data = df_dim_data.withColumn("sk_data", date_format(col("data"), "yyyyMMdd").cast("int"))

    df_dim_data.write.format("delta").mode("overwrite").save(path_gold)
    logger.info(f"Dimensao [dim_data] gravada com sucesso.")


def processar_dimensao_scd2(spark: SparkSession, base_path: str, nome_dim: str, tabela_silver: str, chave_negocio: str, colunas_comparacao: list[str]):
    """Processa uma dimensão usando a lógica SCD Type 2."""
    logger.info(f"Processando dimensao [SCD-2] [{nome_dim}]...")
    path_gold = f"{base_path}/gold/{nome_dim}"
    
    # 1. Prepara os novos dados (source) da camada Silver
    df_source = ler_tabela_silver(spark, base_path, tabela_silver)
    
    # 2. Verifica se a tabela de destino (target) na Gold existe
    if not DeltaTable.isDeltaTable(spark, path_gold):
        logger.info(f"Tabela [{nome_dim}] não existe. Criando pela primeira vez.")
        (df_source.withColumn("is_current", lit(True))
                  .withColumn("start_date", current_timestamp())
                  .withColumn("end_date", lit(None).cast("timestamp"))
                  .write.format("delta").save(path_gold))
        return

    delta_target = DeltaTable.forPath(spark, path_gold)
    
    # 3. Cria um DataFrame com as atualizações a serem inseridas
    # Adiciona colunas de controle SCD2 e um hash para comparação
    df_updates = (
        df_source.alias("source")
        .join(delta_target.toDF().alias("target"), col(f"source.{chave_negocio}") == col(f"target.{chave_negocio}"))
        .where("target.is_current = true")
        .filter(" OR ".join([f"source.{c} <> target.{c}" for c in colunas_comparacao]))
        .select("source.*")
    )

    # 4. Expira os registros antigos que foram atualizados
    if df_updates.count() > 0:
        logger.info(f"Encontradas {df_updates.count()} atualizações. Expirando registros antigos...")
        condition_expire = f"target.{chave_negocio} IN ({','.join([str(row[chave_negocio]) for row in df_updates.select(chave_negocio).collect()])})"
        delta_target.update(
            condition=f"is_current = true AND {condition_expire}",
            set={"is_current": lit(False), "end_date": current_timestamp()}
        )

    # 5. Insere os registros novos e as novas versões dos registros atualizados
    df_inserts = (
        df_source.withColumn("is_current", lit(True))
                 .withColumn("start_date", current_timestamp())
                 .withColumn("end_date", lit(None).cast("timestamp"))
    )

    delta_target.alias("target").merge(
        df_inserts.alias("source"),
        f"target.{chave_negocio} = source.{chave_negocio}"
    ).whenNotMatchedInsertAll().execute()
    
    logger.info(f"Dimensao [{nome_dim}] atualizada com sucesso.")


def ler_checkpoint(spark: SparkSession, path: str) -> datetime:
    """Lê o valor do último checkpoint. Retorna uma data mínima se não existir."""
    try:
        df = spark.read.format("delta").load(path)
        return df.select("last_processed_date").first()[0]
    except Exception:
        logger.warning(f"Checkpoint não encontrado em {path}. Usando data mínima.")
        return datetime(1900, 1, 1)

def gravar_checkpoint(spark: SparkSession, path: str, value: datetime):
    """Salva o novo valor do checkpoint."""
    logger.info(f"Gravando novo checkpoint [{value}] em {path}")
    spark.createDataFrame([(value,)], ["last_processed_date"]).write.format("delta").mode("overwrite").save(path)


def processar_fato_vendas(spark: SparkSession, base_path: str) -> None:
    logger.info("Processando fato [fato_vendas]...")
    checkpoint_path = f"{base_path}/gold/_checkpoints/fato_vendas"
    
    last_processed_date = ler_checkpoint(spark, checkpoint_path)
    logger.info(f"Última data processada (checkpoint): {last_processed_date}")

    pedidos = ler_tabela_silver(spark, base_path, "pedidos").filter(col("data_pedido") > last_processed_date)
    
    if pedidos.rdd.isEmpty():
        logger.info("Nenhum novo pedido encontrado para processar.")
        return

    new_checkpoint = pedidos.select(max(col("data_pedido"))).first()[0]
    logger.info(f"Novos pedidos encontrados até: {new_checkpoint}")

    pedido_itens = ler_tabela_silver(spark, base_path, "pedido_itens")
    colunas_para_remover = ["_input_file_name", "ingestion_date", "_updated_at", "_loaded_at"]

    fato = pedidos.join(
        pedido_itens.drop(*colunas_para_remover, "id"),
        pedidos.id == pedido_itens.pedido_id,
        "inner"
    )

    (fato.write.format("delta")
               .mode("append")  # Alterado para append
               .save(f"{base_path}/gold/fato_vendas"))
    
    gravar_checkpoint(spark, checkpoint_path, new_checkpoint)
    logger.info("Fato [fato_vendas] atualizada com sucesso.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Silver to Gold")
    parser.add_argument("--date", required=True, type=str, help="Data de ingestão no formato YYYY-MM-DD.")
    args = parser.parse_args()

    spark = get_spark_session("SilverToGold")
    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"

    try:
        processar_dim_data(spark, base_path)
        processar_dimensao_scd2(spark, base_path, "dim_cliente", "usuarios", "id", ["nome", "email", "telefone"])
        processar_dimensao_scd2(spark, base_path, "dim_produto", "produtos", "id", ["nome", "descricao", "preco"])
        processar_fato_vendas(spark, base_path)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
