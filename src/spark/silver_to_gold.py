"""Silver to Gold.

Lê as tabelas da camada Silver, constrói o modelo dimensional e grava na
camada Gold em formato Delta Lake, aplicando a lógica de carga incremental
(SCD Type 2 para dimensões e checkpoint para fatos).
"""

import argparse
import logging
import os
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, lit, max, to_date, expr

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


def processar_dimensao_scd2(spark: SparkSession, base_path: str, nome_dim: str, tabela_silver: str, chave_negocio: str, colunas_comparacao: list[str]):
    logger.info(f"Processando dimensao [SCD-2] [{nome_dim}]...")
    path_gold = f"{base_path}/gold/{nome_dim}"
    target_key_name = f"id_{nome_dim.split('_')[1]}"

    df_source = ler_tabela_silver(spark, base_path, tabela_silver)

    # Se a tabela Gold não existe, cria pela primeira vez com o esquema correto.
    if not DeltaTable.isDeltaTable(spark, path_gold):
        logger.info(f"Tabela [{nome_dim}] não existe. Criando pela primeira vez.")
        (df_source.withColumn("is_current", lit(True))
                  .withColumn("start_date", current_timestamp())
                  .withColumn("end_date", lit(None).cast("timestamp"))
                  .withColumnRenamed(chave_negocio, target_key_name)
                  .write.format("delta").save(path_gold))
        return

    delta_target = DeltaTable.forPath(spark, path_gold)
    df_target_current = delta_target.toDF().where(col("is_current") == True)

    # 1. Identifica registros novos ou alterados
    join_condition = col(f"s.{chave_negocio}") == col(f"t.{target_key_name}")
    compare_condition = " OR ".join([f"s.{c} <> t.{c}" for c in colunas_comparacao])

    new_and_changed_df = (
        df_source.alias("s")
        .join(df_target_current.alias("t"), join_condition, "left_outer")
        .where(col(f"t.{target_key_name}").isNull() | expr(compare_condition))
        .select("s.*")
    )

    if new_and_changed_df.rdd.isEmpty():
        logger.info(f"Nenhuma alteração ou novo registro encontrado para a dimensao [{nome_dim}].")
        return

    logger.info(f"Encontrados {new_and_changed_df.count()} registros novos ou alterados.")

    # 2. Expira os registros antigos que foram atualizados
    keys_to_expire_df = new_and_changed_df.select(col(chave_negocio).alias("merge_key"))
    
    (delta_target.alias("t")
        .merge(keys_to_expire_df.alias("s"), col(f"t.{target_key_name}") == col("s.merge_key"))
        .whenMatchedUpdate(condition="t.is_current = true", set={"is_current": lit(False), "end_date": current_timestamp()})
        .execute()
    )
    logger.info("Versões antigas dos registros foram expiradas.")

    # 3. Insere as novas versões e os registros completamente novos
    (new_and_changed_df
        .withColumn("is_current", lit(True))
        .withColumn("start_date", current_timestamp())
        .withColumn("end_date", lit(None).cast("timestamp"))
        .withColumnRenamed(chave_negocio, target_key_name)
        .write.format("delta").mode("append").save(path_gold)
    )
    logger.info(f"Novos registros e atualizações inseridos com sucesso na dimensao [{nome_dim}].")


def ler_checkpoint(spark: SparkSession, path: str) -> datetime:
    try:
        df = spark.read.format("delta").load(path)
        return df.select("last_processed_date").first()[0]
    except Exception:
        logger.warning(f"Checkpoint não encontrado em {path}. Usando data mínima.")
        return datetime(1900, 1, 1)

def gravar_checkpoint(spark: SparkSession, path: str, value: datetime):
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
               .mode("append")
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
        processar_dimensao_scd2(spark, base_path, "dim_cliente", "usuarios", "id", ["nome", "email"])
        processar_dimensao_scd2(spark, base_path, "dim_produto", "produtos", "id", ["nome", "descricao", "preco"])
        processar_fato_vendas(spark, base_path)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
