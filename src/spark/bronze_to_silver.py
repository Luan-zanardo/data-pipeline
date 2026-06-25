"""Bronze to Silver.

Lê as tabelas da camada Bronze (via S3A), realiza a limpeza, tipagem e
deduplicação dos dados, e grava na camada Silver usando MERGE (upsert).
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, lower, row_number, to_timestamp, trim, upper
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("bronze_to_silver")


def aplicar_transformacoes(df, tabela):
    """Aplica tipagem, padronização de campos e limpezas específicas por tabela."""
    logger.info(f"Aplicando transformações para a tabela {tabela}...")

    window_spec = Window.partitionBy("id").orderBy(desc("_loaded_at"))
    df_dedup = df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

    # Regras de transformação (simplificado para brevidade)
    if tabela == "usuarios":
        df_clean = df_dedup.withColumn("email", lower(trim(col("email"))))
    elif tabela == "produtos":
        df_clean = df_dedup.withColumn("preco", col("preco").cast("decimal(10,2)"))
    elif tabela == "pedidos":
        df_clean = df_dedup.withColumn("data_pedido", to_timestamp(col("data_pedido")))
    else:
        df_clean = df_dedup

    return df_clean.withColumn("_updated_at", current_timestamp())


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Bronze to Silver")
    parser.add_argument("--date", required=True, type=str, help="Data de ingestão no formato YYYY-MM-DD.")
    args = parser.parse_args()

    spark = get_spark_session("BronzeToSilver")

    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"

    try:
        tabelas = ['produtos', 'categorias', 'envio', 'usuarios', 'avaliacoes', 'carrinho', 'pagamentos', 'pedidos', 'enderecos', 'pedido_itens']
        logger.info(f"Tabelas para processar: {tabelas}")

        for tabela in tabelas:
            path_bronze = f"{base_path}/bronze/{tabela}"
            path_silver = f"{base_path}/silver/{tabela}"

            logger.info(f"Processando tabela [{tabela}] da Bronze para a Silver...")

            try:
                df_bronze = spark.read.format("delta").load(path_bronze).filter(col("ingestion_date") == args.date)

                if df_bronze.rdd.isEmpty():
                    logger.warning(f"Nenhum dado encontrado para a tabela {tabela} na data {args.date}.")
                    continue

                df_silver_source = aplicar_transformacoes(df_bronze, tabela)

                if not DeltaTable.isDeltaTable(spark, path_silver):
                    df_silver_source.write.format("delta").mode("overwrite").save(path_silver)
                    logger.info(f"Tabela Silver [{tabela}] criada com sucesso.")
                else:
                    delta_table = DeltaTable.forPath(spark, path_silver)
                    (delta_table.alias("target")
                        .merge(df_silver_source.alias("source"), "target.id = source.id")
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute())
                    logger.info(f"Tabela Silver [{tabela}] atualizada com sucesso via MERGE.")

            except Exception as e:
                logger.error(f"Erro ao processar tabela {tabela}: {str(e)}", exc_info=True)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
