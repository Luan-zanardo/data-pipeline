"""Landing to Bronze.

Lê os dados em formato CSV bruto da camada Landing e grava na camada Bronze
em formato Delta Lake, adicionando colunas de metadados de auditoria e
garantindo particionamento por data de ingestão.
"""

import argparse
import glob
import logging
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("landing_to_bronze")


def get_spark_session() -> SparkSession:
    """Cria ou recupera a SparkSession configurada com suporte ao Delta Lake."""
    logger.info("Inicializando SparkSession com suporte a Delta Lake...")
    return SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.sources.default", "delta") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .getOrCreate()


def processar_tabela_landing(spark: SparkSession, tabela: str, data_especifica: str | None, base_path: Path) -> None:
    """Lê a tabela da camada Landing e grava na Bronze em formato Delta."""
    path_landing = base_path / "landing" / tabela
    path_bronze = base_path / "bronze" / tabela

    if not path_landing.exists():
        logger.warning(f"Diretório da tabela {tabela} não encontrado na Landing: {path_landing}")
        return

    # Construir caminhos de leitura e basePath para que o Spark infira o particionamento ingestion_date=...
    if data_especifica:
        path_busca = path_landing / f"ingestion_date={data_especifica}" / "*.csv"
        arquivos = glob.glob(str(path_busca))
        if not arquivos:
            logger.info(f"Nenhum arquivo CSV encontrado para a tabela {tabela} na data {data_especifica}.")
            return
        caminho_leitura = str(path_busca)
    else:
        path_busca = path_landing / "ingestion_date=*" / "*.csv"
        arquivos = glob.glob(str(path_busca))
        if not arquivos:
            logger.info(f"Nenhum arquivo CSV encontrado para a tabela {tabela} na camada Landing.")
            return
        caminho_leitura = str(path_busca)

    logger.info(f"Processando tabela [{tabela}] de {caminho_leitura} para {path_bronze}")

    # Ler dados em CSV
    df_landing = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("basePath", str(path_landing)) \
        .csv(caminho_leitura)

    # Adicionar colunas de auditoria
    df_bronze = df_landing \
        .withColumn("_input_file_name", input_file_name()) \
        .withColumn("_loaded_at", current_timestamp())

    total_registros = df_bronze.count()

    # Gravar na Bronze no formato Delta
    # Particionado por ingestion_date, no modo append para manter histórico
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .save(str(path_bronze))

    logger.info(f"Tabela [{tabela}] gravada na Bronze com sucesso. Total de registros processados: {total_registros}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Landing to Bronze")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data específica de processamento no formato YYYY-MM-DD. Se omitida, processa todas as datas disponíveis."
    )
    args = parser.parse_args()

    # Caminho do Data Lake
    base_datalake = Path(os.environ.get("DATALAKE_PATH", "datalake"))

    spark = get_spark_session()

    try:
        # Descobrir tabelas dinamicamente na Landing
        path_landing_base = base_datalake / "landing"
        if not path_landing_base.exists():
            logger.error(f"Diretório base da Landing não existe: {path_landing_base}")
            return

        tabelas = [
            d.name for d in path_landing_base.iterdir()
            if d.is_dir() and not d.name.startswith("_")
        ]

        if not tabelas:
            logger.info("Nenhuma tabela encontrada na camada Landing para processamento.")
            return

        logger.info(f"Tabelas identificadas na Landing: {tabelas}")

        for tabela in tabelas:
            try:
                processar_tabela_landing(spark, tabela, args.date, base_datalake)
            except Exception as e:
                logger.error(f"Erro ao processar tabela {tabela}: {str(e)}", exc_info=True)

    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
