"""Validacao da camada Gold.

Le as tabelas Delta da camada Gold, imprime metricas de controle e valida
regras importantes da issue: existencia das tabelas, idempotencia por snapshot,
unicidade de registros atuais nas dimensoes SCD2 e checkpoint da fato.
"""

import argparse
import json
import logging
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count, lit, max as spark_max

# Configuracao de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validar_gold")


def get_spark_session() -> SparkSession:
    """Cria ou recupera a SparkSession configurada com suporte ao Delta Lake."""
    logger.info("Inicializando SparkSession com suporte a Delta Lake...")
    return SparkSession.builder \
        .appName("ValidarGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.sources.default", "delta") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .getOrCreate()


def tabela_delta_existe(spark: SparkSession, path_tabela: Path) -> bool:
    """Verifica se um caminho existe e contem uma tabela Delta."""
    return path_tabela.exists() and DeltaTable.isDeltaTable(spark, str(path_tabela))


def ler_tabela_gold(spark: SparkSession, base_path: Path, tabela: str):
    """Le uma tabela Delta da camada Gold."""
    path_gold = base_path / "gold" / tabela
    if not tabela_delta_existe(spark, path_gold):
        raise FileNotFoundError(f"Tabela Gold [{tabela}] nao encontrada em: {path_gold}")
    return spark.read.format("delta").load(str(path_gold))


def contar_duplicados_current(df, chave_natural: str) -> int:
    """Conta quantas chaves naturais possuem mais de um registro atual."""
    return df.filter(col("is_current") == lit(True)) \
        .groupBy(chave_natural) \
        .agg(spark_count(lit(1)).alias("qtd_current")) \
        .filter(col("qtd_current") > 1) \
        .count()


def coletar_metricas(spark: SparkSession, base_path: Path) -> tuple[dict, list[str]]:
    """Coleta metricas e erros de validacao da camada Gold."""
    tabelas = ["dim_cliente", "dim_produto", "dim_data", "fato_vendas"]
    metricas = {"counts": {}, "checks": {}, "checkpoint": None}
    erros = []

    for tabela in tabelas:
        try:
            df = ler_tabela_gold(spark, base_path, tabela)
            metricas["counts"][tabela] = df.count()
        except Exception as e:
            metricas["counts"][tabela] = None
            erros.append(str(e))

    try:
        dim_cliente = ler_tabela_gold(spark, base_path, "dim_cliente")
        duplicados_cliente = contar_duplicados_current(dim_cliente, "id_cliente")
        metricas["checks"]["dim_cliente_current_duplicado"] = duplicados_cliente
        if duplicados_cliente > 0:
            erros.append("dim_cliente possui mais de um registro current para a mesma chave natural.")
    except Exception as e:
        erros.append(f"Falha ao validar dim_cliente: {str(e)}")

    try:
        dim_produto = ler_tabela_gold(spark, base_path, "dim_produto")
        duplicados_produto = contar_duplicados_current(dim_produto, "id_produto")
        metricas["checks"]["dim_produto_current_duplicado"] = duplicados_produto
        if duplicados_produto > 0:
            erros.append("dim_produto possui mais de um registro current para a mesma chave natural.")
    except Exception as e:
        erros.append(f"Falha ao validar dim_produto: {str(e)}")

    try:
        path_checkpoint = base_path / "gold" / "_checkpoints" / "fato_vendas"
        if not tabela_delta_existe(spark, path_checkpoint):
            erros.append(f"Checkpoint da fato [fato_vendas] nao encontrado em: {path_checkpoint}")
        else:
            checkpoint = spark.read.format("delta").load(str(path_checkpoint))
            last_value = checkpoint.filter(col("tabela") == lit("fato_vendas")) \
                .agg(spark_max(col("last_value")).alias("last_value")) \
                .collect()[0]["last_value"]
            metricas["checkpoint"] = str(last_value) if last_value is not None else None
            if last_value is None:
                erros.append("Checkpoint da fato [fato_vendas] existe, mas last_value esta vazio.")
    except Exception as e:
        erros.append(f"Falha ao validar checkpoint da fato: {str(e)}")

    return metricas, erros


def salvar_snapshot(metricas: dict, path_snapshot: Path) -> None:
    """Salva as metricas atuais em um arquivo JSON para comparacao futura."""
    path_snapshot.parent.mkdir(parents=True, exist_ok=True)
    path_snapshot.write_text(json.dumps(metricas, indent=2, sort_keys=True), encoding="utf-8")
    logger.info(f"Snapshot salvo em: {path_snapshot}")


def comparar_snapshot(metricas: dict, path_snapshot: Path) -> list[str]:
    """Compara as metricas atuais com um snapshot salvo anteriormente."""
    if not path_snapshot.exists():
        return [f"Snapshot para comparacao nao encontrado: {path_snapshot}"]

    snapshot = json.loads(path_snapshot.read_text(encoding="utf-8"))
    erros = []

    for tabela, count_anterior in snapshot.get("counts", {}).items():
        count_atual = metricas.get("counts", {}).get(tabela)
        if count_atual != count_anterior:
            erros.append(
                f"Count divergente em {tabela}: anterior={count_anterior}, atual={count_atual}"
            )

    return erros


def imprimir_metricas(metricas: dict) -> None:
    """Imprime as metricas principais no log."""
    logger.info("Resumo de counts da camada Gold:")
    for tabela, total in metricas["counts"].items():
        logger.info(f"  - {tabela}: {total}")

    logger.info("Checks SCD2:")
    logger.info(
        f"  - dim_cliente current duplicado: "
        f"{metricas['checks'].get('dim_cliente_current_duplicado')}"
    )
    logger.info(
        f"  - dim_produto current duplicado: "
        f"{metricas['checks'].get('dim_produto_current_duplicado')}"
    )
    logger.info(f"Checkpoint fato_vendas last_value: {metricas['checkpoint']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validacao Spark da camada Gold")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data especifica no formato YYYY-MM-DD. Mantido para compatibilidade com os scripts Spark."
    )
    parser.add_argument(
        "--save-snapshot",
        type=str,
        default=None,
        help="Caminho de um JSON para salvar os counts atuais antes do teste de idempotencia."
    )
    parser.add_argument(
        "--compare-snapshot",
        type=str,
        default=None,
        help="Caminho de um JSON salvo anteriormente para comparar os counts atuais."
    )
    args = parser.parse_args()

    if args.date:
        logger.info(f"Parametro --date recebido ({args.date}); validacao Gold le o estado atual.")

    # Caminho do Data Lake
    base_datalake = Path(os.environ.get("DATALAKE_PATH", "datalake"))

    spark = get_spark_session()

    try:
        metricas, erros = coletar_metricas(spark, base_datalake)
        imprimir_metricas(metricas)

        if args.save_snapshot:
            salvar_snapshot(metricas, Path(args.save_snapshot))

        if args.compare_snapshot:
            erros.extend(comparar_snapshot(metricas, Path(args.compare_snapshot)))

        if erros:
            logger.error("Validacao da camada Gold encontrou problemas:")
            for erro in erros:
                logger.error(f"  - {erro}")
            raise SystemExit(1)

        logger.info("Validacao da camada Gold concluida com sucesso.")

    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
