"""
Validação da Camada Gold e Otimização.

Este script executa uma série de testes de qualidade de dados nas tabelas
dimensionais da camada Gold. Se qualquer um dos testes falhar, ele levanta
uma exceção para interromper o pipeline, impedindo que dados de baixa
qualidade sejam carregados na camada de servir.

Ao final, executa otimizações nas tabelas Delta para melhorar a performance
de leitura.
"""

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validar_gold")

def run_data_quality_checks(spark: SparkSession, base_path: str):
    """Executa uma suíte de testes de qualidade de dados."""
    logger.info("Iniciando validações de qualidade de dados na camada Gold...")

    dim_cliente = spark.read.format("delta").load(f"{base_path}/gold/dim_cliente").filter(col("is_current") == True)
    dim_produto = spark.read.format("delta").load(f"{base_path}/gold/dim_produto").filter(col("is_current") == True)
    fato_vendas = spark.read.format("delta").load(f"{base_path}/gold/fato_vendas")

    # Teste 1: Chaves na fato_vendas não podem ser nulas
    null_counts = fato_vendas.filter(
        col("pedido_id").isNull() | col("produto_id").isNull() | col("usuario_id").isNull()
    ).count()
    if null_counts > 0:
        raise Exception(f"[FALHA] Teste de Nulidade: Encontrados {null_counts} registros com chaves nulas na fato_vendas.")
    logger.info("[SUCESSO] Teste de Nulidade: Nenhuma chave nula encontrada na fato_vendas.")

    # Teste 2: Integridade Referencial (Vendas sem cliente ou produto)
    # Correção: Usar os nomes de chave corretos (id_cliente, id_produto)
    vendas_sem_cliente = fato_vendas.join(dim_cliente, fato_vendas.usuario_id == dim_cliente.id_cliente, "left_anti").count()
    if vendas_sem_cliente > 0:
        raise Exception(f"[FALHA] Integridade Referencial: Encontradas {vendas_sem_cliente} vendas sem cliente correspondente.")
    logger.info("[SUCESSO] Integridade Referencial: Todas as vendas possuem um cliente válido.")

    vendas_sem_produto = fato_vendas.join(dim_produto, fato_vendas.produto_id == dim_produto.id_produto, "left_anti").count()
    if vendas_sem_produto > 0:
        raise Exception(f"[FALHA] Integridade Referencial: Encontradas {vendas_sem_produto} vendas sem produto correspondente.")
    logger.info("[SUCESSO] Integridade Referencial: Todas as vendas possuem um produto válido.")

    # Teste 3: Valores de negócio válidos
    vendas_invalidas = fato_vendas.filter((col("quantidade") <= 0) | (col("preco") < 0)).count()
    if vendas_invalidas > 0:
        raise Exception(f"[FALHA] Validade de Negócio: Encontradas {vendas_invalidas} vendas com quantidade ou preço inválidos.")
    logger.info("[SUCESSO] Validade de Negócio: Todas as vendas possuem valores de quantidade e preço positivos.")

    logger.info("✅ Todas as validações de qualidade de dados foram concluídas com sucesso.")

def optimize_gold_tables(spark: SparkSession, base_path: str):
    """Otimiza as tabelas da camada Gold."""
    logger.info("Iniciando otimização das tabelas da camada Gold...")

    tabelas_gold = ["dim_cliente", "dim_produto", "dim_data", "fato_vendas"]
    for tabela in tabelas_gold:
        path = f"{base_path}/gold/{tabela}"
        logger.info(f"Executando OPTIMIZE na tabela {tabela}...")
        spark.sql(f"OPTIMIZE delta.`{path}`")

        if tabela == "fato_vendas":
            logger.info(f"Executando Z-ORDER na tabela {tabela} por usuario_id e produto_id...")
            spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (usuario_id, produto_id)")

    logger.info("✅ Otimização das tabelas Gold concluída.")

def main() -> None:
    parser = argparse.ArgumentParser(description="Validação e Otimização da Camada Gold")
    parser.add_argument("--date", required=False, help="Data de ingestão (não utilizada nesta tarefa).")
    args = parser.parse_args()

    spark = get_spark_session("ValidarOtimizarGold")
    base_path = f"s3a://{os.environ.get('DATALAKE_BUCKET', 'datalake')}"

    try:
        run_data_quality_checks(spark, base_path)
        optimize_gold_tables(spark, base_path)
    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()

if __name__ == "__main__":
    main()
