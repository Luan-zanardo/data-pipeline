"""DAG: Pipeline de Dados Completo (Data Lake)

Orquestra o pipeline de dados de ponta a ponta, da origem à camada de servir,
utilizando o SparkSubmitOperator para uma integração profissional com o Spark.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from ingestion.landing import (
    extrair_tabela_para_landing,
    gravar_manifesto,
    listar_tabelas,
)
from setup import populate_source_database_if_empty

# Argumentos comuns para os operadores Spark
spark_common_args = {
    "conn_id": "spark_default",
    "packages": "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4",
    "verbose": True,
}

@dag(
    dag_id="pipeline_completo",
    description="Executa o pipeline de dados de ponta a ponta, da origem à camada de servir.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["pipeline-completo", "spark-submit", "gold", "serving"],
)
def pipeline_completo():
    # --- 0. TAREFA DE SETUP DO AMBIENTE DE ORIGEM ---
    @task(task_id="setup_ambiente_origem")
    def setup_task():
        populate_source_database_if_empty()

    # --- 1. TAREFAS DE INGESTÃO (Python) ---
    @task
    def descobrir_tabelas() -> list[str]:
        tabelas = listar_tabelas()
        if not tabelas:
            raise ValueError("Nenhuma tabela encontrada no schema de origem.")
        return tabelas

    @task
    def extrair_para_landing(tabela: str, data_ingestao: str) -> dict:
        return extrair_tabela_para_landing(tabela, data_ingestao)

    @task
    def gerar_manifesto(resultados: list[dict], data_ingestao: str) -> str:
        return gravar_manifesto(resultados, data_ingestao)

    # --- 2. TAREFAS DE TRANSFORMAÇÃO (Spark via SparkSubmitOperator) ---
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application="/opt/airflow/src/spark/landing_to_bronze.py",
        application_args=["--date", "{{ ds }}"],
        **spark_common_args
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/airflow/src/spark/bronze_to_silver.py",
        application_args=["--date", "{{ ds }}"],
        **spark_common_args
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/airflow/src/spark/silver_to_gold.py",
        application_args=["--date", "{{ ds }}"],
        **spark_common_args
    )

    validar_gold = SparkSubmitOperator(
        task_id="validar_gold",
        application="/opt/airflow/src/spark/validar_gold.py",
        application_args=["--date", "{{ ds }}"],
        **spark_common_args
    )

    # --- 3. TAREFA DE CARGA PARA A CAMADA DE SERVIR ---
    gold_to_serving_layer = SparkSubmitOperator(
        task_id="gold_to_serving_layer",
        application="/opt/airflow/src/serving/gold_to_postgres.py",
        **spark_common_args
    )

    # --- 4. DEFINIÇÃO DO FLUXO ---
    data_ingestao = "{{ ds }}"
    
    ambiente_pronto = setup_task()
    tabelas_descobertas = descobrir_tabelas()
    
    ambiente_pronto >> tabelas_descobertas

    resultados_extracao = extrair_para_landing.partial(data_ingestao=data_ingestao).expand(
        tabela=tabelas_descobertas
    )
    manifesto_gerado = gerar_manifesto(resultados=resultados_extracao, data_ingestao=data_ingestao)

    (
        manifesto_gerado 
        >> landing_to_bronze 
        >> bronze_to_silver 
        >> silver_to_gold 
        >> validar_gold
        >> gold_to_serving_layer
    )

pipeline_completo()
