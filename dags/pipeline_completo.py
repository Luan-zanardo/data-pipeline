"""DAG: Pipeline de Dados Completo (Data Lake)

Orquestra o pipeline de dados de ponta a ponta dentro do Data Lake:
1.  Ingestão da origem para a camada Landing.
2.  Transformação Spark: Landing -> Bronze -> Silver -> Gold.
3.  Validação da camada Gold.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Funções de ingestão importadas do diretório src
from ingestion.landing import (
    extrair_tabela_para_landing,
    gravar_manifesto,
    listar_tabelas,
)

PYTHON_CMD = "python"

@dag(
    dag_id="pipeline_completo",
    description="Executa o pipeline de dados de ponta a ponta (até a camada Gold).",
    schedule=None,  # Apenas execução manual para apresentação
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["pipeline-completo", "ingestao", "spark", "gold"],
)
def pipeline_completo():
    # --- 1. TAREFAS DE INGESTÃO (Python) ---

    @task
    def descobrir_tabelas() -> list[str]:
        """Lista as tabelas de negócio existentes na origem."""
        tabelas = listar_tabelas()
        if not tabelas:
            raise ValueError("Nenhuma tabela encontrada no schema de origem.")
        return tabelas

    @task
    def extrair_para_landing(tabela: str, data_ingestao: str) -> dict:
        """Extrai uma tabela e grava o CSV bruto na Landing."""
        return extrair_tabela_para_landing(tabela, data_ingestao)

    @task
    def gerar_manifesto(resultados: list[dict], data_ingestao: str) -> str:
        """Consolida o resultado da ingestão num manifesto da execução."""
        return gravar_manifesto(resultados, data_ingestao)

    # --- 2. TAREFAS DE TRANSFORMAÇÃO (Spark via Bash) ---

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/landing_to_bronze.py --date {{{{ ds }}}} 2>&1",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/bronze_to_silver.py --date {{{{ ds }}}} 2>&1",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/silver_to_gold.py --date {{{{ ds }}}} 2>&1",
    )

    validar_gold = BashOperator(
        task_id="validar_gold",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/validar_gold.py --date {{{{ ds }}}} 2>&1",
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/gold_to_postgres.py 2>&1",
    )

    # --- 3. DEFINIÇÃO DO FLUXO ---

    data_ingestao = "{{ ds }}"

    tabelas_descobertas = descobrir_tabelas()
    resultados_extracao = extrair_para_landing.partial(data_ingestao=data_ingestao).expand(
        tabela=tabelas_descobertas
    )
    manifesto_gerado = gerar_manifesto(resultados=resultados_extracao, data_ingestao=data_ingestao)

    # O fluxo de transformação começa APÓS a conclusão do manifesto
    manifesto_gerado >> landing_to_bronze >> bronze_to_silver >> silver_to_gold >> validar_gold >> gold_to_postgres

pipeline_completo()
