"""DAG: Transformação das camadas Bronze, Silver e Gold e carga no Postgres.

Orquestra a execução dos scripts Spark que transformam os dados entre as
camadas do Data Lake e, ao final, carrega a camada Gold no banco de dados
PostgreSQL de destino.

Este DAG deve ser executado APÓS o DAG `ingestao_landing` ter concluído.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

# O BashOperator executa os scripts usando o Python do ambiente do Airflow,
# que já tem as dependências do Spark instaladas via docker-compose.
PYTHON_CMD = "python"

@dag(
    dag_id="bronze_silver_gold",
    description="Transforma os dados (Bronze, Silver, Gold) e carrega no Postgres.",
    schedule=None,  # Apenas execução manual
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["etapa-4", "etapa-5", "spark", "bronze", "silver", "gold", "postgres"],
)
def bronze_silver_gold():
    # Sensor para garantir que a ingestão na Landing foi concluída com sucesso
    # no mesmo dia lógico de execução.
    wait_for_landing = ExternalTaskSensor(
        task_id="wait_for_landing",
        external_dag_id="ingestao_landing",
        external_task_id=None,  # Espera o DAG inteiro
        allowed_states=["success"],
        failed_states=["failed"],
        mode="poke",
        poke_interval=30,
        timeout=1800,  # 30 minutos
    )

    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/landing_to_bronze.py",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/silver_to_gold.py",
    )

    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=f"{PYTHON_CMD} /opt/airflow/src/spark/gold_to_postgres.py",
    )

    # Define a ordem de execução das tarefas
    wait_for_landing >> landing_to_bronze >> bronze_to_silver >> silver_to_gold >> gold_to_postgres

bronze_silver_gold()
