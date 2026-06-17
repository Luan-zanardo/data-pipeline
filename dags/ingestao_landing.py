"""DAG: Ingestão e movimentação para a camada Landing.

Etapa 3 do projeto (Issue #5). Orquestrado pelo Apache Airflow — o
agendamento é responsabilidade do scheduler do Airflow, **sem** cron do
Linux nem Agendador de Tarefas do Windows.

Fluxo:
    descobrir_tabelas  →  extrair_para_landing (1 task por tabela)  →  gerar_manifesto

- Ingestão:     lê cada tabela do Postgres de origem.
- Movimentação: grava o CSV bruto na Landing do Data Lake (MinIO/object storage,
                configurado via ``LANDING_PATH``) particionada por data e registra
                um manifesto da execução (entrada para a Etapa 4 / Bronze).
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from ingestion.landing import (
    extrair_tabela_para_landing,
    gravar_manifesto,
    listar_tabelas,
)

DOC_MD = __doc__


@dag(
    dag_id="ingestao_landing",
    description="Ingere as tabelas da origem (Postgres) na camada Landing em CSV bruto.",
    schedule="0 6 * * *",  # diariamente às 06:00 — agendado pelo Airflow
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=2)},
    tags=["etapa-3", "landing", "ingestao"],
    doc_md=DOC_MD,
)
def ingestao_landing():
    @task
    def descobrir_tabelas() -> list[str]:
        """Lista as tabelas de negócio existentes na origem."""
        tabelas = listar_tabelas()
        if not tabelas:
            raise ValueError("Nenhuma tabela encontrada no schema de origem.")
        print(f"Tabelas a ingerir ({len(tabelas)}): {', '.join(tabelas)}")
        return tabelas

    @task
    def extrair_para_landing(tabela: str, data_ingestao: str) -> dict:
        """Extrai uma tabela e grava o CSV bruto na Landing."""
        resumo = extrair_tabela_para_landing(tabela, data_ingestao)
        print(
            f"[{resumo['tabela']}] {resumo['linhas']} linhas "
            f"({resumo['bytes']} bytes) -> {resumo['arquivo']}"
        )
        return resumo

    @task
    def gerar_manifesto(resultados: list[dict], data_ingestao: str) -> str:
        """Consolida o resultado da ingestão num manifesto da execução.

        O manifesto serve de contrato/handoff para a camada Bronze (Etapa 4):
        descreve o que foi aterrissado, quantas linhas e onde.
        """
        destino = gravar_manifesto(resultados, data_ingestao)
        print(
            f"Manifesto gravado em {destino} "
            f"({sum(r['linhas'] for r in resultados)} linhas no total)"
        )
        return destino

    # {{ ds }} = data lógica da execução (YYYY-MM-DD), usada para particionar.
    data_ingestao = "{{ ds }}"

    tabelas = descobrir_tabelas()
    resultados = extrair_para_landing.partial(data_ingestao=data_ingestao).expand(
        tabela=tabelas
    )
    gerar_manifesto(resultados=resultados, data_ingestao=data_ingestao)


ingestao_landing()
