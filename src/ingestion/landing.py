"""Ingestão da camada Landing.

Lê as tabelas do banco relacional de ORIGEM e as grava na camada Landing
no **formato bruto original** (CSV), exatamente como vieram da origem.

A extração usa o comando nativo ``COPY ... TO STDOUT WITH CSV HEADER`` do
PostgreSQL, garantindo que os dados sejam serializados pelo próprio banco —
sem nenhuma transformação/coerção de tipo no caminho (fidelidade ao "bruto").

As funções aqui são puro Python (sem dependência do Airflow), o que facilita
testar a ingestão isoladamente e reaproveitá-la em outros contextos.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2

# Schema da origem onde estão as tabelas de negócio.
SOURCE_SCHEMA = "public"


def get_source_dsn() -> dict:
    """Monta os parâmetros de conexão com o banco de origem a partir do ambiente."""
    return {
        "host": os.environ["SOURCE_DB_HOST"],
        "port": int(os.environ.get("SOURCE_DB_PORT", "5432")),
        "dbname": os.environ.get("SOURCE_DB_NAME", "postgres"),
        "user": os.environ["SOURCE_DB_USER"],
        "password": os.environ["SOURCE_DB_PASSWORD"],
        "sslmode": os.environ.get("SOURCE_DB_SSLMODE", "require"),
    }


def get_source_connection():
    """Abre uma conexão com o banco de origem."""
    return psycopg2.connect(connect_timeout=30, **get_source_dsn())


def listar_tabelas() -> list[str]:
    """Descobre dinamicamente as tabelas de negócio no schema de origem.

    Evita hardcode: se a Etapa 2 adicionar/remover tabelas, a ingestão se
    adapta automaticamente.
    """
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    with get_source_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, (SOURCE_SCHEMA,))
        return [row[0] for row in cur.fetchall()]


def caminho_landing(tabela: str, data_ingestao: str) -> Path:
    """Caminho particionado por data de ingestão na camada Landing.

    Estrutura: ``landing/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv``
    O particionamento por data permite reprocessar/auditar cada execução e é o
    layout que a Etapa 4 (Spark) lê para gerar a Bronze.
    """
    base = Path(os.environ.get("LANDING_PATH", "datalake/landing"))
    destino = base / tabela / f"ingestion_date={data_ingestao}" / f"{tabela}.csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    return destino


def extrair_tabela_para_landing(tabela: str, data_ingestao: str) -> dict:
    """Extrai uma tabela da origem e grava o CSV bruto na Landing.

    Retorna um pequeno resumo (tabela, linhas, caminho) usado no manifesto.
    """
    destino = caminho_landing(tabela, data_ingestao)

    # Identificador citado para suportar nomes com maiúsculas/palavras reservadas.
    copy_sql = (
        f'COPY (SELECT * FROM "{SOURCE_SCHEMA}"."{tabela}") '
        f"TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
    )

    with get_source_connection() as conn, conn.cursor() as cur:
        with open(destino, "w", encoding="utf-8", newline="") as fh:
            cur.copy_expert(copy_sql, fh)

    # Conta as linhas gravadas (descontando o cabeçalho) para o manifesto.
    with open(destino, "r", encoding="utf-8") as fh:
        linhas = sum(1 for _ in fh) - 1

    return {
        "tabela": tabela,
        "linhas": max(linhas, 0),
        "arquivo": str(destino),
        "bytes": destino.stat().st_size,
    }
