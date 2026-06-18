"""Ingestão da camada Landing.

Lê as tabelas do banco relacional de ORIGEM e as grava na camada Landing do
Data Lake no **formato bruto original** (CSV), exatamente como vieram da origem.

O destino é definido por ``LANDING_PATH`` e pode ser tanto um caminho local
(``datalake/landing``) quanto um bucket de object storage S3-compatible
(``s3://datalake/landing`` — ex.: MinIO). A escrita usa ``fsspec``, então o
mesmo código atende os dois casos: em desenvolvimento grava em disco e no
Docker grava no MinIO, sem ramificar a lógica.

A extração usa o comando nativo ``COPY ... TO STDOUT WITH CSV HEADER`` do
PostgreSQL, garantindo que os dados sejam serializados pelo próprio banco —
sem nenhuma transformação/coerção de tipo no caminho (fidelidade ao "bruto").

As funções aqui são puro Python (sem dependência do Airflow), o que facilita
testar a ingestão isoladamente e reaproveitá-la em outros contextos.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

import fsspec
import psycopg2

# Schema da origem onde estão as tabelas de negócio.
SOURCE_SCHEMA = "public"

# Raiz da camada Landing quando ``LANDING_PATH`` não está definido.
DEFAULT_LANDING_PATH = "datalake/landing"


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


def _landing_root() -> str:
    """Raiz da Landing (local ou ``s3://``), sem barra final."""
    return os.environ.get("LANDING_PATH", DEFAULT_LANDING_PATH).rstrip("/")


def _storage_options(url: str) -> dict:
    """Opções de I/O do ``fsspec`` conforme o destino.

    - ``s3://``: credenciais e endpoint do object storage (MinIO/S3) vindos do
      ambiente. ``S3_ENDPOINT_URL`` aponta para o MinIO; sem ele, usa o S3 da AWS.
    - local: cria os diretórios-pai automaticamente na escrita.
    """
    if url.startswith("s3://"):
        opts: dict = {
            "key": os.environ.get("AWS_ACCESS_KEY_ID"),
            "secret": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        }
        endpoint = os.environ.get("S3_ENDPOINT_URL")
        if endpoint:
            opts["client_kwargs"] = {"endpoint_url": endpoint}
        return opts
    return {"auto_mkdir": True}


def caminho_landing(tabela: str, data_ingestao: str) -> str:
    """Caminho/URL particionado por data de ingestão na camada Landing.

    Estrutura: ``<root>/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv``,
    onde ``<root>`` é ``LANDING_PATH`` (local ou ``s3://``). O particionamento
    por data permite reprocessar/auditar cada execução e é o layout que a
    Etapa 4 (Spark) lê para gerar a Bronze.
    """
    return (
        f"{_landing_root()}/{tabela}"
        f"/ingestion_date={data_ingestao}/{tabela}.csv"
    )


def extrair_tabela_para_landing(tabela: str, data_ingestao: str) -> dict:
    """Extrai uma tabela da origem e grava o CSV bruto na Landing.

    Retorna um pequeno resumo (tabela, linhas, caminho, bytes) usado no manifesto.
    """
    destino = caminho_landing(tabela, data_ingestao)
    arquivo = fsspec.open(destino, "w", encoding="utf-8", newline="", **_storage_options(destino))

    # Identificador citado para suportar nomes com maiúsculas/palavras reservadas.
    copy_sql = (
        f'COPY (SELECT * FROM "{SOURCE_SCHEMA}"."{tabela}") '
        f"TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
    )

    # Ordem de saída do `with`: o arquivo fecha primeiro (faz o upload ao MinIO)
    # antes de contarmos as linhas abaixo.
    with get_source_connection() as conn, conn.cursor() as cur, arquivo as fh:
        cur.copy_expert(copy_sql, fh)

    fs = arquivo.fs
    # Conta as linhas gravadas (descontando o cabeçalho) para o manifesto.
    with fs.open(arquivo.path, "r", encoding="utf-8") as fh:
        linhas = sum(1 for _ in fh) - 1

    return {
        "tabela": tabela,
        "linhas": max(linhas, 0),
        "arquivo": destino,
        "bytes": fs.size(arquivo.path),
    }


def gravar_manifesto(resultados: list[dict], data_ingestao: str) -> str:
    """Consolida o resultado da ingestão num manifesto JSON da execução.

    O manifesto serve de contrato/handoff para a camada Bronze (Etapa 4):
    descreve o que foi aterrissado, quantas linhas e onde. É gravado ao lado
    dos dados, em ``<root>/_manifests/ingestion_<data>.json``.
    """
    destino = f"{_landing_root()}/_manifests/ingestion_{data_ingestao}.json"
    manifesto = {
        "data_ingestao": data_ingestao,
        "gerado_em": datetime.utcnow().isoformat() + "Z",
        "total_tabelas": len(resultados),
        "total_linhas": sum(r["linhas"] for r in resultados),
        "tabelas": sorted(resultados, key=lambda r: r["tabela"]),
    }
    with fsspec.open(destino, "w", encoding="utf-8", **_storage_options(destino)) as fh:
        json.dump(manifesto, fh, indent=2, ensure_ascii=False)
    return destino
