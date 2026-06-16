# Camada Landing no MinIO — Data Lake Base (Issue #4)

**Data:** 2026-06-16
**Autor:** minattinho
**Issue:** [#4 — Data Lake Base](https://github.com/Luan-zanardo/data-pipeline/issues/4)
**Branch:** `data_lake`

## Objetivo

Provisionar o object storage do Data Lake e conectar a camada **Landing** a ele.
Escopo desta entrega: **somente a camada Landing**. As camadas Bronze, Silver e
Gold e o banco relacional da Gold ficam para as etapas/responsáveis seguintes.

## Decisões

| Decisão | Escolha | Por quê |
| ------- | ------- | ------- |
| Object storage | **MinIO** (Docker, S3-compatible) | Grátis, roda no mesmo `docker-compose` do Airflow, é o setup clássico para Spark + Delta Lake (Etapa 4). |
| Layout | Bucket único `datalake`, prefixo por zona (`landing/`, …) | Espelha a convenção `datalake/landing` já existente; o console do MinIO mostra o medalhão visualmente. |
| Escrita | `fsspec` + `s3fs` | A mesma função grava em disco local **ou** em `s3://` conforme `LANDING_PATH`, sem ramificar a lógica; preserva o `COPY ... TO STDOUT` bruto do Postgres. |

Alternativa considerada e descartada: `boto3` explícito (mais robusto contra
conflito de deps no Airflow, porém ramifica o código). Fica como plano B caso o
`s3fs` conflite na imagem do Airflow.

## Arquitetura

```
Postgres (origem)  ──COPY CSV──▶  Landing (MinIO: s3://datalake/landing)  ──manifesto──▶  Bronze (Etapa 4)
```

Layout dos objetos:

```
s3://datalake/landing/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv
s3://datalake/landing/_manifests/ingestion_<YYYY-MM-DD>.json
```

## Componentes alterados

- **`docker-compose.yml`** — serviços `minio` (API 9000 / console 9001) e
  `minio-init` (cria o bucket `datalake`); variáveis S3 e `LANDING_PATH` nos
  containers do Airflow; `s3fs` em `_PIP_ADDITIONAL_REQUIREMENTS`; volume
  `minio-data`.
- **`src/ingestion/landing.py`** — `LANDING_PATH` aceita URL `s3://` ou caminho
  local; helper `_storage_options`; I/O via `fsspec`; nova função
  `gravar_manifesto`.
- **`dags/ingestao_landing.py`** — `gerar_manifesto` delega a `gravar_manifesto`.
- **`.env.example`**, **`requirements.txt`**, **`ORQUESTRACAO_LANDING.md`** —
  variáveis, dependências e documentação.

## Configuração

| Variável | Default | Função |
| -------- | ------- | ------ |
| `LANDING_PATH` | `s3://datalake/landing` | Raiz da Landing (use caminho local para gravar em disco). |
| `S3_ENDPOINT_URL` | `http://minio:9000` | Endpoint do MinIO (vazio = AWS S3 real). |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `minioadmin` | Credenciais do MinIO (= chaves S3 da ingestão). |

## Validação

- `docker compose config` — compose válido (sem daemon).
- Teste local do `fsspec`: gravação/leitura/contagem de linhas no fallback local.
- **Pendente do usuário** (precisa do daemon Docker + senha do Supabase no `.env`):
  `docker compose up -d` → bucket `datalake` criado → trigger da DAG
  `ingestao_landing` → conferir os CSVs e o manifesto no console do MinIO
  (<http://localhost:9001>).

## Fora de escopo

Camadas Bronze/Silver/Gold, transformação Spark/Delta, banco relacional da Gold.
