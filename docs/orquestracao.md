# Orquestração e Camada Landing (Etapa 3)

A orquestração é feita pelo Apache Airflow em Docker. A DAG implementada no
repositório é `pipeline_completo`, definida em `dags/pipeline_completo.py`.

O agendamento externo por `cron` ou Agendador de Tarefas não é usado.

## Arquitetura desta etapa

```text
PostgreSQL origem → Airflow/Python COPY CSV → Landing no MinIO → manifesto JSON → Bronze
```

- **Orquestrador:** Apache Airflow `2.10.5` com `LocalExecutor`.
- **Fonte:** PostgreSQL de origem configurado por `SOURCE_DB_*`.
- **Destino:** MinIO, bucket `datalake`, prefixo `landing/`.
- **Extração:** `COPY ... TO STDOUT WITH CSV HEADER`, implementado em
  `src/ingestion/landing.py`.
- **Manifesto:** JSON em `landing/_manifests/ingestion_<data>.json`, usado pela
  etapa Bronze.

## DAG `pipeline_completo`

A DAG não possui agendamento automático (`schedule=None`). Ela é disparada
manualmente pela UI do Airflow.

Fluxo principal:

```text
setup_ambiente_origem
  → descobrir_tabelas
  → extrair_para_landing (dynamic task mapping, uma task por tabela)
  → gerar_manifesto
  → landing_to_bronze
  → bronze_to_silver
  → silver_to_gold
  → validar_gold
  → gold_to_serving_layer
```

## Serviços relacionados no Docker Compose

| Serviço | Papel |
| ------- | ----- |
| `minio` | Object storage S3-compatible (API 9000, console 9001) |
| `minio-init` | Cria o bucket `datalake` |
| `airflow-metadata` | Postgres interno de metadados do Airflow |
| `airflow-logs-init` | Ajusta permissões do volume de logs |
| `airflow-init` | Migra o banco de metadados e cria o usuário admin |
| `airflow-webserver` | UI do Airflow na porta 8080 |
| `airflow-scheduler` | Scheduler do Airflow |

O Compose não cria um PostgreSQL de origem. A origem vem das variáveis
`SOURCE_DB_*`.

## Como executar

```bash
cp .env.example .env
docker compose up -d --build
```

Depois:

1. Abra <http://localhost:8080>.
2. Faça login com `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.
3. Ative e dispare a DAG `pipeline_completo`.

## Resultado esperado na Landing

```text
s3://datalake/landing/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv
s3://datalake/landing/_manifests/ingestion_<YYYY-MM-DD>.json
```
