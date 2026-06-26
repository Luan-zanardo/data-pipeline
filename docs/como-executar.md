# Como Executar

O caminho suportado pelo projeto é o fluxo orquestrado pelo Docker Compose e
pela DAG `pipeline_completo` do Airflow.

## Pré-requisitos

- Git
- Docker + Docker Compose
- Acesso ao PostgreSQL de origem (`SOURCE_DB_*`)
- Acesso ao PostgreSQL de destino (`DEST_DB_*`)

## Instalação

```bash
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
cp .env.example .env
```

Edite o `.env` e preencha as credenciais dos bancos de origem e destino. O
Compose cria os containers de Airflow, MinIO e Metabase, mas não cria os bancos
PostgreSQL de origem/destino.

## Subir o ambiente

```bash
docker compose up -d --build
```

Interfaces:

- Airflow: <http://localhost:8080>
- MinIO Console: <http://localhost:9001>
- Metabase: <http://localhost:3000>

## Rodar o pipeline

1. Acesse o Airflow com o usuário/senha do `.env` (padrão `admin`/`admin`).
2. Ative e dispare manualmente a DAG `pipeline_completo`.
3. Acompanhe as tarefas nesta ordem:
   `setup_ambiente_origem` → `descobrir_tabelas` → `extrair_para_landing`
   → `gerar_manifesto` → `landing_to_bronze` → `bronze_to_silver`
   → `silver_to_gold` → `validar_gold` → `gold_to_serving_layer`.

## O que cada etapa executa

| Etapa | Implementação |
| ----- | ------------- |
| Massa de dados | `src/setup.py`, chamado pela tarefa `setup_ambiente_origem` |
| Landing | `src/ingestion/landing.py`, chamado pelas tasks Python da DAG |
| Bronze | `src/spark/landing_to_bronze.py --date {{ ds }}` |
| Silver | `src/spark/bronze_to_silver.py --date {{ ds }}` |
| Gold | `src/spark/silver_to_gold.py --date {{ ds }}` |
| Validação | `src/spark/validar_gold.py --date {{ ds }}` |
| Postgres destino | `src/serving/gold_to_postgres.py` |

## Parar o ambiente

```bash
docker compose down
```

Para remover também os volumes do MinIO, Airflow e Metabase:

```bash
docker compose down -v
```
