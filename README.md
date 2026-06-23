# Data Pipeline

Pipeline de **engenharia de dados** construído sobre um Data Lake com
**arquitetura medalhão** (Landing → Bronze → Silver → Gold), incluindo geração
de massa de dados, orquestração com Airflow, transformação com Apache Spark +
Delta Lake e disponibilização dos dados em um banco relacional para análise.

## Arquitetura

```
Postgres origem → Landing → Bronze → Silver → Gold → Postgres destino → Looker Studio
   (Airflow)       (MinIO)   (Spark)  (Spark)  (Spark)     (JDBC)
```

| Camada  | Formato    | Conteúdo                                       |
| ------- | ---------- | ---------------------------------------------- |
| Landing | CSV bruto  | Dados como vieram da origem                    |
| Bronze  | Delta Lake | Dados padronizados, com auditoria de origem    |
| Silver  | Delta Lake | Dados limpos, tipados e deduplicados           |
| Gold    | Delta Lake | Modelo dimensional (fato + dimensões SCD2)     |

## Tecnologias

- **Python** + **Faker** — geração de massa de dados
- **Apache Airflow** (Docker) — orquestração e agendamento da ingestão
- **MinIO** (Docker, S3-compatible) — object storage do Data Lake
- **Apache Spark / PySpark** + **Delta Lake** — transformação entre camadas
- **PostgreSQL** (Supabase) — banco de origem e de destino da Gold
- **Looker Studio** — visualização
- **MkDocs (Material)** — documentação

## Estrutura do repositório

```
data-pipeline/
├── generate_mock_landing.py     # geração de massa (Faker) direto na Landing
├── docker-compose.yml           # Airflow + MinIO
├── .env.example                 # variáveis de ambiente (copiar para .env)
├── dags/
│   └── ingestao_landing.py      # DAG de ingestão da Landing
├── src/
│   ├── ingestion/landing.py     # extração Postgres -> CSV bruto (reutilizável)
│   └── spark/
│       ├── landing_to_bronze.py # Landing -> Bronze (Delta)
│       ├── bronze_to_silver.py  # Bronze -> Silver (limpeza + MERGE)
│       ├── silver_to_gold.py    # Silver -> Gold (estrela, SCD2, incremental)
│       ├── gold_to_postgres.py  # Gold -> Postgres destino (JDBC)
│       └── validar_gold.py      # validação/métricas da Gold
└── docs/                        # documentação MkDocs
```

## Como executar

```bash
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
pip install -r requirements.txt
cp .env.example .env             # preencha as senhas

# 1. Geração de massa na Landing (Etapa 2)
python generate_mock_landing.py

# 2. Transformação das camadas (Etapas 4 e 5)
python src/spark/landing_to_bronze.py
python src/spark/bronze_to_silver.py
python src/spark/silver_to_gold.py

# 3. Validação e (opcional) carga no Postgres de destino
python src/spark/validar_gold.py
python src/spark/gold_to_postgres.py
```

Para o fluxo orquestrado com Airflow + MinIO:

```bash
docker compose up -d             # UI: http://localhost:8080 · MinIO: http://localhost:9001
```

O passo a passo completo está em [`docs/como-executar.md`](docs/como-executar.md).

## Documentação

A documentação completa é publicada com **MkDocs**:

```bash
pip install mkdocs-material
mkdocs serve     # http://127.0.0.1:8000
```

## Etapas e responsáveis

| Etapa | Descrição | Issue |
| ----- | --------- | ----- |
| 1 | Data Lake Base | #4 |
| 2 | Origem dos Dados e Geração de Massa | #2 |
| 3 | Orquestração e Camada Landing | #5 |
| 4 | Transformação Spark (Bronze e Silver) | #6 |
| 5 | Modelagem, Carga Incremental e Virtualização (Gold) | #9 |
| 6 | Dataviz com Looker Studio | #10 |
| 7 | Documentação, Apresentação e Entrega | #11 |
