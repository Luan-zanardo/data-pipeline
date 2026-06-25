# Data Pipeline

[![Documentação](https://img.shields.io/badge/docs-online-blue?logo=materialformkdocs&logoColor=white)](https://luan-zanardo.github.io/data-pipeline/)
[![MkDocs Material](https://img.shields.io/badge/MkDocs-Material-526CFE?logo=materialformkdocs&logoColor=white)](https://squidfunk.github.io/mkdocs-material/)

📖 **Documentação online:** <https://luan-zanardo.github.io/data-pipeline/>

Pipeline de **engenharia de dados** construído sobre um Data Lake com
**arquitetura medalhão** (Landing → Bronze → Silver → Gold), incluindo geração
de massa de dados, orquestração com Airflow, transformação com Apache Spark +
Delta Lake e disponibilização dos dados em um banco relacional para análise.

## Arquitetura

```
Postgres origem → Landing → Bronze → Silver → Gold → Postgres destino → Metabase
   (Airflow)       (MinIO)   (Spark)  (Spark)  (Spark)     (JDBC)      (self-host)
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
- **Metabase** (Docker, self-host) — visualização e dashboards sobre a Gold
- **MkDocs (Material)** — documentação

## Estrutura do repositório

```
data-pipeline/
├── generate_mock_landing.py     # geração de massa (Faker) direto na Landing
├── docker-compose.yml           # Airflow + MinIO
├── .env.example                 # variáveis de ambiente (copiar para .env)
├── dags/
│   └── pipeline_completo.py     # DAG principal: ingestão + Spark + Gold + Postgres
├── src/
│   ├── ingestion/landing.py     # extração Postgres -> CSV bruto (reutilizável)
│   └── spark/
│       ├── utils.py             # SparkSession centralizada (Delta Lake + S3A/MinIO)
│       ├── landing_to_bronze.py # Landing -> Bronze (Delta, particionado por data)
│       ├── bronze_to_silver.py  # Bronze -> Silver (tipagem, limpeza, MERGE/upsert)
│       ├── silver_to_gold.py    # Silver -> Gold (estrela, SCD2, carga incremental)
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
docker compose up -d             # Airflow: http://localhost:8080 · MinIO: http://localhost:9001 · Metabase: http://localhost:3000
```

O passo a passo completo está em [`docs/como-executar.md`](docs/como-executar.md).

## Dataviz com Metabase (self-host)

A camada de visualização usa o **Metabase** rodando em Docker. Ele guarda a
própria configuração no Postgres dedicado `metabase-db` e consome o **banco de
destino** (Gold, `DEST_DB_*`) como fonte de dados.

```bash
# subir só o Metabase (e o banco dele)
docker compose up -d metabase

# ou subir o stack inteiro (Airflow + MinIO + Metabase)
docker compose up -d
```

1. Acesse **<http://localhost:3000>** (o primeiro acesso pede a criação do
   usuário admin).
2. **Admin settings → Databases → Add database → PostgreSQL** e preencha com os
   valores de `DEST_DB_*` do seu `.env` (host, porta, database, usuário, senha;
   SSL habilitado).
3. Com a fonte conectada, monte os dashboards sobre as tabelas Gold
   (`fato_vendas`, `dim_cliente`, `dim_produto`, `dim_data`).

> O banco de destino precisa estar populado — rode antes o
> `src/spark/gold_to_postgres.py` (ver [Como Executar](docs/como-executar.md)).

Comandos úteis:

```bash
docker compose logs -f metabase    # acompanhar logs
docker compose stop metabase       # parar (mantém os dados no volume)
docker compose down                # derruba o stack (volumes preservados)
```

O passo a passo detalhado está em [`docs/metabase.md`](docs/metabase.md).

## Documentação

A documentação completa é publicada com **MkDocs** e está disponível online em
**<https://luan-zanardo.github.io/data-pipeline/>**.

Para rodar localmente:

```bash
pip install mkdocs-material
mkdocs serve     # http://127.0.0.1:8000
```

Para publicar/atualizar o site no GitHub Pages:

```bash
mkdocs gh-deploy
```

## Etapas e responsáveis

| Etapa | Descrição | Issue |
| ----- | --------- | ----- |
| 1 | Data Lake Base | #4 |
| 2 | Origem dos Dados e Geração de Massa | #2 |
| 3 | Orquestração e Camada Landing | #5 |
| 4 | Transformação Spark (Bronze e Silver) | #6 |
| 5 | Modelagem, Carga Incremental e Virtualização (Gold) | #9 |
| 6 | Dataviz com Metabase (self-host) | #10 |
| 7 | Documentação, Apresentação e Entrega | #11 |
