# Data Pipeline

Pipeline de **engenharia de dados** construído sobre um Data Lake com
**arquitetura medalhão** (Landing → Bronze → Silver → Gold), incluindo geração
de massa de dados, orquestração, transformação com Apache Spark e
disponibilização dos dados em um banco relacional para análise.

## Arquitetura

```
Origem → Landing → Bronze → Silver → Gold → Banco Relacional → Looker Studio
```

| Camada  | Formato    | Conteúdo                                       |
| ------- | ---------- | ---------------------------------------------- |
| Landing | CSV bruto  | Dados como vieram da origem                    |
| Bronze  | Delta Lake | Dados padronizados, com rastreabilidade        |
| Silver  | Delta Lake | Dados limpos e tratados                        |
| Gold    | Delta/SQL  | Dados modelados e agregados para análise       |

## Tecnologias

- **Python** + **Faker** (geração de massa de dados)
- **Apache Spark / PySpark** + **Delta Lake** (transformação)
- **PostgreSQL** (Supabase/Neon/Render) — dados finalizados
- **Docker / Cloud** — orquestração
- **Looker Studio** — visualização
- **MkDocs (Material)** — documentação

## Como executar

```bash
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
pip install -r requirements.txt

# Transformação das camadas (Etapa 4)
python src/spark/landing_to_bronze.py
python src/spark/bronze_to_silver.py
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
