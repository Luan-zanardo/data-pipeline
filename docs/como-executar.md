# Como Executar

Há dois caminhos para rodar o pipeline:

- **Caminho A — Local (rápido, para validar a transformação).** Gera a massa com
  Faker direto na Landing local e roda os scripts Spark na sua máquina.
- **Caminho B — Orquestrado (completo).** Sobe Airflow + MinIO no Docker e
  ingere os dados do Postgres de origem.

## Pré-requisitos

- Python 3.10+
- Java 8/11 (necessário para o Apache Spark)
- Docker + Docker Compose (apenas para o Caminho B)

## Instalação

```bash
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
pip install -r requirements.txt
```

Copie e preencha as variáveis de ambiente:

```bash
cp .env.example .env
# preencha SOURCE_DB_PASSWORD (origem) e, para a Gold, as variáveis DEST_DB_*
```

---

## Caminho A — Execução local

```bash
# 1. Geração de massa na Landing (Etapa 2)
python generate_mock_landing.py

# 2. Landing -> Bronze (Etapa 4)
python src/spark/landing_to_bronze.py

# 3. Bronze -> Silver (Etapa 4)
python src/spark/bronze_to_silver.py

# 4. Silver -> Gold: modelo dimensional + carga incremental (Etapa 5)
python src/spark/silver_to_gold.py

# 5. Validação da Gold (counts, SCD2, checkpoint)
python src/spark/validar_gold.py

# 6. (Opcional) Gold -> Postgres de destino para o Looker (Etapa 5)
python src/spark/gold_to_postgres.py
```

Os scripts Spark aceitam `--date YYYY-MM-DD` para processar uma data específica
e leem a raiz do Data Lake de `DATALAKE_PATH` (padrão: `datalake`).

---

## Caminho B — Orquestrado (Airflow + MinIO)

```bash
docker compose up -d
```

- UI do Airflow: <http://localhost:8080> (login no `.env`, padrão `admin`/`admin`).
- Console do MinIO: <http://localhost:9001> (padrão `minioadmin`/`minioadmin`).

Ative e dispare a DAG **`ingestao_landing`** para ingerir o Postgres de origem
na Landing. Depois siga do passo 2 do Caminho A para transformar as camadas.

O passo a passo detalhado está em [Orquestração e Landing](orquestracao.md).

---

## Ordem das etapas

| Passo | Script / Ação | Página |
| ----- | ------------- | ------ |
| Massa | `generate_mock_landing.py` | [Geração de Massa](geracao-massa.md) |
| Landing | DAG `ingestao_landing` (Airflow) | [Orquestração](orquestracao.md) |
| Bronze | `src/spark/landing_to_bronze.py` | [Bronze e Silver](bronze-silver.md) |
| Silver | `src/spark/bronze_to_silver.py` | [Bronze e Silver](bronze-silver.md) |
| Gold | `src/spark/silver_to_gold.py` | [Gold](gold.md) |
| Validação | `src/spark/validar_gold.py` | [Gold](gold.md) |
| Postgres | `src/spark/gold_to_postgres.py` | [Gold](gold.md) |

## Visualização da documentação

```bash
pip install mkdocs-material
mkdocs serve   # http://127.0.0.1:8000
```
