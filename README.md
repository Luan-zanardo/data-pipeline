# data-pipeline

Projeto de engenharia de dados: pipeline com arquitetura **medalhão**
(Landing → Bronze → Silver → Gold) sobre um Data Lake, com banco relacional
para os dados finalizados.

## Arquitetura medalhão

| Camada  | Conteúdo                                            |
| ------- | --------------------------------------------------- |
| Landing | Dados brutos na origem (CSV)                        |
| Bronze  | Dados padronizados e limpos (Parquet)               |
| Silver  | Dados tratados com regras de negócio                |
| Gold    | Dados agregados para análise                        |

## Etapas

- **Etapa 1:** Data Lake Base (object storage + banco relacional)
- **Etapa 2:** Origem dos Dados e Geração de Massa (Python + Faker)
- **Etapa 3:** Orquestração e Camada Landing
- **Etapa 4:** Transformação Spark — Bronze e Silver (issue #6)

## Transformação Spark (Etapa 4)

Scripts em **PySpark** que populam as camadas Bronze e Silver em formato
**Delta Lake**:

- `landing_to_bronze.py`: lê os CSVs da Landing e grava na Bronze (Delta),
  com rastreabilidade da origem.
- `bronze_to_silver.py`: lê a Bronze, faz limpeza/tratamento (padronização
  de colunas, duplicatas e nulos) e grava na Silver (Delta).

```bash
pip install -r requirements.txt
python src/spark/landing_to_bronze.py
python src/spark/bronze_to_silver.py
```
