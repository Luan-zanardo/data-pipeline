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
- **Etapa 4:** Camada Bronze — Limpeza e Padronização (issue #6)

## Camada Bronze (Etapa 4)

Converte os CSVs da Landing em Parquet, padronizando colunas e tratando
duplicatas/nulos, preservando a origem de cada registro.

```bash
pip install -r requirements.txt
python src/bronze/landing_to_bronze.py
```
