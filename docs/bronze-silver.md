# Transformação Spark: Bronze e Silver (Etapa 4)

As transformações entre camadas são feitas **obrigatoriamente com Apache Spark
(PySpark)** e gravadas em **Delta Lake**. Os scripts ficam em `src/spark/` e
descobrem as tabelas dinamicamente — não há lista hardcoded.

Ambos aceitam o argumento opcional `--date YYYY-MM-DD` para processar apenas uma
data de ingestão e leem a raiz do Data Lake da variável `DATALAKE_PATH`
(padrão: `datalake`).

## Landing → Bronze

Script:
[`src/spark/landing_to_bronze.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/src/spark/landing_to_bronze.py)

A Bronze é a **cópia fiel padronizada** da Landing: nenhum dado é descartado,
apenas se adiciona rastreabilidade.

- Lê os CSVs da Landing (`header`, `inferSchema`), respeitando o
  particionamento `ingestion_date=...`.
- Adiciona colunas de **auditoria**:
    - `_input_file_name` — arquivo de origem do registro.
    - `_loaded_at` — timestamp do carregamento.
- Grava em **Delta Lake**, particionado por `ingestion_date`, em modo `append`
  (mantém o histórico de todas as ingestões).

```bash
python src/spark/landing_to_bronze.py            # todas as datas
python src/spark/landing_to_bronze.py --date 2026-06-23
```

## Bronze → Silver

Script:
[`src/spark/bronze_to_silver.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/src/spark/bronze_to_silver.py)

A Silver contém os dados **limpos, tipados e deduplicados**, prontos para
modelagem.

1. **Deduplicação** — para cada `id`, mantém o registro mais recente
   (`row_number()` ordenado por `_loaded_at` decrescente).
2. **Tipagem e padronização por tabela** — cast de tipos (inteiros, decimais,
   timestamps), `trim` de textos, `lower` em e-mails, `upper` em status/UF.
3. **Regras de qualidade (filtros)** — descarta registros inválidos, por
   exemplo:
    - `usuarios`: `id` não nulo e `nome` não vazio;
    - `produtos`: `preco >= 0`;
    - `pedido_itens` / `carrinho`: `quantidade > 0`;
    - `pagamentos`: `quantia >= 0`;
    - `avaliacoes`: nota entre 1 e 5.
4. **Carga via MERGE (upsert)** — usa `DeltaTable.merge` por `id`
   (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`), mantendo a Silver
   sempre com o estado mais atual. Adiciona o metadado `_updated_at`.

```bash
python src/spark/bronze_to_silver.py             # todas as datas
python src/spark/bronze_to_silver.py --date 2026-06-23
```

São esses filtros que **eliminam os dados sujos** injetados de propósito pelo
gerador de massa, comprovando o tratamento de qualidade.

## Delta Lake

As Bronze e Silver usam o formato **Delta Lake**, que traz transações ACID,
suporte nativo a `MERGE`/upsert e *time travel*. A SparkSession é configurada em
cada script com as extensões do Delta:

```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
```
