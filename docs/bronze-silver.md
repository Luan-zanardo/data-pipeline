# Transformação Spark: Bronze e Silver (Etapa 4)

As transformações ficam em `src/spark/` e usam PySpark com Delta Lake. No fluxo
orquestrado, os scripts são executados pela DAG `pipeline_completo` via
`SparkSubmitOperator`.

A raiz do Data Lake é montada como `s3a://<DATALAKE_BUCKET>`, com
`DATALAKE_BUCKET=datalake` como padrão.

## Landing → Bronze

Script: `src/spark/landing_to_bronze.py`

Entrada:

- argumento obrigatório `--date YYYY-MM-DD`;
- manifesto `s3a://datalake/landing/_manifests/ingestion_<data>.json`;
- CSVs da Landing em
  `s3a://datalake/landing/<tabela>/ingestion_date=<data>/*.csv`.

O script lê a lista de tabelas a partir do manifesto da ingestão. Para cada
tabela, ele:

- lê os CSVs com `header=true` e `inferSchema=true`;
- adiciona `_input_file_name`;
- adiciona `_loaded_at`;
- adiciona `ingestion_date`;
- grava em Delta Lake em `s3a://datalake/bronze/<tabela>`;
- particiona a escrita por `ingestion_date`;
- usa modo `append`.

## Bronze → Silver

Script: `src/spark/bronze_to_silver.py`

Entrada:

- argumento obrigatório `--date YYYY-MM-DD`;
- tabelas Bronze em `s3a://datalake/bronze/<tabela>`.

O script processa explicitamente esta lista de tabelas:

```python
[
    "produtos",
    "categorias",
    "envio",
    "usuarios",
    "avaliacoes",
    "carrinho",
    "pagamentos",
    "pedidos",
    "enderecos",
    "pedido_itens",
]
```

Transformações implementadas:

- deduplicação por `id`, mantendo o registro com `_loaded_at` mais recente;
- `usuarios.email`: `trim` + `lower`;
- `produtos.preco`: cast para `decimal(10,2)`;
- `pedidos.data_pedido`: cast para timestamp;
- inclusão de `_updated_at`.

Escrita:

- se a tabela Silver ainda não existe, grava em Delta com `overwrite`;
- se já existe, usa `DeltaTable.merge` por `id`, com
  `whenMatchedUpdateAll` e `whenNotMatchedInsertAll`.

O script atual não implementa filtros de qualidade para descartar preços
negativos, quantidades inválidas, notas fora do intervalo ou nulos.
