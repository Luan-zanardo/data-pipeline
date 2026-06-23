# Gold: Modelagem, Carga Incremental e Virtualização (Etapa 5)

A camada Gold transforma a Silver num **modelo dimensional (esquema estrela)**
pronto para análise, com **carga incremental** e **histórico (SCD2)**, e depois
**virtualiza** os dados em um Postgres de destino para o Looker Studio.

Os scripts ficam em `src/spark/` e usam Delta Lake. A raiz do Data Lake vem de
`DATALAKE_PATH` (padrão: `datalake`).

## Silver → Gold

Script:
[`src/spark/silver_to_gold.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/src/spark/silver_to_gold.py)

Constrói, nesta ordem, as tabelas do
[modelo dimensional](modelo-dados.md#modelo-dimensional-gold):

- **`dim_data`** — dimensão de calendário gerada entre a menor e a maior
  `data_pedido` da Silver. Sobrescrita a cada execução (`overwrite`).
- **`dim_cliente`** e **`dim_produto`** — dimensões **SCD Tipo 2**
  (histórico). A cada execução:
    1. calcula um `hash_scd` (SHA-256) dos atributos versionados;
    2. fecha as versões vigentes que mudaram (`is_current = false`,
       preenche `valido_ate`);
    3. insere as versões novas/alteradas com `is_current = true` e nova
       *surrogate key*.
- **`fato_vendas`** — fato no grão de **item de pedido**, com **carga
  incremental por checkpoint** (ver abaixo). Liga-se às dimensões pelas
  *surrogate keys* vigentes (`is_current = true`).

```bash
python src/spark/silver_to_gold.py
```

### Carga incremental (checkpoint)

A `fato_vendas` não reprocessa tudo a cada execução. O script mantém um
checkpoint Delta em `gold/_checkpoints/fato_vendas` com o maior `data_pedido`
já processado (`last_value`). Em execuções seguintes, apenas pedidos com
`data_pedido > last_value` entram, e um `left_anti` por `id_pedido_item` garante
que nada seja inserido em duplicidade. Isso torna a carga **idempotente**.

## Gold → Postgres (virtualização)

Script:
[`src/spark/gold_to_postgres.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/src/spark/gold_to_postgres.py)

Lê as tabelas Delta da Gold e as grava em um **Postgres de destino via JDBC**,
disponibilizando o modelo dimensional para o Looker Studio. Para as dimensões
SCD2 (`dim_cliente`, `dim_produto`), envia apenas os registros vigentes
(`is_current = true`).

Requer as variáveis `DEST_DB_*` configuradas no `.env`:

```bash
python src/spark/gold_to_postgres.py
```

O driver JDBC do Postgres é baixado automaticamente via
`spark.jars.packages` (`org.postgresql:postgresql:42.7.4`).

## Validação da Gold

Script:
[`src/spark/validar_gold.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/src/spark/validar_gold.py)

Imprime métricas de controle e valida as regras da etapa: existência das
tabelas, **unicidade de registros vigentes** nas dimensões SCD2 (não pode haver
duas versões `is_current` para a mesma chave natural) e existência do checkpoint
da fato. Falha com código de saída ≠ 0 se algo estiver errado.

Para comprovar **idempotência**, salve um snapshot, rode o `silver_to_gold`
novamente e compare:

```bash
# 1. snapshot dos counts atuais
python src/spark/validar_gold.py --save-snapshot snapshot.json

# 2. reprocessa a Gold (não deve duplicar nada)
python src/spark/silver_to_gold.py

# 3. compara — os counts devem bater
python src/spark/validar_gold.py --compare-snapshot snapshot.json
```
