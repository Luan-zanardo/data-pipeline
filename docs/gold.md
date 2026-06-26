# Gold: Modelagem, Carga Incremental e Serving (Etapa 5)

A camada Gold é construída por `src/spark/silver_to_gold.py`, a partir das
tabelas Silver em Delta Lake.

A raiz usada pelos scripts é `s3a://<DATALAKE_BUCKET>`, com `datalake` como
padrão.

## Silver → Gold

Script: `src/spark/silver_to_gold.py`

No fluxo da DAG, ele é chamado com:

```bash
python src/spark/silver_to_gold.py --date YYYY-MM-DD
```

O argumento `--date` é obrigatório pela CLI, mas o script atual lê as tabelas
Silver completas e usa checkpoint próprio na fato.

Tabelas criadas:

- `dim_data`
- `dim_cliente`
- `dim_produto`
- `fato_vendas`

### `dim_data`

Criada a partir das datas distintas de `pedidos.data_pedido`.

Colunas principais:

- `data`
- `sk_data`, calculada como `yyyyMMdd`

A dimensão é sobrescrita a cada execução.

### `dim_cliente` e `dim_produto`

As dimensões implementam SCD Tipo 2 com colunas de controle:

- `is_current`
- `start_date`
- `end_date`

Na primeira carga, o script grava a dimensão a partir da tabela Silver e renomeia
a chave natural:

- `usuarios.id` → `dim_cliente.id_cliente`
- `produtos.id` → `dim_produto.id_produto`

Em cargas seguintes, compara atributos selecionados:

- `dim_cliente`: `nome`, `email`
- `dim_produto`: `nome`, `descricao`, `preco`

Quando encontra registro novo ou alterado, expira a versão vigente e insere uma
nova versão.

### `fato_vendas`

A fato é construída a partir de:

- `pedidos`
- `pedido_itens`

O script filtra `pedidos.data_pedido` maior que o checkpoint salvo em
`gold/_checkpoints/fato_vendas`. Depois faz join com `pedido_itens` e grava em
`gold/fato_vendas` com modo `append`.

O checkpoint grava a coluna `last_processed_date`.

A fato atual contém os campos vindos de `pedidos` e `pedido_itens`, além dos
metadados herdados das camadas anteriores.

## Validação da Gold

Script: `src/spark/validar_gold.py`

Validações implementadas:

- chaves nulas em `fato_vendas` (`pedido_id`, `produto_id`, `usuario_id`);
- vendas sem cliente vigente correspondente;
- vendas sem produto vigente correspondente;
- quantidade menor ou igual a zero;
- preço negativo.

Depois das validações, o script tenta executar otimizações Delta nas tabelas
Gold.

## Gold → PostgreSQL de destino

No fluxo orquestrado, a carga para o banco de destino usa:

```text
src/serving/gold_to_postgres.py
```

Esse job lê as tabelas Delta da Gold e grava no PostgreSQL definido por
`DEST_DB_*`. Para dimensões com `is_current`, envia apenas registros vigentes.
