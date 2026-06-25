# Modelo Dimensional (Camada Gold)

A camada Gold é a camada final do nosso Data Lake, onde os dados são modelados e otimizados para análise de negócios. Utilizamos um modelo dimensional no estilo Star Schema, composto por tabelas de Fato e Dimensão.

Este modelo permite que ferramentas de BI realizem consultas de forma rápida e intuitiva.

## Esquema das Tabelas

### Dimensões

As tabelas de dimensão descrevem as entidades de negócio (o "quem", "o quê", "onde", "quando" dos eventos). Elas são carregadas utilizando a técnica **SCD (Slowly Changing Dimension) Tipo 2**, o que nos permite manter um histórico completo das alterações.

#### `dim_cliente`
- **Descrição:** Armazena informações sobre os usuários/clientes.
- **Colunas de Controle (SCD-2):**
    - `is_current` (boolean): `true` se for a versão mais recente do registro.
    - `start_date` (timestamp): Data em que o registro se tornou válido.
    - `end_date` (timestamp): Data em que o registro foi expirado (se aplicável).
- **Esquema:**
    - `id` (int): Chave primária da origem.
    - `nome` (string)
    - `email` (string)
    - `...` (demais colunas da tabela `usuarios`)

#### `dim_produto`
- **Descrição:** Armazena informações sobre os produtos.
- **Colunas de Controle (SCD-2):** Idênticas à `dim_cliente`.
- **Esquema:**
    - `id` (int): Chave primária da origem.
    - `nome` (string)
    - `descricao` (string)
    - `preco` (decimal)
    - `...` (demais colunas da tabela `produtos`)

#### `dim_data`
- **Descrição:** Uma dimensão de data clássica, derivada da data dos pedidos. Permite análises temporais complexas (ex: por dia da semana, mês, trimestre).
- **Esquema:**
    - `data` (date): A data completa.
    - `sk_data` (int): Chave substituta no formato `YYYYMMDD`.
    - `ano` (int)
    - `mes` (int)
    - `dia` (int)
    - `...` (outros atributos de data podem ser adicionados)

### Tabela Fato

A tabela de fatos contém as métricas e os eventos de negócio que queremos medir. Ela é conectada às dimensões através de chaves estrangeiras.

#### `fato_vendas`
- **Descrição:** Registra os eventos de venda, contendo métricas como quantidade e valor. É uma tabela transacional, onde cada linha representa um item de um pedido.
- **Carga:** A carga é incremental, baseada em um sistema de **checkpoint** que controla a última data de pedido processada.
- **Esquema:**
    - `pedido_id` (int): Chave estrangeira para `dim_pedido` (se existisse) ou identificador do pedido.
    - `produto_id` (int): Chave estrangeira para `dim_produto`.
    - `cliente_id` (int): Chave estrangeira para `dim_cliente`.
    - `data_id` (int): Chave estrangeira para `dim_data` (sk_data).
    - `quantidade` (int): A quantidade de itens vendidos.
    - `preco_unitario` (decimal): O preço do item no momento da venda.
    - `valor_total` (decimal): `quantidade * preco_unitario`.
    - `...` (outras métricas e chaves)
