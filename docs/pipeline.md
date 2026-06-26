# Fluxo do Pipeline de Dados

O pipeline é orquestrado por uma única DAG no Airflow (`pipeline_completo`) e é responsável por mover e transformar os dados através das quatro camadas da arquitetura medalhão.

## 1. Camada Landing (Raw Data)

-   **Objetivo:** Ingerir os dados da origem no Data Lake da forma mais bruta e rápida possível, sem aplicar transformações.
-   **Processo:**
    1.  A tarefa `extrair_para_landing` no Airflow se conecta ao banco de dados PostgreSQL de origem.
    2.  Utiliza o comando `COPY` do PostgreSQL para extrair cada tabela para um arquivo **CSV**.
    3.  Os arquivos CSV são gravados no MinIO, particionados por `ingestion_date`.
-   **Formato:** CSV

## 2. Camada Bronze (Enriched)

-   **Objetivo:** Converter os dados brutos para um formato otimizado (Delta Lake) e adicionar metadados de rastreabilidade.
-   **Processo:**
    1.  O script Spark `landing_to_bronze.py` é acionado.
    2.  Ele lê os arquivos CSV da camada Landing.
    3.  Adiciona colunas de metadados, como `_input_file_name` e `_loaded_at`, para auditoria.
    4.  Salva os dados no formato **Delta Lake**, mantendo o esquema original.
-   **Formato:** Delta Lake

## 3. Camada Silver

-   **Objetivo:** Deduplicar registros por `id` e aplicar as padronizações/casts implementados no script.
-   **Processo:**
    1.  O script Spark `bronze_to_silver.py` é executado.
    2.  Ele lê as tabelas da camada Bronze.
    3.  Aplica as seguintes transformações:
        -   **Deduplicação:** mantém o registro mais recente por `id`.
        -   **Usuários:** aplica `trim` e `lower` em `email`.
        -   **Produtos:** converte `preco` para `decimal(10,2)`.
        -   **Pedidos:** converte `data_pedido` para timestamp.
    4.  Salva ou atualiza as tabelas Silver em **Delta Lake** usando `MERGE` por `id`.
-   **Formato:** Delta Lake

## 4. Camada Gold (Curated & Modeled)

-   **Objetivo:** Criar modelos de dados agregados e otimizados para análise, prontos para serem consumidos por ferramentas de BI.
-   **Processo:**
    1.  O script Spark `silver_to_gold.py` é acionado.
    2.  Ele lê as tabelas limpas da camada Silver.
    3.  Constrói as tabelas `dim_data`, `dim_cliente`, `dim_produto` e `fato_vendas`.
    4.  **Carga Incremental:**
        -   **Dimensões:** `dim_cliente` e `dim_produto` usam colunas `is_current`, `start_date` e `end_date`.
        -   **Fatos:** `fato_vendas` usa checkpoint por `data_pedido`.
    5.  Salva as tabelas Fato e Dimensão no formato **Delta Lake**.
-   **Formato:** Delta Lake
