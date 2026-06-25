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

## 3. Camada Silver (Cleaned & Conformed)

-   **Objetivo:** Aplicar regras de negócio, limpeza, normalização e enriquecimento dos dados.
-   **Processo:**
    1.  O script Spark `bronze_to_silver.py` é executado.
    2.  Ele lê as tabelas da camada Bronze.
    3.  Aplica as seguintes transformações:
        -   **Limpeza:** Remove espaços em branco, padroniza strings (ex: `UPPERCASE`).
        -   **Tratamento de Nulos:** Remove ou preenche valores nulos onde aplicável.
        -   **Deduplicação:** Remove registros duplicados.
        -   **Tipagem de Dados:** Garante que todas as colunas estejam com os tipos corretos (ex: `string` para `timestamp`, `decimal`).
    4.  Salva os dados limpos e conformados no formato **Delta Lake**.
-   **Formato:** Delta Lake

## 4. Camada Gold (Curated & Modeled)

-   **Objetivo:** Criar modelos de dados agregados e otimizados para análise, prontos para serem consumidos por ferramentas de BI.
-   **Processo:**
    1.  O script Spark `silver_to_gold.py` é acionado.
    2.  Ele lê as tabelas limpas da camada Silver.
    3.  Constrói um **modelo dimensional** com tabelas de Fato e Dimensão.
    4.  **Carga Incremental:**
        -   **Dimensões:** Utiliza a técnica **SCD (Slowly Changing Dimension) Tipo 2** para manter o histórico das alterações. O comando `merge` do Delta Lake é usado para inserir novos registros e expirar os antigos.
        -   **Fatos:** Utiliza um sistema de **checkpoint** para processar apenas os dados novos desde a última execução, tornando a carga mais eficiente.
    5.  Salva as tabelas Fato e Dimensão no formato **Delta Lake**.
-   **Formato:** Delta Lake
