
## Componentes Principais

-   **PostgreSQL (Origem e Destino):**
    -   **Origem:** Simula o banco de dados transacional de um sistema de produção (OLTP), como um e-commerce. É populado com dados falsos (Faker) para simular um ambiente realista.
    -   **Destino:** Atua como a **Camada de Servir (Serving Layer)**, recebendo os dados já modelados da camada Gold para serem consumidos pelas ferramentas de BI.

-   **Apache Airflow:** Atua como o orquestrador responsável por executar e monitorar a DAG do pipeline, sem depender de agendadores do sistema operacional.

-   **MinIO (Data Lake):** Um serviço de object storage de alta performance, compatível com a API do Amazon S3. Ele armazena os dados em todas as camadas da arquitetura medalhão.

-   **Apache Spark (PySpark):** O motor de processamento de dados distribuído. É utilizado para executar as transformações (ETL) entre as camadas do Data Lake, desde a limpeza e enriquecimento até a modelagem dimensional.

-   **Delta Lake:** Um formato de armazenamento open-source que traz confiabilidade (transações ACID), performance e funcionalidades avançadas de gerenciamento de dados (como `merge`, `time travel`, `SCD-2`) para o Data Lake.

-   **Metabase:** Uma ferramenta de Business Intelligence open-source, utilizada para criar os dashboards e visualizações. Ela se conecta ao PostgreSQL de destino para consumir os dados da camada Gold.

## Jornada do Dado

1.  **Geração de Massa:** A primeira tarefa do pipeline garante que o banco de dados de origem esteja populado com dados realistas (10 tabelas, 10k+ linhas, 3 anos de histórico), usando a biblioteca Faker.

2.  **Ingestão para a Landing:** O Airflow orquestra a extração dos dados brutos do PostgreSQL e os salva como arquivos CSV na camada **Landing** do MinIO, particionados por data.

3.  **Landing → Bronze:** O Spark lê os dados brutos da Landing, adiciona metadados de auditoria e os converte para o formato **Delta Lake** na camada **Bronze**.

4.  **Bronze → Silver:** O Spark deduplica por `id`, aplica padronizações/casts pontuais e grava a camada **Silver** (Delta Lake).

5.  **Silver → Gold:** O Spark lê os dados da Silver e cria `dim_data`, `dim_cliente`, `dim_produto` e `fato_vendas`. As dimensões de cliente/produto usam lógica **SCD Tipo 2**, e a fato é carregada de forma incremental usando checkpoint.

6.  **Gold → Camada de Servir:** Os dados modelados da camada Gold são carregados em um banco de dados PostgreSQL de destino, otimizando o acesso para ferramentas de BI.

7.  **Visualização:** O **Metabase** se conecta ao PostgreSQL de destino e recebe um dashboard inicial provisionado por `metabase-init`.
