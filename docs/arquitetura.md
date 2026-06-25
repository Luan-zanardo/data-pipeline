# Arquitetura do Pipeline

A arquitetura do projeto foi desenhada para ser moderna, escalável e baseada em componentes open-source amplamente utilizados no mercado de engenharia de dados. Todos os serviços são containerizados com Docker, garantindo portabilidade e facilidade de configuração.

## Diagrama de Arquitetura

O diagrama abaixo ilustra o fluxo de dados e a interação entre os principais componentes do sistema:

```mermaid
graph TD
    subgraph "Ambiente de Origem"
        A[PostgreSQL - Source DB]
    end

    subgraph "Orquestração (Airflow)"
        B(DAG: pipeline_completo)
        B -- 1. Ingestão --> C{extrair_para_landing}
    end

    subgraph "Data Lake (MinIO)"
        subgraph "Landing Layer (Raw)"
            D[CSV Bruto]
        end
        subgraph "Bronze Layer (Enriched)"
            E[Delta Table]
        end
        subgraph "Silver Layer (Cleaned)"
            F[Delta Table]
        end
        subgraph "Gold Layer (Dimensional)"
            G[Fatos e Dimensões em Delta]
        end
    end
    
    subgraph "Motor de Transformação (Spark)"
        H[PySpark Scripts]
    end

    A -- Dados --> C
    C -- Grava CSV --> D
    B -- 2. Landing > Bronze --> H
    H -- Lê --> D
    H -- Grava --> E
    B -- 3. Bronze > Silver --> H
    H -- Lê --> E
    H -- Grava --> F
    B -- 4. Silver > Gold --> H
    H -- Lê --> F
    H -- Grava --> G
```

## Componentes

-   **PostgreSQL (Origem):** Simula o banco de dados transacional de um sistema de produção (OLTP), como um e-commerce. É populado com dados falsos (Faker) para simular um ambiente realista.

-   **Apache Airflow:** Atua como o cérebro do pipeline. É o orquestrador responsável por agendar, executar e monitorar as tarefas (DAGs) de forma automática e resiliente.

-   **MinIO (Data Lake):** Um serviço de object storage de alta performance, compatível com a API do Amazon S3. Ele armazena os dados em todas as camadas da arquitetura medalhão.

-   **Apache Spark (PySpark):** O motor de processamento de dados distribuído. É utilizado para executar as transformações (ETL) entre as camadas do Data Lake, desde a limpeza e enriquecimento até a modelagem dimensional.

-   **Delta Lake:** Um formato de armazenamento open-source que traz confiabilidade (transações ACID), performance e funcionalidades de gerenciamento de dados (como `merge`, `time travel`) para o Data Lake. Todas as camadas, exceto a Landing, utilizam o formato Delta.
