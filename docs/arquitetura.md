# Arquitetura

O projeto adota a **arquitetura medalhão**, que organiza o Data Lake em camadas
sucessivas de refinamento. Cada camada agrega qualidade e valor ao dado.

## Camadas

| Camada  | Formato      | Conteúdo                                                |
| ------- | ------------ | ------------------------------------------------------- |
| Landing | CSV bruto    | Dados exatamente como vieram da origem                  |
| Bronze  | Delta Lake   | Dados padronizados, com rastreabilidade da origem       |
| Silver  | Delta Lake   | Dados limpos e tratados (regras de negócio)             |
| Gold    | Delta / SQL  | Dados modelados e agregados, prontos para análise       |

## Componentes

- **Object Storage**: armazena o Data Lake (Landing, Bronze, Silver, Gold).
- **Apache Spark (PySpark)**: motor de transformação entre as camadas.
- **Delta Lake**: formato transacional usado nas camadas Bronze e Silver.
- **Orquestrador** (Docker/Cloud): agenda as ingestões e movimentações.
- **Banco Relacional** (PostgreSQL — Supabase/Neon/Render): recebe a camada Gold.
- **Looker Studio**: camada de visualização sobre os dados Gold.

## Jornada do dado

1. Os dados são gerados (Faker) e disponibilizados na origem.
2. A orquestração ingere os dados brutos na **Landing**.
3. PySpark lê a Landing e grava a **Bronze** (Delta Lake).
4. PySpark limpa e trata os dados, gravando a **Silver** (Delta Lake).
5. Os dados são modelados e agregados na **Gold**, com carga incremental.
6. A Gold alimenta o banco relacional e os dashboards no Looker Studio.
