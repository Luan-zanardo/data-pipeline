# Etapas do Projeto

O trabalho foi dividido em issues no GitHub, uma por responsável. Cada etapa
tem uma página detalhada nesta documentação.

## Etapa 1 — Data Lake Base (#4)
- Criar o Data Lake em object storage (**MinIO**) com a arquitetura medalhão.
- Criar o banco relacional online (PostgreSQL) para os dados finais.

## Etapa 2 — Origem dos Dados e Geração de Massa (#2)
- Criar no mínimo 10 tabelas.
- Gerar 10.000 linhas por tabela com Python (**Faker**).
- Distribuir as datas dos registros nos últimos 3 anos.
- 📄 [Geração de Massa](geracao-massa.md)

## Etapa 3 — Orquestração e Camada Landing (#5)
- Subir ferramenta de orquestração (**Apache Airflow** em Docker), sem usar
  cron/Agendador do SO.
- Agendar as atividades de ingestão e movimentação.
- Gravar os dados na Landing no formato bruto original (CSV).
- 📄 [Orquestração e Landing](orquestracao.md)

## Etapa 4 — Transformação Spark: Bronze e Silver (#6)
- Desenvolver os scripts obrigatoriamente com **Apache Spark (PySpark)**.
- Ler da Landing e gravar na Bronze em **Delta Lake**.
- Limpar e tratar, gravando na Silver em Delta Lake.
- 📄 [Bronze e Silver](bronze-silver.md)

## Etapa 5 — Modelagem, Carga Incremental e Virtualização — Gold (#9)
- Modelar a camada Gold (esquema estrela) e implementar **carga incremental**.
- Manter histórico nas dimensões (**SCD Tipo 2**).
- Virtualizar/carregar para o banco relacional via JDBC.
- 📄 [Gold](gold.md)

## Etapa 6 — Dataviz com Metabase (#10)
- Subir o **Metabase self-host** (Docker) e conectá-lo ao Postgres de destino.
- Construir os dashboards de análise sobre os dados Gold.
- 📄 [Dataviz (Metabase)](metabase.md)

## Etapa 7 — Documentação, Apresentação e Entrega (#11)
- Documentação completa no MkDocs e README caprichado.
- Resumo em PowerPoint (20 min) e demonstração prática.
- Entrega no Portal AVA com URLs do GitHub e do MkDocs.
- 📄 [Entrega](entrega.md)
