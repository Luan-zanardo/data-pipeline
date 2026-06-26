# Etapas do Projeto

Esta página resume as partes implementadas no repositório e aponta para as
páginas detalhadas da documentação.

## Etapa 1 — Data Lake Base
- Criar o Data Lake em object storage (**MinIO**) com a arquitetura medalhão.
- Criar o banco relacional online (PostgreSQL) para os dados finais.

## Etapa 2 — Origem dos Dados e Geração de Massa
- Criar no mínimo 10 tabelas.
- Gerar 10.000 linhas por tabela com Python (**Faker**).
- Distribuir as datas dos registros nos últimos 3 anos.
- 📄 [Geração de Massa](geracao-massa.md)

## Etapa 3 — Orquestração e Camada Landing
- Subir ferramenta de orquestração (**Apache Airflow** em Docker), sem usar
  cron/Agendador do SO.
- Agendar as atividades de ingestão e movimentação.
- Gravar os dados na Landing no formato bruto original (CSV).
- 📄 [Orquestração e Landing](orquestracao.md)

## Etapa 4 — Transformação Spark: Bronze e Silver
- Desenvolver os scripts obrigatoriamente com **Apache Spark (PySpark)**.
- Ler da Landing e gravar na Bronze em **Delta Lake**.
- Limpar e tratar, gravando na Silver em Delta Lake.
- 📄 [Bronze e Silver](bronze-silver.md)

## Etapa 5 — Modelagem, Carga Incremental e Serving — Gold
- Modelar a camada Gold (esquema estrela) e implementar **carga incremental**.
- Manter histórico nas dimensões (**SCD Tipo 2**).
- Virtualizar/carregar para o banco relacional via JDBC.
- 📄 [Gold](gold.md)

## Etapa 6 — Dataviz com Metabase
- Subir o **Metabase self-host** (Docker) e conectá-lo ao Postgres de destino.
- Construir os dashboards de análise sobre os dados Gold.
- 📄 [Dataviz (Metabase)](metabase.md)

## Documentação
- Documentação publicada com MkDocs.
- README com visão geral e execução do projeto.
