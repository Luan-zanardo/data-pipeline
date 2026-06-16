# Etapas do Projeto

O trabalho foi dividido em issues no GitHub, uma por responsável.

## Etapa 1 — Data Lake Base (#4)
- Criar o Data Lake em object storage com a arquitetura medalhão.
- Criar o banco relacional online gratuito (PostgreSQL) para os dados finais.

## Etapa 2 — Origem dos Dados e Geração de Massa (#2)
- Criar no mínimo 10 tabelas.
- Gerar 10.000 linhas por tabela com Python (Faker ou similar).
- Distribuir as datas dos registros nos últimos 3 anos.

## Etapa 3 — Orquestração e Camada Landing (#5)
- Subir ferramenta de orquestração (Docker ou Cloud), sem usar cron/Agendador.
- Agendar as atividades de ingestão e movimentação.
- Gravar os dados na Landing no formato bruto original.

## Etapa 4 — Transformação Spark: Bronze e Silver (#6)
- Desenvolver os scripts obrigatoriamente com Apache Spark (PySpark).
- Ler da Landing e gravar na Bronze em Delta Lake.
- Limpar e tratar, gravando na Silver em Delta Lake.

## Etapa 5 — Modelagem, Carga Incremental e Virtualização — Gold (#9)
- Modelar a camada Gold e implementar carga incremental.
- Virtualização/carga para o banco relacional.

## Etapa 6 — Dataviz com Looker Studio (#10)
- Construir os dashboards de análise sobre os dados Gold.

## Etapa 7 — Documentação, Apresentação e Entrega (#11)
- Documentação completa no MkDocs e README caprichado.
- Resumo em PowerPoint (20 min) e demonstração prática.
- Entrega no Portal AVA com URLs do GitHub e do MkDocs.
