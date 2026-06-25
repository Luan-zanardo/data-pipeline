# Bem-vindo à Documentação do Pipeline de Dados

Este site contém a documentação completa do projeto de pipeline de dados desenvolvido para a disciplina de Engenharia de Dados.

## Visão Geral

O projeto consiste na construção de um pipeline de ponta a ponta, que simula a coleta de dados de um sistema de e-commerce, sua transformação e armazenamento em um Data Lake, e a disponibilização em um modelo dimensional para análise.

Navegue pelas seções no menu lateral para explorar os detalhes da arquitetura, o fluxo do pipeline e as instruções para configuração do ambiente.

## Objetivos do Projeto

- **Orquestração Robusta:** Utilizar o Apache Airflow para orquestrar todas as etapas do pipeline de forma automática e idempotente.
- **Arquitetura Escalável:** Implementar a arquitetura medalhão (Landing, Bronze, Silver, Gold) em um Data Lake sobre object storage (MinIO).
- **Transformação Eficiente:** Usar o Apache Spark como motor de transformação para processar grandes volumes de dados.
- **Histórico de Dados:** Manter o histórico das informações nas tabelas de dimensão utilizando a técnica de Slowly Changing Dimension (SCD) Tipo 2.
- **Carga Incremental:** Otimizar o processamento da tabela de fatos através de um sistema de checkpoints.
- **Qualidade e Boas Práticas:** Seguir as melhores práticas de engenharia de software, incluindo versionamento com Git, documentação clara e containerização com Docker.
