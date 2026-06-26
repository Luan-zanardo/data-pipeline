
## Objetivos e Funcionalidades

- **Orquestração:** Utilizar o Apache Airflow para executar as etapas do pipeline pela DAG `pipeline_completo`.
- **Arquitetura Escalável:** Implementar a arquitetura medalhão (Landing, Bronze, Silver, Gold) em um Data Lake sobre object storage (MinIO).
- **Transformação Eficiente:** Usar o Apache Spark como motor de transformação para processar grandes volumes de dados.
- **Histórico de Dados:** Manter histórico em `dim_cliente` e `dim_produto` com Slowly Changing Dimension (SCD) Tipo 2.
- **Carga Incremental:** Otimizar o processamento da tabela de fatos através de um sistema de checkpoints.
- **Qualidade e Boas Práticas:** Seguir as melhores práticas de engenharia de software, incluindo versionamento com Git, documentação clara e containerização com Docker.

## Navegação

Navegue pelas seções no menu lateral para explorar os detalhes do projeto:

- **[Arquitetura](arquitetura.md):** Detalhes sobre as camadas do Data Lake e o fluxo do dado.
- **[Fluxo do Pipeline](pipeline.md):** Descrição passo a passo do processamento dos dados.
- **[Modelo de Dados](modelo-dados.md):** Documentação completa das tabelas de origem e do esquema estrela da camada Gold.
- **[Setup e Execução](setup.md):** Passo a passo para configurar e rodar o pipeline localmente.
