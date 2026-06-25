
## Objetivos e Funcionalidades

- **Orquestração Robusta:** Utilizar o Apache Airflow para orquestrar todas as etapas do pipeline de forma automática e idempotente.
- **Arquitetura Escalável:** Implementar a arquitetura medalhão (Landing, Bronze, Silver, Gold) em um Data Lake sobre object storage (MinIO).
- **Transformação Eficiente:** Usar o Apache Spark como motor de transformação para processar grandes volumes de dados.
- **Histórico de Dados:** Manter o histórico das informações nas tabelas de dimensão utilizando a técnica de Slowly Changing Dimension (SCD) Tipo 2.
- **Carga Incremental:** Otimizar o processamento da tabela de fatos através de um sistema de checkpoints.
- **Qualidade e Boas Práticas:** Seguir as melhores práticas de engenharia de software, incluindo versionamento com Git, documentação clara e containerização com Docker.

## Navegação

Navegue pelas seções no menu lateral para explorar os detalhes do projeto:

- **[Arquitetura](arquitetura.md):** Detalhes sobre as camadas do Data Lake e o fluxo do dado.
- **[Fluxo do Pipeline](pipeline.md):** Descrição passo a passo do processamento dos dados.
- **[Modelo Dimensional](modelo_dimensional.md):** Documentação do esquema estrela da camada Gold.
- **[Setup e Execução](setup.md):** Passo a passo para configurar e rodar o pipeline localmente.

## Equipe e Responsabilidades

| Etapa | Responsável (issue) |
| ----- | ------------------- |
| Data Lake Base | #4 — minattinho |
| Origem dos Dados e Geração de Massa | #2 — AmonAmarth2003 |
| Orquestração e Camada Landing | #5 — p-afonso |
| Transformação Spark (Bronze e Silver) | #6 |
| Modelagem, Carga Incremental e Virtualização (Gold) | #9 |
| Dataviz com Metabase | #10 — Luan-zanardo |
| Documentação, Apresentação e Entrega | #11 — gabrielpagnan |