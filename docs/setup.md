# Setup e Execução do Ambiente

Este guia detalha os passos necessários para configurar e executar o ambiente de desenvolvimento completo do pipeline de dados em uma máquina local.

## Pré-requisitos

Antes de começar, garanta que você tenha as seguintes ferramentas instaladas e funcionando em seu sistema:

-   **Git:** Para clonar o repositório.
-   **Docker:** Para executar os contêineres dos serviços.
-   **Docker Compose:** Para orquestrar os múltiplos contêineres definidos no projeto.

## Passo 1: Clonar o Repositório

Abra um terminal e clone o repositório do projeto para a sua máquina local.

```sh
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
```

## Passo 2: Configurar Variáveis de Ambiente

O projeto utiliza um arquivo `.env` para gerenciar credenciais e configurações sensíveis. Para criar o seu, copie o arquivo de exemplo fornecido:

```sh
cp .env.example .env
```

As configurações padrão neste arquivo já estão ajustadas para o ambiente Docker local. Para uma execução padrão, nenhuma alteração é necessária.

## Passo 3: Construir e Iniciar os Serviços

Com o Docker e o Docker Compose instalados, execute o seguinte comando na raiz do projeto:

```sh
docker-compose up -d --build
```

-   `--build`: Força a reconstrução da imagem customizada do Airflow, garantindo que quaisquer alterações no `Dockerfile` ou no `requirements.txt` sejam aplicadas.
-   `-d`: (Detached mode) Executa os contêineres em segundo plano.

Este comando irá iniciar todos os serviços necessários:
-   `airflow-webserver`
-   `airflow-scheduler`
-   `postgres-source` (Banco de dados de origem)
-   `minio` (Data Lake)
-   `airflow-metadata` (Banco de dados do Airflow)

## Passo 4: Acessar o Airflow e Executar o Pipeline

1.  **Acesse a Interface do Airflow:**
    -   Abra seu navegador e acesse `http://localhost:8080`.
    -   Pode levar um ou dois minutos para o webserver estar totalmente disponível na primeira inicialização.

2.  **Faça o Login:**
    -   **Usuário:** `admin`
    -   **Senha:** `admin`
    (Ou as credenciais que você definiu no seu arquivo `.env`).

3.  **Ative e Execute a DAG:**
    -   Na lista de DAGs, encontre a `pipeline_completo`.
    -   Ative-a clicando no botão de **toggle** na lateral esquerda.
    -   Para iniciar uma execução manual, clique no botão **Play (▶️)** na coluna "Actions".

### Comportamento da Primeira Execução

Na **primeira vez** que a DAG `pipeline_completo` for executada, a tarefa inicial `setup_ambiente_origem` irá popular o banco de dados de origem com 100.000 registros. Esta etapa pode levar alguns minutos para ser concluída.

Nas execuções subsequentes, esta tarefa será concluída rapidamente, pois ela é idempotente e detectará que o banco de dados já está populado.
