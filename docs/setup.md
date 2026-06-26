# Setup e Execução do Ambiente

Este guia descreve o ambiente Docker existente no projeto.

## Pré-requisitos

- Git
- Docker
- Docker Compose
- Credenciais para o PostgreSQL de origem e para o PostgreSQL de destino

## 1. Clonar o repositório

```sh
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
```

## 2. Configurar variáveis de ambiente

```sh
cp .env.example .env
```

Edite o `.env` e preencha as variáveis obrigatórias:

- `SOURCE_DB_HOST`
- `SOURCE_DB_PORT`
- `SOURCE_DB_NAME`
- `SOURCE_DB_USER`
- `SOURCE_DB_PASSWORD`
- `SOURCE_DB_SSLMODE`
- `DEST_DB_HOST`
- `DEST_DB_PORT`
- `DEST_DB_NAME`
- `DEST_DB_USER`
- `DEST_DB_PASSWORD`
- `DEST_DB_SSLMODE`

## 3. Construir e iniciar os serviços

```sh
docker compose up -d --build
```

Serviços principais:

- `airflow-webserver`
- `airflow-scheduler`
- `airflow-metadata`
- `minio`
- `minio-init`
- `metabase`
- `metabase-db`
- `metabase-init`

## 4. Executar a DAG

1. Acesse <http://localhost:8080>.
2. Faça login com o usuário e senha definidos no `.env` (padrão `admin`/`admin`).
3. Ative a DAG `pipeline_completo`.
4. Dispare uma execução manual.

Na primeira execução, a tarefa `setup_ambiente_origem` popula o PostgreSQL de
origem se ele ainda não tiver dados em `pedidos`. O script cria 10 tabelas e
insere 10.000 registros por tabela.

## 5. Acessar serviços auxiliares

- MinIO Console: <http://localhost:9001>
- Metabase: <http://localhost:3000>

O usuário admin do Metabase é criado pelo serviço `metabase-init`, usando
`MB_ADMIN_EMAIL` e `MB_ADMIN_PASSWORD`.
