# Orquestração e Camada Landing (Etapa 3)

Esta etapa sobe um orquestrador (**Apache Airflow** em Docker) que **agenda** a
ingestão das tabelas do banco de origem e grava os dados na camada **Landing**
no **formato bruto original (CSV)**.

> ✅ O agendamento é feito pelo **scheduler do Airflow** — conforme exigido pela
> Etapa 3, **não** usamos `cron` do Linux nem o Agendador de Tarefas do Windows.

## Arquitetura desta etapa

```
Postgres (origem) ──ingestão──▶ Landing (CSV bruto, MinIO) ──manifesto──▶ Bronze (Etapa 4)
```

- **Orquestrador:** Apache Airflow `2.10.5` (LocalExecutor) — definido no
  `docker-compose.yml`.
- **Fonte:** Postgres do projeto (Supabase). A connection string fica no `.env`.
- **Destino (Landing):** object storage **MinIO** (S3-compatible), no bucket
  `datalake`, prefixo `landing/`, particionado por data de ingestão. O destino é
  controlado pela variável `LANDING_PATH` (`s3://datalake/landing`); apontá-la
  para um caminho local faz a ingestão gravar em disco, **sem mudar o código**.
- **Extração:** `COPY ... TO STDOUT WITH CSV` nativo do Postgres → fidelidade
  total ao dado bruto (sem transformação). A escrita usa `fsspec`/`s3fs`.

## DAG `ingestao_landing`

```
descobrir_tabelas → extrair_para_landing (1 task por tabela, em paralelo) → gerar_manifesto
```

- **descobrir_tabelas** — lista dinamicamente as tabelas do schema `public`
  (sem hardcode; adapta-se ao que a Etapa 2 criar).
- **extrair_para_landing** — para cada tabela, grava o CSV bruto na Landing
  (dynamic task mapping: uma task por tabela).
- **gerar_manifesto** — registra um manifesto JSON da execução (tabelas, linhas,
  caminhos) que serve de handoff para a camada Bronze.
- **Agendamento:** `schedule="0 6 * * *"` (diário às 06:00, `America/Sao_Paulo`),
  `catchup=False`, `retries=2`.

O código está em
[`dags/ingestao_landing.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/dags/ingestao_landing.py)
e a lógica reutilizável (pura, sem Airflow) em
[`src/ingestion/landing.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/src/ingestion/landing.py).

## Como executar

### 1. Pré-requisitos
- Docker + Docker Compose.

### 2. Configurar variáveis
```bash
cp .env.example .env
# edite o .env e preencha SOURCE_DB_PASSWORD com a senha do banco de origem
```

### 3. Subir o Airflow + MinIO
```bash
docker compose up -d
```

A primeira subida baixa as imagens e instala as dependências; aguarde o
`airflow-webserver` ficar *healthy*.

### 4. Acessar a UI e rodar
- Abra <http://localhost:8080> (login: `admin`/`admin`, ou o que estiver no `.env`).
- Ative (toggle) a DAG **`ingestao_landing`**.
- Dispare manualmente em **Trigger DAG** para ver a ingestão rodar na hora, ou
  aguarde o agendamento diário.

### 5. Conferir o resultado no MinIO
Console do MinIO em <http://localhost:9001> (login: `minioadmin`/`minioadmin`,
ou o do `.env`). Navegue no bucket `datalake`:

```
s3://datalake/landing/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv
s3://datalake/landing/_manifests/ingestion_<YYYY-MM-DD>.json
```

### Parar
```bash
docker compose down       # mantém os dados aterrissados
docker compose down -v    # remove também os metadados do Airflow e o volume MinIO
```

## Serviços do `docker-compose.yml`

| Serviço | Papel |
| ------- | ----- |
| `minio` | Object storage S3-compatible (API 9000, console 9001) que hospeda o Data Lake |
| `minio-init` | Cria o bucket `datalake` e encerra (idempotente) |
| `airflow-metadata` | Postgres de metadados **do próprio Airflow** (não é a fonte dos dados) |
| `airflow-init` | Migra o banco de metadados e cria o usuário admin da UI |
| `airflow-webserver` | UI do Airflow (porta 8080) |
| `airflow-scheduler` | Scheduler que dispara a DAG no horário agendado |

Os dados aterrissados ficam no volume Docker `minio-data`, **não** no
repositório.

## Validação

A conexão com o banco de origem e a leitura das 10 tabelas foram validadas com
sucesso (`usuarios`, `produtos`, `pedidos`, `pedido_itens`, `pagamentos`,
`envio`, `enderecos`, `categorias`, `avaliacoes`, `carrinho`).
