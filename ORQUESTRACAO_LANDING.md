# Etapa 3 — Orquestração e Camada Landing (Issue #5)

Sobe um orquestrador (**Apache Airflow** em Docker) que **agenda** a ingestão
das tabelas do banco de origem e grava os dados na camada **Landing** no
**formato bruto original (CSV)**.

> ✅ O agendamento é feito pelo **scheduler do Airflow**, conforme exigido pela
> issue — **não** usamos `cron` do Linux nem o Agendador de Tarefas do Windows.

## Arquitetura desta etapa

```
Postgres (origem)  ──ingestão──▶  Landing (CSV bruto, MinIO)  ──manifesto──▶  Bronze (Etapa 4)
        ▲                                  ▲
        └── connection string (README)     └── s3://datalake/landing/<tabela>/ingestion_date=<data>/<tabela>.csv
```

- **Orquestrador:** Apache Airflow `2.10.5` (LocalExecutor) — `docker-compose.yml`.
- **Fonte:** Postgres do projeto (Supabase) — connection string no `README.md`.
- **Destino (Landing):** object storage **MinIO** (S3-compatible) provisionado
  pela Issue #4, no bucket `datalake`, prefixo `landing/`, particionado por data
  de ingestão. O destino é controlado pela variável `LANDING_PATH`
  (`s3://datalake/landing`); apontá-la para um caminho local faz a ingestão
  gravar em disco, sem mudar o código.
- **Extração:** `COPY ... TO STDOUT WITH CSV` nativo do Postgres → fidelidade
  total ao dado bruto (sem transformação). A escrita usa `fsspec`/`s3fs`.

## DAG `ingestao_landing`

```
descobrir_tabelas  →  extrair_para_landing (1 task por tabela, em paralelo)  →  gerar_manifesto
```

- **descobrir_tabelas** — lista dinamicamente as tabelas do schema `public`
  (não há hardcode; adapta-se ao que a Etapa 2 criar).
- **extrair_para_landing** — para cada tabela, grava o CSV bruto na Landing.
- **gerar_manifesto** — registra um manifesto JSON da execução (tabelas, linhas,
  caminhos) que serve de handoff para a camada Bronze.
- **Agendamento:** `schedule="0 6 * * *"` (diário às 06:00, fuso America/Sao_Paulo).

## Como executar

### 1. Pré-requisitos
- Docker + Docker Compose.

### 2. Configurar variáveis
```bash
cp .env.example .env
# edite o .env e preencha SOURCE_DB_PASSWORD com a senha do README
```

### 3. Subir o Airflow
```bash
docker compose up -d
```

A primeira subida baixa a imagem e instala as dependências; aguarde o
`airflow-webserver` ficar healthy.

### 4. Acessar a UI e rodar
- Abra <http://localhost:8080> (usuário/senha: `admin`/`admin`, ou o que estiver no `.env`).
- Ative (toggle) a DAG **`ingestao_landing`**.
- Dispare manualmente em **Trigger DAG** para ver a ingestão rodar na hora, ou
  aguarde o agendamento diário.

### 5. Conferir o resultado
Os CSVs aterrissados aparecem no MinIO. Abra o console em
<http://localhost:9001> (login: `minioadmin`/`minioadmin`, ou o do `.env`) e
navegue no bucket `datalake`:
```
s3://datalake/landing/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv
s3://datalake/landing/_manifests/ingestion_<YYYY-MM-DD>.json
```

### Parar
```bash
docker compose down          # mantém os dados aterrissados
docker compose down -v       # remove também os metadados do Airflow
```

## Validação

A conexão com o banco de origem e a leitura das 10 tabelas foram validadas
(≈100 mil linhas por tabela: `usuarios`, `produtos`, `pedidos`, `pedido_itens`,
`pagamentos`, `envio`, `enderecos`, `categorias`, `avaliacoes`, `carrinho`).

## Estrutura adicionada

```
docker-compose.yml              # Airflow + MinIO (orquestrador + object storage)
.env.example                    # configuração (copiar para .env)
dags/
  ingestao_landing.py           # DAG de ingestão + movimentação
src/ingestion/
  landing.py                    # extração Postgres -> CSV bruto no MinIO (reutilizável)
```

Os serviços `minio` (object storage) e `minio-init` (cria o bucket `datalake`)
fazem parte do `docker-compose.yml`. Os dados aterrissados ficam no volume
Docker `minio-data`, não no repositório.
