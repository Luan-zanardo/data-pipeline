# Etapa 3 — Orquestração e Camada Landing (Issue #5)

> ℹ️ A documentação desta etapa foi consolidada no MkDocs para manter uma única
> fonte de verdade. Veja **[`docs/orquestracao.md`](docs/orquestracao.md)**
> (ou a página *Orquestração e Landing* no site do MkDocs).

Resumo: sobe um orquestrador (**Apache Airflow** em Docker) que **agenda** a
ingestão das tabelas do Postgres de origem e grava os dados na camada
**Landing** no formato bruto original (CSV), no object storage **MinIO**. O
agendamento é do scheduler do Airflow — sem `cron` do Linux nem Agendador do
Windows.

Início rápido:

```bash
cp .env.example .env       # preencha SOURCE_DB_PASSWORD
docker compose up -d       # Airflow: http://localhost:8080 · MinIO: http://localhost:9001
```

Ative e dispare a DAG **`ingestao_landing`**. Detalhes completos (DAG, layout
dos objetos, serviços do compose, validação) em
[`docs/orquestracao.md`](docs/orquestracao.md).
