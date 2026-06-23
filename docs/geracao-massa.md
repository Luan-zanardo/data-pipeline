# Geração de Massa (Etapa 2)

A origem dos dados é simulada com **Python + Faker**. O script
[`generate_mock_landing.py`](https://github.com/Luan-zanardo/data-pipeline/blob/main/generate_mock_landing.py)
gera as 10 tabelas do e-commerce com **10.000 linhas cada** e datas
distribuídas pelos **últimos 3 anos**, gravando os CSVs diretamente na camada
Landing local.

> Este script é o atalho para rodar o pipeline **offline**, sem depender do
> banco de origem nem do Airflow. Em produção, a ingestão da Landing vem do
> Postgres de origem via Airflow (ver [Orquestração e Landing](orquestracao.md)).

## O que ele gera

- **10 tabelas**: `categorias`, `usuarios`, `produtos`, `pedidos`,
  `enderecos`, `pedido_itens`, `pagamentos`, `envio`, `avaliacoes`, `carrinho`.
- **10.000 linhas por tabela**, com `Faker('pt_BR')` (nomes, e-mails, endereços
  brasileiros).
- **Histórico de 3 anos**: as colunas de data são sorteadas no intervalo
  `[hoje − 3 anos, hoje]`.
- **Dados sujos propositais** nas últimas linhas de cada tabela (IDs
  duplicados, nulos, strings mal formatadas, valores negativos), para validar
  a limpeza na Silver — ver [Modelo de Dados](modelo-dados.md#dados-sujos-propositais).

## Como executar

```bash
pip install faker          # ou: pip install -r requirements.txt
python generate_mock_landing.py
```

A saída segue o mesmo layout particionado da Landing oficial:

```
datalake/landing/<tabela>/ingestion_date=<YYYY-MM-DD>/<tabela>.csv
```

Com os CSVs no lugar, siga direto para a transformação Spark em
[Bronze e Silver](bronze-silver.md).
