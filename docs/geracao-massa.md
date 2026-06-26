# Geração de Massa (Etapa 2)

A massa de dados é criada por `src/setup.py`. Esse script é chamado pela tarefa
`setup_ambiente_origem` da DAG `pipeline_completo`.

## O que o script faz

- Conecta no PostgreSQL de origem usando `SOURCE_DB_*`.
- Verifica se a tabela `pedidos` existe e já possui registros.
- Se a origem já estiver populada, não altera os dados.
- Se estiver vazia, cria 10 tabelas de e-commerce e insere 10.000 linhas em
  cada uma.
- Usa `Faker('pt_BR')` para nomes, e-mails, endereços e textos.
- Distribui datas aleatórias nos últimos 3 anos.

## Tabelas criadas

- `categorias`
- `usuarios`
- `produtos`
- `pedidos`
- `enderecos`
- `pedido_itens`
- `pagamentos`
- `envio`
- `avaliacoes`
- `carrinho`

## Observação sobre qualidade dos dados

O script atual não injeta dados sujos de propósito. A etapa Silver ainda faz
deduplicação por `id` e algumas padronizações/casts, mas a documentação não deve
assumir a existência de registros inválidos artificiais.

## Execução

No fluxo normal, não é necessário chamar esse arquivo manualmente: a DAG executa
o setup antes da ingestão.

Para rodar fora do Airflow, o ambiente precisa ter as dependências Python e as
variáveis `SOURCE_DB_*` configuradas:

```bash
python src/setup.py
```
