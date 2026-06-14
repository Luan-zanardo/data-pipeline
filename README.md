# data-pipeline

## Estrutura do Banco

```mermaid
erDiagram
    Usuarios ||--o{ Pedidos : realiza
    Usuarios ||--o{ Enderecos : possui
    Usuarios ||--o{ Avaliacoes : escreve
    Usuarios ||--o{ Carrinho : possui

    Pedidos ||--|{ Pedido_Itens : contem
    Pedidos ||--|| Pagamentos : pago_por
    Pedidos ||--|| Envio : enviado_para

    Produtos ||--o{ Pedido_Itens : incluido_em
    Produtos ||--o{ Avaliacoes : recebe
    Produtos ||--o{ Carrinho : adicionado_ao

    Categorias ||--o{ Produtos : categoriza

    Enderecos ||--o{ Envio : utilizado_em

    Usuarios {
        int id PK
        string nome
        string email
    }

    Produtos {
        int id PK
        string nome
        string descricao
        decimal preco
        int estoque
        int categoria_id FK
    }

    Pedidos {
        int id PK
        int usuario_id FK
        datetime data_pedido
        string status
    }

    Enderecos {
        int id PK
        int usuario_id FK
        string rua
        string cidade
        string estado
        string zip_code
        string pais
    }

    Categorias {
        int id PK
        string nome
        string descricao
    }

    Pedido_Itens {
        int id PK
        int pedido_id FK
        int produto_id FK
        int quantidade
        decimal preco
    }

    Pagamentos {
        int id PK
        int pedido_id FK
        string forma_pagamento
        decimal quantia
        datetime data_pagamento
    }

    Envio {
        int id PK
        int pedido_id FK
        int endereco_id FK
        datetime data_envio
        datetime data_entrega
        string status
    }

    Avaliacoes {
        int id PK
        int usuario_id FK
        int produto_id FK
        int avaliacao
        string comentario
        datetime data_avaliacao
    }

    Carrinho {
        int id PK
        int usuario_id FK
        int produto_id FK
        int quantidade
    }
```


## Connection Info

- **Host:** db.wutleihrwkhcfevexdcj.supabase.co  
- **Port:** 5432  
- **Database:** postgres  
- **User:** teammate  
- **Password:** vv4WSpDhi5vv  
- **SSL:** required  

### Connection String

```bash
psql "postgresql://teammate:PASSWORD@db.wutleihrwkhcfevexdcj.supabase.co:5432/postgres?sslmode=require"
```