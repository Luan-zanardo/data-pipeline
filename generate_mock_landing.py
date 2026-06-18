"""Gerador de dados mock para a camada Landing.

Cria arquivos CSV correspondentes às 10 tabelas do banco de dados de origem
diretamente na pasta datalake/landing. Gera 10.000 registros principais
para simular um ambiente de produção realista com histórico de 3 anos,
atendendo rigorosamente às exigências do trabalho final.
Inclui também alguns registros com problemas (duplicados, nulos, strings
mal formatadas) propositalmente, para validar as etapas de limpeza e
deduplicação na camada Silver do seu pipeline.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
import csv
import random

try:
    from faker import Faker
except ImportError:
    print("Biblioteca 'faker' não encontrada. Instale com: pip install faker")
    exit(1)

# Inicializa o Faker configurado para o Brasil
fake = Faker('pt_BR')

NUM_ROWS = 10000
END_DATE = datetime.today()
START_DATE = END_DATE - timedelta(days=3 * 365)  # 3 anos atrás

def random_date(start, end):
    """Gera uma data aleatória entre start e end."""
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

def gerar_csv(caminho: Path, cabecalho: list[str], linhas: list[list]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(cabecalho)
        writer.writerows(linhas)
    print(f"Gerado: {caminho} ({len(linhas)} linhas)")

def main():
    today = datetime.today().strftime("%Y-%m-%d")
    base_path = Path("datalake/landing")

    print(f"Iniciando geração de {NUM_ROWS} registros por tabela usando Faker...")
    print(f"As datas estarão distribuídas entre {START_DATE.strftime('%Y-%m-%d')} e {END_DATE.strftime('%Y-%m-%d')}.")
    print("Aguarde, isso pode levar alguns segundos...")

    # 1. Categorias
    linhas_categorias = []
    for i in range(1, NUM_ROWS + 1):
        if i > NUM_ROWS - 5:  # Registros "sujos" no final
            linhas_categorias.append([str(i), " Categoria Suja ", ""])
        else:
            linhas_categorias.append([str(i), fake.word().capitalize(), fake.sentence(nb_words=6)])
    gerar_csv(base_path / "categorias" / f"ingestion_date={today}" / "categorias.csv", ["id", "nome", "descricao"], linhas_categorias)

    # 2. Usuarios
    linhas_usuarios = []
    for i in range(1, NUM_ROWS + 1):
        if i > NUM_ROWS - 5:
            # Duplicando ID e sujando nome
            linhas_usuarios.append([str(i - 1), "", "sem_nome@test.com"])
        else:
            nome = fake.name()
            # 10% com e-mail mal formatado (espaços e letras maiúsculas)
            email = fake.email() if random.random() > 0.1 else f" {fake.email().upper()} "
            linhas_usuarios.append([str(i), nome, email])
    gerar_csv(base_path / "usuarios" / f"ingestion_date={today}" / "usuarios.csv", ["id", "nome", "email"], linhas_usuarios)

    # 3. Produtos
    linhas_produtos = []
    for i in range(1, NUM_ROWS + 1):
        preco = round(random.uniform(10.0, 5000.0), 2)
        estoque = random.randint(0, 500)
        cat_id = random.randint(1, NUM_ROWS)
        if i > NUM_ROWS - 5:
            linhas_produtos.append([str(i), "", "Produto Invalido", "-10.00", "5", "1"])  # Preço negativo e nome vazio
        else:
            linhas_produtos.append([str(i), fake.word().capitalize(), fake.sentence(nb_words=10), f"{preco:.2f}", str(estoque), str(cat_id)])
    gerar_csv(base_path / "produtos" / f"ingestion_date={today}" / "produtos.csv", ["id", "nome", "descricao", "preco", "estoque", "categoria_id"], linhas_produtos)

    # 4. Pedidos (Datas distribuídas em 3 anos)
    linhas_pedidos = []
    status_list = ["pendente", "aprovado", "enviado", " entregue ", "cancelado", "CANCELADO"]
    for i in range(1, NUM_ROWS + 1):
        user_id = random.randint(1, NUM_ROWS)
        dt_pedido = random_date(START_DATE, END_DATE).strftime("%Y-%m-%d %H:%M:%S")
        status = random.choice(status_list)
        if i > NUM_ROWS - 5:
            linhas_pedidos.append([str(i - 1), str(user_id), dt_pedido, status])  # IDs duplicados para testar deduplicação
        else:
            linhas_pedidos.append([str(i), str(user_id), dt_pedido, status])
    gerar_csv(base_path / "pedidos" / f"ingestion_date={today}" / "pedidos.csv", ["id", "usuario_id", "data_pedido", "status"], linhas_pedidos)

    # 5. Enderecos
    linhas_enderecos = []
    for i in range(1, NUM_ROWS + 1):
        user_id = random.randint(1, NUM_ROWS)
        # Inserindo sigla de estado incorreta ou com espaços às vezes
        estado = fake.state_abbr() if random.random() > 0.05 else fake.state_abbr().lower() + " "
        linhas_enderecos.append([str(i), str(user_id), fake.street_address(), fake.city(), estado, fake.postcode(), "Brasil"])
    gerar_csv(base_path / "enderecos" / f"ingestion_date={today}" / "enderecos.csv", ["id", "usuario_id", "rua", "cidade", "estado", "zip_code", "pais"], linhas_enderecos)

    # 6. Pedido Itens
    linhas_pedido_itens = []
    for i in range(1, NUM_ROWS + 1):
        pedido_id = random.randint(1, NUM_ROWS)
        produto_id = random.randint(1, NUM_ROWS)
        qtd = random.randint(1, 5)
        preco = round(random.uniform(10.0, 1000.0), 2)
        if i > NUM_ROWS - 5:
            linhas_pedido_itens.append([str(i), str(pedido_id), str(produto_id), "0", f"{preco:.2f}"])  # Quantidade 0 inválida
        else:
            linhas_pedido_itens.append([str(i), str(pedido_id), str(produto_id), str(qtd), f"{preco:.2f}"])
    gerar_csv(base_path / "pedido_itens" / f"ingestion_date={today}" / "pedido_itens.csv", ["id", "pedido_id", "produto_id", "quantidade", "preco"], linhas_pedido_itens)

    # 7. Pagamentos
    linhas_pagamentos = []
    formas = ["cartao_credito", "boleto", "pix", " CARTAO_CREDITO "]
    for i in range(1, NUM_ROWS + 1):
        pedido_id = random.randint(1, NUM_ROWS)
        quantia = round(random.uniform(20.0, 2000.0), 2)
        dt_pagamento = random_date(START_DATE, END_DATE).strftime("%Y-%m-%d %H:%M:%S")
        if i > NUM_ROWS - 5:
            linhas_pagamentos.append([str(i), str(pedido_id), "pix", "-50.00", dt_pagamento])  # Quantia negativa inválida
        else:
            linhas_pagamentos.append([str(i), str(pedido_id), random.choice(formas), f"{quantia:.2f}", dt_pagamento])
    gerar_csv(base_path / "pagamentos" / f"ingestion_date={today}" / "pagamentos.csv", ["id", "pedido_id", "forma_pagamento", "quantia", "data_pagamento"], linhas_pagamentos)

    # 8. Envio
    linhas_envio = []
    status_envio = ["preparando", "em_transito", "entregue", "cancelado", " ENTREGUE "]
    for i in range(1, NUM_ROWS + 1):
        pedido_id = random.randint(1, NUM_ROWS)
        endereco_id = random.randint(1, NUM_ROWS)
        dt_envio = random_date(START_DATE, END_DATE)
        # Data de entrega pode ser None se não entregue ainda (20% das vezes)
        dt_entrega = (dt_envio + timedelta(days=random.randint(1, 15))) if random.random() > 0.2 else None
        
        dt_envio_str = dt_envio.strftime("%Y-%m-%d %H:%M:%S")
        dt_entrega_str = dt_entrega.strftime("%Y-%m-%d %H:%M:%S") if dt_entrega else ""
        
        linhas_envio.append([str(i), str(pedido_id), str(endereco_id), dt_envio_str, dt_entrega_str, random.choice(status_envio)])
    gerar_csv(base_path / "envio" / f"ingestion_date={today}" / "envio.csv", ["id", "pedido_id", "endereco_id", "data_envio", "data_entrega", "status"], linhas_envio)

    # 9. Avaliacoes
    linhas_avaliacoes = []
    for i in range(1, NUM_ROWS + 1):
        usuario_id = random.randint(1, NUM_ROWS)
        produto_id = random.randint(1, NUM_ROWS)
        avaliacao = random.randint(1, 5)
        dt_aval = random_date(START_DATE, END_DATE).strftime("%Y-%m-%d %H:%M:%S")
        if i > NUM_ROWS - 5:
            linhas_avaliacoes.append([str(i), str(usuario_id), str(produto_id), "6", "Invalido", dt_aval])  # Nota maior que 5
        else:
            linhas_avaliacoes.append([str(i), str(usuario_id), str(produto_id), str(avaliacao), fake.text(max_nb_chars=50).replace("\n", " "), dt_aval])
    gerar_csv(base_path / "avaliacoes" / f"ingestion_date={today}" / "avaliacoes.csv", ["id", "usuario_id", "produto_id", "avaliacao", "comentario", "data_avaliacao"], linhas_avaliacoes)

    # 10. Carrinho
    linhas_carrinho = []
    for i in range(1, NUM_ROWS + 1):
        usuario_id = random.randint(1, NUM_ROWS)
        produto_id = random.randint(1, NUM_ROWS)
        qtd = random.randint(1, 10)
        if i > NUM_ROWS - 5:
            linhas_carrinho.append([str(i), str(usuario_id), str(produto_id), "0"])  # Qtd 0
        else:
            linhas_carrinho.append([str(i), str(usuario_id), str(produto_id), str(qtd)])
    gerar_csv(base_path / "carrinho" / f"ingestion_date={today}" / "carrinho.csv", ["id", "usuario_id", "produto_id", "quantidade"], linhas_carrinho)

    print("=====================================================")
    print("✅ Sucesso! Massa mock criada na Landing!")
    print("Agora as 10 tabelas possuem 10.000 linhas com histórico")
    print("de 3 anos, simulando perfeitamente a premissa do professor.")
    print("=====================================================")

if __name__ == "__main__":
    main()
