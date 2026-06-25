"""Gerador de dados mock para o banco de dados de ORIGEM.

Conecta-se a um banco de dados PostgreSQL e insere dados gerados pela
biblioteca Faker. Cria 10 tabelas e popula com 10.000 registros principais,
simulando um ambiente de produção com histórico de 3 anos.

Este script deve ser executado uma vez para preparar o ambiente de origem
que será consumido pelo pipeline de dados do Airflow.
"""

import os
import csv
import random
from datetime import datetime, timedelta
from io import StringIO

try:
    from faker import Faker
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    print(f"Biblioteca '{e.name}' não encontrada. Instale com: pip install {e.name}")
    exit(1)

# --- CONFIGURAÇÕES ---
NUM_ROWS = 10000
END_DATE = datetime.today()
START_DATE = END_DATE - timedelta(days=3 * 365)
DB_HOST = os.environ.get("SOURCE_DB_HOST", "localhost")
DB_PORT = os.environ.get("SOURCE_DB_PORT", "5433")
DB_NAME = os.environ.get("SOURCE_DB_NAME", "postgres")
DB_USER = os.environ.get("SOURCE_DB_USER", "postgres")
DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD", "postgres")

fake = Faker('pt_BR')

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def random_date(start, end):
    """Gera uma data aleatória entre start e end."""
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

def bulk_insert(conn, table_name: str, columns: list[str], data: list[tuple]):
    """Insere dados em massa em uma tabela usando o comando COPY."""
    if not data:
        return
    
    with conn.cursor() as cur:
        # Usando StringIO para simular um arquivo em memória
        buffer = StringIO()
        writer = csv.writer(buffer, delimiter='\t', quotechar='"')
        writer.writerows(data)
        buffer.seek(0)
        
        # O comando COPY é muito mais rápido para grandes volumes
        cur.copy_from(buffer, table_name, sep='\t', columns=columns)
    print(f"Inserido {len(data)} registros na tabela '{table_name}'.")


def main():
    """Função principal para gerar e inserir os dados."""
    print("Conectando ao banco de dados de origem...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        print(f"ERRO: Não foi possível conectar ao banco de dados em {DB_HOST}:{DB_PORT}.")
        print("Verifique se o ambiente Docker está rodando ('docker-compose up -d').")
        print(f"Detalhes do erro: {e}")
        return

    print("Conexão bem-sucedida. Iniciando a criação das tabelas (se não existirem)...")

    # --- ESTRUTURA DAS TABELAS ---
    create_scripts = [
        "CREATE TABLE IF NOT EXISTS categorias (id INT PRIMARY KEY, nome VARCHAR(100), descricao TEXT);",
        "CREATE TABLE IF NOT EXISTS usuarios (id INT PRIMARY KEY, nome VARCHAR(255), email VARCHAR(255));",
        "CREATE TABLE IF NOT EXISTS produtos (id INT PRIMARY KEY, nome VARCHAR(255), descricao TEXT, preco DECIMAL(10, 2), estoque INT, categoria_id INT);",
        "CREATE TABLE IF NOT EXISTS pedidos (id INT PRIMARY KEY, usuario_id INT, data_pedido TIMESTAMP, status VARCHAR(50));",
        "CREATE TABLE IF NOT EXISTS enderecos (id INT PRIMARY KEY, usuario_id INT, rua VARCHAR(255), cidade VARCHAR(100), estado VARCHAR(2), zip_code VARCHAR(10), pais VARCHAR(50));",
        "CREATE TABLE IF NOT EXISTS pedido_itens (id INT PRIMARY KEY, pedido_id INT, produto_id INT, quantidade INT, preco DECIMAL(10, 2));",
        "CREATE TABLE IF NOT EXISTS pagamentos (id INT PRIMARY KEY, pedido_id INT, forma_pagamento VARCHAR(50), quantia DECIMAL(10, 2), data_pagamento TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS envio (id INT PRIMARY KEY, pedido_id INT, endereco_id INT, data_envio TIMESTAMP, data_entrega TIMESTAMP, status VARCHAR(50));",
        "CREATE TABLE IF NOT EXISTS avaliacoes (id INT PRIMARY KEY, usuario_id INT, produto_id INT, avaliacao INT, comentario TEXT, data_avaliacao TIMESTAMP);",
        "CREATE TABLE IF NOT EXISTS carrinho (id INT PRIMARY KEY, usuario_id INT, produto_id INT, quantidade INT);"
    ]
    for script in create_scripts:
        cur.execute(script)

    print("Tabelas prontas. Gerando e inserindo dados...")
    print(f"As datas serão distribuídas entre {START_DATE.strftime('%Y-%m-%d')} e {END_DATE.strftime('%Y-%m-%d')}.")

    # --- GERAÇÃO DE DADOS ---
    # 1. Categorias
    linhas_categorias = [(i, fake.word().capitalize(), fake.sentence(nb_words=6)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "categorias", ["id", "nome", "descricao"], linhas_categorias)

    # 2. Usuarios
    linhas_usuarios = [(i, fake.name(), fake.email()) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "usuarios", ["id", "nome", "email"], linhas_usuarios)

    # 3. Produtos
    linhas_produtos = [(i, fake.word().capitalize(), fake.sentence(nb_words=10), round(random.uniform(10.0, 5000.0), 2), random.randint(0, 500), random.randint(1, NUM_ROWS)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "produtos", ["id", "nome", "descricao", "preco", "estoque", "categoria_id"], linhas_produtos)

    # 4. Pedidos
    status_list = ["pendente", "aprovado", "enviado", "entregue", "cancelado"]
    linhas_pedidos = [(i, random.randint(1, NUM_ROWS), random_date(START_DATE, END_DATE), random.choice(status_list)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "pedidos", ["id", "usuario_id", "data_pedido", "status"], linhas_pedidos)

    # 5. Enderecos
    linhas_enderecos = [(i, random.randint(1, NUM_ROWS), fake.street_address(), fake.city(), fake.state_abbr(), fake.postcode(), "Brasil") for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "enderecos", ["id", "usuario_id", "rua", "cidade", "estado", "zip_code", "pais"], linhas_enderecos)

    # 6. Pedido Itens
    linhas_pedido_itens = [(i, random.randint(1, NUM_ROWS), random.randint(1, NUM_ROWS), random.randint(1, 5), round(random.uniform(10.0, 1000.0), 2)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "pedido_itens", ["id", "pedido_id", "produto_id", "quantidade", "preco"], linhas_pedido_itens)

    # 7. Pagamentos
    formas = ["cartao_credito", "boleto", "pix"]
    linhas_pagamentos = [(i, random.randint(1, NUM_ROWS), random.choice(formas), round(random.uniform(20.0, 2000.0), 2), random_date(START_DATE, END_DATE)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "pagamentos", ["id", "pedido_id", "forma_pagamento", "quantia", "data_pagamento"], linhas_pagamentos)

    # 8. Envio
    status_envio = ["preparando", "em_transito", "entregue", "cancelado"]
    linhas_envio = []
    for i in range(1, NUM_ROWS + 1):
        dt_envio = random_date(START_DATE, END_DATE)
        dt_entrega = (dt_envio + timedelta(days=random.randint(1, 15))) if random.random() > 0.2 else None
        linhas_envio.append((i, random.randint(1, NUM_ROWS), random.randint(1, NUM_ROWS), dt_envio, dt_entrega, random.choice(status_envio)))
    bulk_insert(conn, "envio", ["id", "pedido_id", "endereco_id", "data_envio", "data_entrega", "status"], linhas_envio)

    # 9. Avaliacoes
    linhas_avaliacoes = [(i, random.randint(1, NUM_ROWS), random.randint(1, NUM_ROWS), random.randint(1, 5), fake.text(max_nb_chars=50).replace("\n", " "), random_date(START_DATE, END_DATE)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "avaliacoes", ["id", "usuario_id", "produto_id", "avaliacao", "comentario", "data_avaliacao"], linhas_avaliacoes)

    # 10. Carrinho
    linhas_carrinho = [(i, random.randint(1, NUM_ROWS), random.randint(1, NUM_ROWS), random.randint(1, 10)) for i in range(1, NUM_ROWS + 1)]
    bulk_insert(conn, "carrinho", ["id", "usuario_id", "produto_id", "quantidade"], linhas_carrinho)

    # --- FINALIZAÇÃO ---
    conn.commit()
    cur.close()
    conn.close()

    print("\n=====================================================")
    print("✅ Sucesso! Banco de dados de origem populado.")
    print("O pipeline de dados agora pode ser executado para ingerir os dados.")
    print("=====================================================")

if __name__ == "__main__":
    main()
