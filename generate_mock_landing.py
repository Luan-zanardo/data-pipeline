"""Gerador de dados mock para a camada Landing.

Cria arquivos CSV correspondentes às 10 tabelas do banco de dados de origem
diretamente na pasta datalake/landing. Útil para testes locais quando a
conexão direta IPv6 com o banco de dados do Supabase não estiver disponível.
Inclui alguns registros com problemas (duplicados, nulos, textos não formatados)
para validar as etapas de limpeza e deduplicação na camada Silver.
"""

import os
from pathlib import Path
from datetime import datetime
import csv


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

    # 1. Categorias
    gerar_csv(
        base_path / "categorias" / f"ingestion_date={today}" / "categorias.csv",
        ["id", "nome", "descricao"],
        [
            ["1", "Eletrônicos", "Celulares, TVs e computadores"],
            ["2", "Livros", "Ficção, não-ficção e técnicos"],
            ["3", "Roupas", "Calçados e vestuário masculino/feminino"],
            ["4", " Casa & Cozinha ", "Móveis e utensílios domésticos"]  # Espaço a ser limpo
        ]
    )

    # 2. Usuarios
    gerar_csv(
        base_path / "usuarios" / f"ingestion_date={today}" / "usuarios.csv",
        ["id", "nome", "email"],
        [
            ["1", "Carlos Silva", "CARLOS@gmail.com"],
            ["2", "Mariana Costa", " mariana@hotmail.com "],
            ["3", "João Souza", "joao.SOUZA@outlook.com"],
            ["3", "João Souza Duplicado", "joao.souza@outlook.com"],  # ID duplicado
            ["4", "", "sem_nome@test.com"]  # Nome vazio (deve ser filtrado)
        ]
    )

    # 3. Produtos
    gerar_csv(
        base_path / "produtos" / f"ingestion_date={today}" / "produtos.csv",
        ["id", "nome", "descricao", "preco", "estoque", "categoria_id"],
        [
            ["1", "Smartphone X", "Celular com 128GB", "2999.99", "50", "1"],
            ["2", "Clean Code", "Livro do Robert Martin", "89.90", "100", "2"],
            ["3", "Camiseta Algodão", "Cor preta tamanho G", "49.90", "200", "3"],
            ["4", "Cafeteira Express", "Máquina de café", "450.00", "15", "4"],
            ["5", "Produto Inválido", "Preço negativo", "-10.00", "5", "1"],  # Negativo (deve ser filtrado)
            ["6", "", "Produto sem nome", "100.00", "5", "1"]  # Nome vazio (deve ser filtrado)
        ]
    )

    # 4. Pedidos
    gerar_csv(
        base_path / "pedidos" / f"ingestion_date={today}" / "pedidos.csv",
        ["id", "usuario_id", "data_pedido", "status"],
        [
            ["1", "1", "2026-06-18 10:00:00", " entregue "],  # Espaço e minúsculo
            ["2", "2", "2026-06-18 10:15:00", "cancelado"],
            ["3", "3", "2026-06-18 11:30:00", "pendente"],
            ["3", "3", "2026-06-18 12:00:00", "aprovado"]  # ID duplicado, deve manter o mais novo
        ]
    )

    # 5. Enderecos
    gerar_csv(
        base_path / "enderecos" / f"ingestion_date={today}" / "enderecos.csv",
        ["id", "usuario_id", "rua", "cidade", "estado", "zip_code", "pais"],
        [
            ["1", "1", "Rua das Flores, 123", "São Paulo", "sp", "01000-000", "Brasil"],
            ["2", "2", "Av. Central, 456", "Rio de Janeiro", " rj ", "20000-000", "Brasil"],
            ["3", "3", "Rua do Bosque, 789", "Curitiba", "PR", "80000-000", "Brasil"]
        ]
    )

    # 6. Pedido Itens
    gerar_csv(
        base_path / "pedido_itens" / f"ingestion_date={today}" / "pedido_itens.csv",
        ["id", "pedido_id", "produto_id", "quantidade", "preco"],
        [
            ["1", "1", "1", "1", "2999.99"],
            ["2", "1", "3", "2", "49.90"],
            ["3", "2", "2", "1", "89.90"],
            ["4", "3", "4", "0", "450.00"]  # Quantidade 0 (deve ser filtrada)
        ]
    )

    # 7. Pagamentos
    gerar_csv(
        base_path / "pagamentos" / f"ingestion_date={today}" / "pagamentos.csv",
        ["id", "pedido_id", "forma_pagamento", "quantia", "data_pagamento"],
        [
            ["1", "1", " cartao_credito ", "3099.79", "2026-06-18 10:05:00"],
            ["2", "2", "boleto", "89.90", "2026-06-18 10:30:00"],
            ["3", "3", "pix", "450.00", "2026-06-18 11:35:00"]
        ]
    )

    # 8. Envio
    gerar_csv(
        base_path / "envio" / f"ingestion_date={today}" / "envio.csv",
        ["id", "pedido_id", "endereco_id", "data_envio", "data_entrega", "status"],
        [
            ["1", "1", "1", "2026-06-18 14:00:00", "2026-06-19 15:00:00", "entregue"],
            ["2", "2", "2", "", "", "cancelado"]
        ]
    )

    # 9. Avaliacoes
    gerar_csv(
        base_path / "avaliacoes" / f"ingestion_date={today}" / "avaliacoes.csv",
        ["id", "usuario_id", "produto_id", "avaliacao", "comentario", "data_avaliacao"],
        [
            ["1", "1", "1", "5", "Excelente aparelho!", "2026-06-19 09:00:00"],
            ["2", "2", "2", "6", "Invalido", "2026-06-19 09:10:00"],  # Nota 6 (deve ser filtrada)
            ["3", "3", "3", "4", "Muito bom", "2026-06-19 09:20:00"]
        ]
    )

    # 10. Carrinho
    gerar_csv(
        base_path / "carrinho" / f"ingestion_date={today}" / "carrinho.csv",
        ["id", "usuario_id", "produto_id", "quantidade"],
        [
            ["1", "1", "2", "1"],
            ["2", "2", "3", "3"]
        ]
    )

    print("Massa mock criada com sucesso na Landing!")


if __name__ == "__main__":
    main()
