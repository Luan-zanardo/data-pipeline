"""Silver to Gold.

Le as tabelas da camada Silver, constroi dimensoes e fato do modelo dimensional
da camada Gold e grava tudo em formato Delta Lake no filesystem local.
"""

import argparse
import logging
import os
from datetime import datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    create_map,
    current_timestamp,
    date_format,
    dayofmonth,
    dayofweek,
    element_at,
    explode,
    first,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    quarter,
    row_number,
    sequence,
    sha2,
    to_date,
    year,
)
from pyspark.sql.window import Window

# Configuracao de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("silver_to_gold")


def get_spark_session() -> SparkSession:
    """Cria ou recupera a SparkSession configurada com suporte ao Delta Lake."""
    logger.info("Inicializando SparkSession com suporte a Delta Lake...")
    return SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.sources.default", "delta") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .getOrCreate()


def tabela_delta_existe(spark: SparkSession, path_tabela: Path) -> bool:
    """Verifica se um caminho existe e contem uma tabela Delta."""
    return path_tabela.exists() and DeltaTable.isDeltaTable(spark, str(path_tabela))


def ler_tabela_silver(spark: SparkSession, base_path: Path, tabela: str):
    """Le uma tabela Delta da camada Silver."""
    path_silver = base_path / "silver" / tabela
    if not tabela_delta_existe(spark, path_silver):
        raise FileNotFoundError(f"Tabela Silver [{tabela}] nao encontrada em: {path_silver}")
    return spark.read.format("delta").load(str(path_silver))


def adicionar_hash_scd(df, atributos: list[str]):
    """Adiciona o hash SCD a partir dos atributos versionados."""
    colunas_hash = [
        coalesce(col(atributo).cast("string"), lit("__null__"))
        for atributo in atributos
    ]
    return df.withColumn("hash_scd", sha2(concat_ws("||", *colunas_hash), 256))


def obter_maior_surrogate(spark: SparkSession, path_tabela: Path, coluna_sk: str) -> int:
    """Busca o maior surrogate ja gravado para evitar colisao de chaves."""
    if not tabela_delta_existe(spark, path_tabela):
        return 0

    valor = spark.read.format("delta").load(str(path_tabela)) \
        .agg(spark_max(col(coluna_sk)).alias("maior_sk")) \
        .collect()[0]["maior_sk"]

    return int(valor or 0)


def processar_dim_data(spark: SparkSession, base_path: Path) -> None:
    """Gera a dimensao de datas a partir do menor e maior pedido da Silver."""
    logger.info("Processando dimensao [dim_data]...")
    pedidos = ler_tabela_silver(spark, base_path, "pedidos")
    path_gold = base_path / "gold" / "dim_data"

    limites = pedidos.select(to_date(col("data_pedido")).alias("data")) \
        .agg(
            spark_min(col("data")).alias("min_data"),
            spark_max(col("data")).alias("max_data"),
        ) \
        .collect()[0]

    if limites["min_data"] is None or limites["max_data"] is None:
        logger.info("Nenhum pedido encontrado para gerar a dimensao [dim_data].")
        return

    nomes_meses = create_map(
        lit(1), lit("janeiro"),
        lit(2), lit("fevereiro"),
        lit(3), lit("marco"),
        lit(4), lit("abril"),
        lit(5), lit("maio"),
        lit(6), lit("junho"),
        lit(7), lit("julho"),
        lit(8), lit("agosto"),
        lit(9), lit("setembro"),
        lit(10), lit("outubro"),
        lit(11), lit("novembro"),
        lit(12), lit("dezembro"),
    )

    df_limites = spark.createDataFrame(
        [(limites["min_data"], limites["max_data"])],
        ["min_data", "max_data"],
    )

    df_dim_data = df_limites.select(explode(sequence(col("min_data"), col("max_data"))).alias("data")) \
        .withColumn("sk_data", date_format(col("data"), "yyyyMMdd").cast("int")) \
        .withColumn("ano", year(col("data"))) \
        .withColumn("trimestre", quarter(col("data"))) \
        .withColumn("mes", month(col("data"))) \
        .withColumn("nome_mes", element_at(nomes_meses, col("mes"))) \
        .withColumn("dia", dayofmonth(col("data"))) \
        .withColumn("dia_semana", dayofweek(col("data"))) \
        .select("sk_data", "data", "ano", "trimestre", "mes", "nome_mes", "dia", "dia_semana")

    df_dim_data.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(str(path_gold))

    logger.info(f"Dimensao [dim_data] gravada com sucesso em: {path_gold}")


def processar_dimensao_scd2(
    spark: SparkSession,
    df_origem,
    tabela: str,
    base_path: Path,
    chave_natural: str,
    coluna_sk: str,
) -> None:
    """Processa uma dimensao SCD tipo 2 de forma idempotente."""
    logger.info(f"Processando dimensao SCD2 [{tabela}]...")
    path_gold = base_path / "gold" / tabela
    df_origem = df_origem.dropDuplicates([chave_natural]).cache()

    if df_origem.limit(1).count() == 0:
        logger.info(f"Nenhum registro de origem encontrado para a dimensao [{tabela}].")
        df_origem.unpersist()
        return

    instante_processamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    timestamp_processamento = lit(instante_processamento).cast("timestamp")

    if not tabela_delta_existe(spark, path_gold):
        logger.info(f"Dimensao [{tabela}] nao existe. Criando pela primeira vez em: {path_gold}")
        maior_sk = 0
        window_sk = Window.orderBy(col(chave_natural))
        colunas_origem = df_origem.columns

        df_inicial = df_origem \
            .withColumn(coluna_sk, (row_number().over(window_sk) + lit(maior_sk)).cast("long")) \
            .withColumn("valido_de", timestamp_processamento) \
            .withColumn("valido_ate", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True)) \
            .select(coluna_sk, *colunas_origem, "valido_de", "valido_ate", "is_current")

        total_inicial = df_inicial.count()
        df_inicial.write \
            .format("delta") \
            .mode("overwrite") \
            .save(str(path_gold))

        df_origem.unpersist()
        logger.info(f"Dimensao [{tabela}] criada com sucesso. Registros inseridos: {total_inicial}")
        return

    df_atual = spark.read.format("delta").load(str(path_gold)) \
        .filter(col("is_current") == lit(True)) \
        .select(chave_natural, "hash_scd")

    df_novos_ou_alterados = df_origem.alias("source") \
        .join(df_atual.alias("target"), chave_natural, "left") \
        .filter(
            col("target.hash_scd").isNull()
            | (col("source.hash_scd") != col("target.hash_scd"))
        ) \
        .select("source.*") \
        .cache()

    if df_novos_ou_alterados.limit(1).count() == 0:
        logger.info(f"Nenhuma mudanca identificada para a dimensao [{tabela}].")
        df_novos_ou_alterados.unpersist()
        df_origem.unpersist()
        return

    df_alterados = df_origem.alias("source") \
        .join(df_atual.alias("target"), chave_natural, "inner") \
        .filter(col("source.hash_scd") != col("target.hash_scd")) \
        .select(col(chave_natural)) \
        .cache()

    if df_alterados.limit(1).count() > 0:
        logger.info(f"Fechando versoes atuais alteradas da dimensao [{tabela}]...")
        delta_table = DeltaTable.forPath(spark, str(path_gold))
        delta_table.alias("target") \
            .merge(
                df_alterados.alias("source"),
                f"target.{chave_natural} = source.{chave_natural} AND target.is_current = true",
            ) \
            .whenMatchedUpdate(set={
                "is_current": "false",
                "valido_ate": f"CAST('{instante_processamento}' AS TIMESTAMP)",
            }) \
            .execute()
    df_alterados.unpersist()

    maior_sk = obter_maior_surrogate(spark, path_gold, coluna_sk)
    window_sk = Window.orderBy(col(chave_natural))
    colunas_origem = df_novos_ou_alterados.columns

    df_para_inserir = df_novos_ou_alterados \
        .withColumn(coluna_sk, (row_number().over(window_sk) + lit(maior_sk)).cast("long")) \
        .withColumn("valido_de", timestamp_processamento) \
        .withColumn("valido_ate", lit(None).cast("timestamp")) \
        .withColumn("is_current", lit(True)) \
        .select(coluna_sk, *colunas_origem, "valido_de", "valido_ate", "is_current")

    total_inseridos = df_para_inserir.count()
    df_para_inserir.write \
        .format("delta") \
        .mode("append") \
        .save(str(path_gold))

    df_novos_ou_alterados.unpersist()
    df_origem.unpersist()
    logger.info(f"Dimensao [{tabela}] atualizada com sucesso. Registros inseridos: {total_inseridos}")


def processar_dim_cliente(spark: SparkSession, base_path: Path) -> None:
    """Monta e processa a dimensao de clientes com SCD tipo 2."""
    usuarios = ler_tabela_silver(spark, base_path, "usuarios")

    df_cliente = usuarios.select(
        col("id").alias("id_cliente"),
        col("nome"),
        col("email"),
    )
    df_cliente = adicionar_hash_scd(df_cliente, ["nome", "email"])

    processar_dimensao_scd2(
        spark=spark,
        df_origem=df_cliente,
        tabela="dim_cliente",
        base_path=base_path,
        chave_natural="id_cliente",
        coluna_sk="sk_cliente",
    )


def processar_dim_produto(spark: SparkSession, base_path: Path) -> None:
    """Monta e processa a dimensao de produtos com SCD tipo 2."""
    produtos = ler_tabela_silver(spark, base_path, "produtos").alias("p")
    categorias = ler_tabela_silver(spark, base_path, "categorias").alias("c")

    df_produto = produtos.join(
        categorias,
        col("p.categoria_id") == col("c.id"),
        "left",
    ).select(
        col("p.id").alias("id_produto"),
        col("p.nome").alias("nome"),
        col("p.preco").alias("preco"),
        col("p.estoque").alias("estoque"),
        col("p.categoria_id").alias("categoria_id"),
        col("c.nome").alias("categoria_nome"),
    )
    df_produto = adicionar_hash_scd(
        df_produto,
        ["nome", "preco", "estoque", "categoria_id", "categoria_nome"],
    )

    processar_dimensao_scd2(
        spark=spark,
        df_origem=df_produto,
        tabela="dim_produto",
        base_path=base_path,
        chave_natural="id_produto",
        coluna_sk="sk_produto",
    )


def obter_checkpoint_fato(spark: SparkSession, path_checkpoint: Path) -> datetime | None:
    """Le o checkpoint da fato de vendas, quando existir."""
    if not tabela_delta_existe(spark, path_checkpoint):
        return None

    registro = spark.read.format("delta").load(str(path_checkpoint)) \
        .filter(col("tabela") == lit("fato_vendas")) \
        .agg(spark_max(col("last_value")).alias("last_value")) \
        .collect()[0]

    return registro["last_value"]


def atualizar_checkpoint_fato(spark: SparkSession, path_checkpoint: Path, last_value: datetime) -> None:
    """Atualiza o checkpoint da fato de vendas em Delta."""
    df_checkpoint = spark.createDataFrame(
        [("fato_vendas", last_value)],
        ["tabela", "last_value"],
    ).withColumn("updated_at", current_timestamp())

    df_checkpoint.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(str(path_checkpoint))

    logger.info(f"Checkpoint da fato [fato_vendas] atualizado para: {last_value}")


def processar_fato_vendas(spark: SparkSession, base_path: Path) -> None:
    """Processa a fato de vendas com carga incremental por checkpoint."""
    logger.info("Processando fato [fato_vendas]...")
    path_gold = base_path / "gold" / "fato_vendas"
    path_checkpoint = base_path / "gold" / "_checkpoints" / "fato_vendas"

    pedidos = ler_tabela_silver(spark, base_path, "pedidos").alias("p")
    pedido_itens = ler_tabela_silver(spark, base_path, "pedido_itens").alias("i")
    dim_cliente = spark.read.format("delta").load(str(base_path / "gold" / "dim_cliente")) \
        .filter(col("is_current") == lit(True)) \
        .select("sk_cliente", "id_cliente") \
        .alias("dc")
    dim_produto = spark.read.format("delta").load(str(base_path / "gold" / "dim_produto")) \
        .filter(col("is_current") == lit(True)) \
        .select("sk_produto", "id_produto") \
        .alias("dp")

    path_pagamentos = base_path / "silver" / "pagamentos"
    if tabela_delta_existe(spark, path_pagamentos):
        pagamentos = spark.read.format("delta").load(str(path_pagamentos)) \
            .groupBy("pedido_id") \
            .agg(first("forma_pagamento", ignorenulls=True).alias("forma_pagamento")) \
            .alias("pg")
    else:
        pagamentos = None

    last_value = obter_checkpoint_fato(spark, path_checkpoint)
    pedidos_incrementais = pedidos
    if last_value is not None:
        logger.info(f"Aplicando checkpoint da fato [fato_vendas]: data_pedido > {last_value}")
        pedidos_incrementais = pedidos_incrementais.filter(col("p.data_pedido") > lit(last_value).cast("timestamp"))
    else:
        logger.info("Checkpoint nao encontrado para [fato_vendas]. Processando carga inicial.")

    df_base = pedido_itens.join(
        pedidos_incrementais,
        col("i.pedido_id") == col("p.id"),
        "inner",
    )

    if pagamentos is not None:
        df_base = df_base.join(pagamentos, col("p.id") == col("pg.pedido_id"), "left")
        coluna_forma_pagamento = col("pg.forma_pagamento")
    else:
        coluna_forma_pagamento = lit(None).cast("string")

    df_fato_incremental = df_base.join(
        dim_cliente,
        col("p.usuario_id") == col("dc.id_cliente"),
        "inner",
    ).join(
        dim_produto,
        col("i.produto_id") == col("dp.id_produto"),
        "inner",
    ).select(
        col("i.id").alias("id_pedido_item"),
        col("dc.sk_cliente").alias("sk_cliente"),
        col("dp.sk_produto").alias("sk_produto"),
        date_format(to_date(col("p.data_pedido")), "yyyyMMdd").cast("int").alias("sk_data"),
        col("i.quantidade").alias("quantidade"),
        col("i.preco").alias("preco_unitario"),
        (col("i.quantidade") * col("i.preco")).cast("decimal(18,2)").alias("valor_total"),
        col("p.status").alias("status_pedido"),
        coluna_forma_pagamento.alias("forma_pagamento"),
        col("p.data_pedido").alias("data_pedido"),
    ).cache()

    if df_fato_incremental.limit(1).count() == 0:
        logger.info("Nenhum registro incremental encontrado para a fato [fato_vendas].")
        df_fato_incremental.unpersist()
        return

    novo_checkpoint = df_fato_incremental.agg(spark_max(col("data_pedido")).alias("last_value")).collect()[0]["last_value"]

    if tabela_delta_existe(spark, path_gold):
        df_existente = spark.read.format("delta").load(str(path_gold)).select("id_pedido_item")
        df_fato_incremental = df_fato_incremental.join(df_existente, "id_pedido_item", "left_anti")

    if df_fato_incremental.limit(1).count() == 0:
        logger.info("Registros incrementais ja estavam gravados na fato [fato_vendas].")
        atualizar_checkpoint_fato(spark, path_checkpoint, novo_checkpoint)
        df_fato_incremental.unpersist()
        return

    maior_sk = obter_maior_surrogate(spark, path_gold, "sk_venda")
    window_sk = Window.orderBy(col("data_pedido"), col("id_pedido_item"))

    df_fato = df_fato_incremental \
        .withColumn("sk_venda", (row_number().over(window_sk) + lit(maior_sk)).cast("long")) \
        .select(
            "sk_venda",
            "id_pedido_item",
            "sk_cliente",
            "sk_produto",
            "sk_data",
            "quantidade",
            "preco_unitario",
            "valor_total",
            "status_pedido",
            "forma_pagamento",
        )

    total_fato = df_fato.count()
    modo_gravacao = "append" if tabela_delta_existe(spark, path_gold) else "overwrite"
    df_fato.write \
        .format("delta") \
        .mode(modo_gravacao) \
        .save(str(path_gold))

    df_fato_incremental.unpersist()
    atualizar_checkpoint_fato(spark, path_checkpoint, novo_checkpoint)
    logger.info(f"Fato [fato_vendas] gravada com sucesso. Registros inseridos: {total_fato}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline Spark: Silver to Gold")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data especifica no formato YYYY-MM-DD. A camada Gold usa checkpoint incremental para a fato."
    )
    args = parser.parse_args()

    if args.date:
        logger.info(f"Parametro --date recebido ({args.date}); a carga Gold sera controlada pelo checkpoint.")

    # Caminho do Data Lake
    base_datalake = Path(os.environ.get("DATALAKE_PATH", "datalake"))

    spark = get_spark_session()

    try:
        path_silver_base = base_datalake / "silver"
        if not path_silver_base.exists():
            logger.error(f"Diretorio base da Silver nao existe: {path_silver_base}")
            return

        operacoes = [
            ("dim_data", lambda: processar_dim_data(spark, base_datalake)),
            ("dim_cliente", lambda: processar_dim_cliente(spark, base_datalake)),
            ("dim_produto", lambda: processar_dim_produto(spark, base_datalake)),
            ("fato_vendas", lambda: processar_fato_vendas(spark, base_datalake)),
        ]

        logger.info(f"Tabelas Gold planejadas para processamento: {[nome for nome, _ in operacoes]}")

        for tabela, operacao in operacoes:
            try:
                operacao()
            except Exception as e:
                logger.error(f"Erro ao processar tabela Gold {tabela}: {str(e)}", exc_info=True)

    finally:
        logger.info("Encerrando SparkSession...")
        spark.stop()


if __name__ == "__main__":
    main()
