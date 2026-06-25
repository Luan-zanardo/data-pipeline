"""Módulo de utilidades para as transformações Spark.

Este módulo centraliza a criação e configuração da SparkSession, garantindo
que todos os scripts Spark no projeto usem uma configuração consistente e
de fácil manutenção.
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """Cria e retorna uma SparkSession configurada para o projeto.

    A função configura a sessão com suporte ao Delta Lake e ao S3 (MinIO),
    lendo as configurações de pacotes e credenciais a partir das variáveis
    de ambiente.

    Args:
        app_name: O nome da aplicação Spark.

    Returns:
        Uma instância configurada da SparkSession.
    """
    # Lê as configurações de pacotes JAR do ambiente.
    # Ex: "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3"
    packages = os.environ.get("AIRFLOW__SPARK__PACKAGES")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if packages:
        builder = builder.config("spark.jars.packages", packages)

    # Configurações para o S3 (MinIO)
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
    if s3_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    return builder.getOrCreate()
