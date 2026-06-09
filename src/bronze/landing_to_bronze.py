"""Camada Bronze - Limpeza e padronização (Etapa 4).

Lê os arquivos brutos da camada Landing (CSV), aplica padronização e
limpeza básica e grava o resultado na camada Bronze em formato Parquet,
preservando a rastreabilidade da origem.

Refs: issue #6
"""

from __future__ import annotations

import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

LANDING_DIR = Path("data/landing")
BRONZE_DIR = Path("data/bronze")


def padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas: minúsculo, sem acento, snake_case."""

    def normalizar(nome: str) -> str:
        nome = unicodedata.normalize("NFKD", nome)
        nome = nome.encode("ascii", "ignore").decode("ascii")
        return nome.strip().lower().replace(" ", "_")

    return df.rename(columns={c: normalizar(c) for c in df.columns})


def limpar(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicatas e registros totalmente vazios."""
    df = df.drop_duplicates()
    df = df.dropna(how="all")
    return df


def processar_arquivo(caminho_csv: Path) -> Path:
    """Converte um CSV da Landing em Parquet na Bronze."""
    df = pd.read_csv(caminho_csv, encoding="utf-8")
    df = padronizar_colunas(df)
    df = limpar(df)

    # Rastreabilidade da origem
    df["_arquivo_origem"] = caminho_csv.name
    df["_data_ingestao"] = datetime.now(timezone.utc).isoformat()

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    destino = BRONZE_DIR / f"{caminho_csv.stem}.parquet"
    df.to_parquet(destino, index=False)
    return destino


def main() -> None:
    arquivos = sorted(LANDING_DIR.glob("*.csv"))
    if not arquivos:
        print(f"Nenhum CSV encontrado em {LANDING_DIR}/")
        return

    for csv in arquivos:
        destino = processar_arquivo(csv)
        print(f"{csv.name} -> {destino}")


if __name__ == "__main__":
    main()
