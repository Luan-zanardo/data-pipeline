# Como Executar

## Pré-requisitos

- Python 3.10+
- Java 8/11 (necessário para o Apache Spark)
- Acesso ao object storage do Data Lake e ao banco relacional

## Instalação

```bash
git clone https://github.com/Luan-zanardo/data-pipeline.git
cd data-pipeline
pip install -r requirements.txt
```

## Executando o pipeline

A execução segue a ordem das camadas medalhão:

```bash
# 1. Geração de massa de dados (Etapa 2)
python src/origem/gerar_dados.py

# 2. Ingestão na Landing (Etapa 3 - orquestrada)
#    Disparada pela ferramenta de orquestração (Docker/Cloud) ou localmente
#    (você pode usar o script auxiliar para testar offline):
python generate_mock_landing.py

# 3. Transformação Landing -> Bronze (Etapa 4)
.venv/bin/python src/spark/landing_to_bronze.py

# 4. Transformação Bronze -> Silver (Etapa 4)
.venv/bin/python src/spark/bronze_to_silver.py

# 5. Modelagem e carga Gold (Etapa 5)
python src/gold/silver_to_gold.py
```

!!! note "Observação"
    Os caminhos e scripts acima refletem o desenho do pipeline. Cada etapa é
    detalhada na issue correspondente e este guia é atualizado conforme as
    etapas são concluídas.

## Visualização da documentação

```bash
pip install mkdocs-material
mkdocs serve   # http://127.0.0.1:8000
```
