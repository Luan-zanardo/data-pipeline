# 1. Começamos com a imagem oficial do Airflow
FROM apache/airflow:2.10.5-python3.11

# 2. Trocamos para o usuário root para instalar dependências do sistema
USER root

# Instala o OpenJDK (necessário para o Spark) e o procps (para o comando 'ps')
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean

# Define a variável de ambiente JAVA_HOME para o Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Trocamos de volta para o usuário padrão do Airflow
USER airflow

# 4. Copiamos o arquivo de requisitos para dentro da imagem
COPY requirements.txt /requirements.txt

# 5. Instalamos as dependências do Python a partir do arquivo de requisitos
RUN pip install --no-cache-dir -r /requirements.txt
