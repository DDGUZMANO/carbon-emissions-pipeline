# 1. Usamos una imagen de Python oficial moderna y ligera
# Usamos Python 3.11 que es compatible con bleach 6.3.0
FROM python:3.11-bullseye

# Instalamos Java 11 (Versión estable y compatible con Spark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Definimos variables de entorno para Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# EXPOSICIÓN DEL PUERTO DE STREAMLIT
EXPOSE 8501

# Comando: Primero procesa los datos (Spark) y LUEGO lanza el Dashboard
CMD ["sh", "-c", "python main.py && streamlit run app_dashboard.py --server.port=8501 --server.address=0.0.0.0"]