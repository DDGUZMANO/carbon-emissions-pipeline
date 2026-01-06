import os
import sys
import platform
from pyspark.sql import SparkSession

def iniciar_contexto():
    print("✔ Detectando sistema operativo...")
    
    # Flags de seguridad para Java 17+ (Obligatorios para Docker/Linux moderno)
    # Estos permiten que Spark acceda a la memoria interna de Java
    java_flags = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
    )

    # 1. Configuración para LINUX (DOCKER)
    if platform.system() == "Linux":
        java_home = "/usr/lib/jvm/java-11-openjdk-amd64"
        os.environ["JAVA_HOME"] = java_home
        os.environ["PYSPARK_PYTHON"] = "python3"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
        os.environ["PATH"] = f"{java_home}/bin:" + os.environ["PATH"]
        warehouse_path = "file:///tmp/spark-warehouse"

    # 2. Configuración para WINDOWS (Local)
    else:
        java_home = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
        hadoop_home = r"C:\hadoop"
        os.environ["JAVA_HOME"] = java_home
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["PATH"] = f"{java_home}\\bin;{hadoop_home}\\bin;" + os.environ["PATH"]
        warehouse_path = "file:///C:/temp"

    print(f"✔ Iniciando SparkSession en {platform.system()}...")

    return SparkSession.builder \
        .appName("CarbonPipeline") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", warehouse_path) \
        .config("spark.driver.extraJavaOptions", java_flags) \
        .config("spark.executor.extraJavaOptions", java_flags) \
        .getOrCreate()