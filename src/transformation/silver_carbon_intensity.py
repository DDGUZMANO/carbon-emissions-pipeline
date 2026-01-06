import os
from datetime import datetime
from pyspark.sql import functions as F
from src.common.config_spark import iniciar_contexto


def ejecutar_silver():
    # ===============================
    # 0. Constantes
    # ===============================
    MIN_YEAR = 1960
    MAX_YEAR = datetime.now().year

    # ===============================
    # 1. Iniciar Spark
    # ===============================
    spark = iniciar_contexto()

    # ===============================
    # 2. Rutas
    # ===============================
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    BRONZE_PATH = os.path.join(
        BASE_DIR,
        "data",
        "processed",
        "bronze",
        "carbon_intensity"
    )

    SILVER_PATH = os.path.join(
        BASE_DIR,
        "data",
        "processed",
        "silver",
        "carbon_intensity"
    )

    print(f"Leyendo BRONZE desde: {BRONZE_PATH}")

    # ===============================
    # 3. Leer BRONZE
    # ===============================
    df = spark.read.parquet(BRONZE_PATH)

    # ===============================
    # 4. Renombrar columnas
    # ===============================
    df_silver = (
        df
        .withColumnRenamed("Entity", "country")
        .withColumnRenamed("Code", "iso_code")
        .withColumnRenamed("Year", "year")
        .withColumnRenamed("Carbon intensity of GDP (kg CO2e per 2021 PPP $)", "carbon_intensity")
        .withColumnRenamed("GDP per capita, PPP (constant 2021 international $)", "gdp_per_capita")
        .withColumnRenamed("World region according to OWID", "region")
    )

    # ===============================
    # 5. Cast de tipos
    # ===============================
    df_silver = (
        df_silver
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("carbon_intensity", F.col("carbon_intensity").cast("double"))
        .withColumn("gdp_per_capita", F.col("gdp_per_capita").cast("double"))
    )

    # ===============================
    # 6. Limpieza de datos
    # ===============================
    df_silver = (
        df_silver
        .filter(F.col("iso_code").isNotNull())
        .filter(F.col("year").isNotNull())
        .filter(F.col("carbon_intensity").isNotNull())
        .filter(F.col("gdp_per_capita").isNotNull())
        # .filter(F.col("region").isNotNull())
    )

    df_silver = df_silver.fillna({"region": "Unknown"})

    # ===============================
    # 7. Validación de rango de años
    # ===============================
    df_silver = df_silver.filter(
        (F.col("year") >= MIN_YEAR) & (F.col("year") <= MAX_YEAR)
    )

    print("\n=== VALIDACIÓN DE AÑOS ===")
    df_silver.select("year").summary("min", "max").show()

    # ===============================
    # 8. Exploración rápida
    # ===============================
    print("\n=== SCHEMA SILVER ===")
    df_silver.printSchema()

    print("\n=== FILAS SILVER ===")
    print(df_silver.count())

    df_silver.show(10, truncate=False)

    # ===============================
    # 9. Guardar SILVER (PARTICIONADO)
    # ===============================
    print(f"\nGuardando datos en SILVER: {SILVER_PATH}")

    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

    (
        df_silver
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(SILVER_PATH)
    )

    # ===============================
    # 10. Cerrar Spark
    # ===============================
    spark.stop()
    print("✅ Capa Silver completada con éxito.")
    
# Mantenemos esto por si quieres ejecutar solo este archivo
if __name__ == "__main__":
    ejecutar_silver()