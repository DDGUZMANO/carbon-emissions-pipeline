import os
from src.common.config_spark import iniciar_contexto


def ejecutar_ingesta():
    print("🚀 Iniciando Ingesta desde OWID...")
    #iniciamos spark

    spark = iniciar_contexto()

    #ruta al dataset raw

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    DATA_RAW_PATH = os.path.join(
        BASE_DIR,
        "data",
        "raw",
        "carbon_intensity_vs_gdp.csv"
    )

    print(f"Leyendo datos desde: {DATA_RAW_PATH}")

    # 3. Leer CSV OWID
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(DATA_RAW_PATH)
    )

    # 4. Exploración técnica básica
    print("\n=== SCHEMA ===")
    df.printSchema()

    print("\n=== NÚMERO DE FILAS ===")
    print(df.count())

    print("\n=== COLUMNAS ===")
    print(df.columns)

    print("\n=== MUESTRA DE DATOS ===")
    df.show(10, truncate=False)

    #crear carpeta bronze

    BRONZE_PATH = os.path.join(
        BASE_DIR,
        "data",
        "processed",
        "bronze",
        "carbon_intensity"
    )

    print(f"\nGuardando datos en BRONZE: {BRONZE_PATH}")

    (
        df.write
        .mode("overwrite")
        .parquet(BRONZE_PATH)
    )

    # 5. Cerrar Spark
    spark.stop()

# Esto permite ejecutar el script de forma independiente para pruebas
if __name__ == "__main__":
    ejecutar_ingesta()