import os
from pyspark.sql import functions as F
from src.common.config_spark import iniciar_contexto

def crear_gold_por_pais():

    # 1. Iniciar Spark
    spark = iniciar_contexto()

    # 2. Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    SILVER_PATH = os.path.join(BASE_DIR, "data", "processed", "silver", "carbon_intensity")
    GOLD_PATH = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_by_country")

    # 3. Leer SILVER
    df = spark.read.parquet(SILVER_PATH)

    # =============================================================
    # 4. PROPAGACIÓN DE REGIÓN (Para no perder años antiguos)
    # =============================================================
    # Creamos el mapa de ISO -> Región (donde sí se conoce)
    mapping_region = (
        df.filter(F.col("region") != "Unknown")
        .select("iso_code", "region")
        .distinct()
    )

    # Unimos para recuperar la región en todos los años
    df_final = (
        df.drop("region") # Borramos la columna con Unknowns
        .join(mapping_region, on="iso_code", how="inner") # Recuperamos la región real
    )

    # =============================================================
    # 5. Agregación por país y año
    # =============================================================
    gold_country = (
        df_final.groupBy("year", "country", "iso_code", "region")
          .agg(
              F.avg("carbon_intensity").alias("avg_carbon_intensity"),
              F.avg("gdp_per_capita").alias("avg_gdp_per_capita")
          )
          .orderBy("year", "country")
    )

    # 6. Escribir GOLD
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        gold_country
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(GOLD_PATH)
    )

    print(f"✅ Gold por país guardado en: {GOLD_PATH}")
    print(f"Total de registros: {gold_country.count()}")

    spark.stop()

if __name__ == "__main__":
    crear_gold_por_pais()