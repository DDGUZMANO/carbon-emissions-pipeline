import os
from pyspark.sql import functions as F
from src.common.config_spark import iniciar_contexto


def crear_gold_por_region():
    print("🏆 Generando Capa Gold: Análisis por Región Geográfica...")
    # 1. Spark
    spark = iniciar_contexto()

    # 2. Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    SILVER_PATH = os.path.join(BASE_DIR, "data", "processed", "silver", "carbon_intensity")
    GOLD_PATH = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_by_region")

    # 3. Leer SILVER
    df = spark.read.parquet(SILVER_PATH)

    # =============================================================
    # NUEVA LÓGICA: Propagar la región a todos los años
    # =============================================================

    # A. Creamos un diccionario de ISO_CODE -> REGION (solo donde no es Unknown)
    mapping_region = (  
    df.filter(F.col("region") != "Unknown")
    .select("iso_code", "region")
    .distinct()
    )

    # B. Quitamos la columna 'region' original (la que tiene nulos/unknowns)
    df_clean = df.drop("region")

    # C. Hacemos un JOIN para que cada país recupere su región en cada año de la historia
    df_final = df_clean.join(mapping_region, on="iso_code", how="inner")

    # =============================================================
    # 4. Agregación GOLD (Ahora con todos los años recuperados)
    # =============================================================
    gold_region = (
        df_final.groupBy("year", "region")
          .agg(
              F.avg("carbon_intensity").alias("avg_carbon_intensity"),
              F.avg("gdp_per_capita").alias("avg_gdp_per_capita"),
              F.countDistinct("iso_code").alias("countries_count")
          )
          .orderBy("year", "region") # Ordenamos para que el .show() sea legible
    )

    # Validación rápida en consola
    print("\n=== RESUMEN GOLD POR DÉCADA (Primeras filas) ===")
    gold_region.show(20)

    print(f"Total de filas en Gold: {gold_region.count()}")

    # 5. Escribir GOLD
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        gold_region.write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(GOLD_PATH)
    )

    print(f"\n✅ Datos guardados exitosamente en: {GOLD_PATH}")

    spark.stop()

if __name__ == "__main__":
    crear_gold_por_region()