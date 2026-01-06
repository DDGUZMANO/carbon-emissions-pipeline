import os
from pyspark.sql import functions as F
from src.common.config_spark import iniciar_contexto


def crear_gold_por_tendencias():
    print("🏆 Generando Capa Gold: Análisis de Tendencias Temporales...")
    # =================================================================
    # 1. Iniciar Spark
    # =================================================================
    spark = iniciar_contexto()

    # =================================================================
    # 2. Rutas (Paths)
    # =================================================================
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

    SILVER_PATH = os.path.join(
        BASE_DIR, 
        "data", "processed", "silver", "carbon_intensity"
    )

    GOLD_PATH = os.path.join(
        BASE_DIR, 
        "data", "processed", "gold", "carbon_trends"
    )

    print(f"Leyendo datos desde Silver: {SILVER_PATH}")

    # =================================================================
    # 3. Leer Capa SILVER
    # =================================================================
    df_silver = spark.read.parquet(SILVER_PATH)

    # =================================================================
    # 4. Propagación de Región (Data Enrichment)
    # =================================================================
    # Encontramos la relación única entre país y región (usando datos de 2023)
    # para aplicarla a los años donde la región aparece como 'Unknown'.
    mapping_region = (
        df_silver.filter(F.col("region") != "Unknown")
        .select("iso_code", "region")
        .distinct()
    )

    # Quitamos la columna region con 'Unknowns' y pegamos la región real
    df_trends = (
        df_silver.drop("region")
        .join(mapping_region, on="iso_code", how="inner")
    )

    # =================================================================
    # 5. Agregación Temporal para Tendencias
    # =================================================================
    gold_trends = (
        df_trends.groupBy("year", "region")
          .agg(
              F.avg("carbon_intensity").alias("avg_carbon_intensity"),
              F.avg("gdp_per_capita").alias("avg_gdp_per_capita"),
              F.countDistinct("iso_code").alias("countries_count")
          )
          .orderBy("year", "region")
    )

    # =================================================================
    # 6. Guardar Resultados en GOLD
    # =================================================================
    print(f"Guardando tendencias en Gold: {GOLD_PATH}")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        gold_trends.write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(GOLD_PATH)
    )

    # =================================================================
    # 7. Resumen Final en Consola
    # =================================================================
    print("\n=== VISTA PREVIA DE TENDENCIAS (GOLD) ===")
    gold_trends.show(20, truncate=False)

    print(f"\n✅ Proceso terminado. Total de filas generadas: {gold_trends.count()}")

    # Opcional: Si quieres un CSV único para graficar en Excel rápidamente
    # gold_trends.toPandas().to_csv("tendencias_carbono.csv", index=False)

    spark.stop()

if __name__ == "__main__":
    crear_gold_por_tendencias()