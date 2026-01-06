import os
from pyspark.sql import functions as F
from pyspark.sql import Window
from src.common.config_spark import iniciar_contexto

def generar_reporte_total():
    spark = iniciar_contexto()
    
    # 1. Definir Rutas
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    PATH_COUNTRY = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_by_country")
    PATH_REGION = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_by_region")
    PATH_TRENDS = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_trends")
    
    # 2. Cargar Datasets
    df_c = spark.read.parquet(PATH_COUNTRY)
    df_r = spark.read.parquet(PATH_REGION)
    df_t = spark.read.parquet(PATH_TRENDS)

    print("\n" + "="*80)
    print("   ANÁLISIS INTEGRADO: COUNTRY + REGION + TRENDS (1990-2023)")
    print("="*80)

    # --- A. ¿Quién domina la tendencia regional? (Trends + Country) ---
    # Queremos ver qué país es el más eficiente de la región más eficiente
    print("\n[A] LÍDERES DE EFICIENCIA DENTRO DE CADA REGIÓN (Datos 2023)")
    
    window_eff = Window.partitionBy("region").orderBy("avg_carbon_intensity")
    
    # Cruzamos el detalle de país con la jerarquía de región
    lideres_regionales = df_c.filter(F.col("year") == 2023) \
        .withColumn("rank", F.row_number().over(window_eff)) \
        .filter(F.col("rank") == 1) \
        .select("region", "country", "avg_carbon_intensity", "avg_gdp_per_capita")
    
    lideres_regionales.orderBy("avg_carbon_intensity").show()

    # --- B. Desacoplamiento Regional vs Nacional (Trends + Region) ---
    # Comparamos si la región mejoró gracias a un aumento masivo de PIB o reducción de CO2
    print("\n[B] MÉTRICAS DE IMPACTO REGIONAL (Histórico 1990-2023)")
    
    # Usamos el dataset de TRENDS que ya tiene la agregación temporal lista
    stats_reg = df_t.filter(F.col("year").isin(1990, 2023)) \
        .groupBy("region") \
        .agg(
            F.first("avg_carbon_intensity").alias("int_1990"),
            F.last("avg_carbon_intensity").alias("int_2023"),
            F.first("avg_gdp_per_capita").alias("pib_1990"),
            F.last("avg_gdp_per_capita").alias("pib_2023")
        ) \
        .withColumn("mejora_eficiencia_%", ((F.col("int_1990") - F.col("int_2023")) / F.col("int_1990")) * 100) \
        .withColumn("crecimiento_pib_%", ((F.col("pib_2023") - F.col("pib_1990")) / F.col("pib_1990")) * 100)

    stats_reg.select("region", 
                     F.round("mejora_eficiencia_%", 2).alias("Reduccion_CO2_%"),
                     F.round("crecimiento_pib_%", 2).alias("Crecimiento_PIB_%")) \
             .orderBy(F.desc("Reduccion_CO2_%")).show()

    # --- C. Validación de Calidad (Region + Country) ---
    # ¿Cuántos países sustentan el dato de cada región?
    print("\n[C] DENSIDAD DE DATOS POR REGIÓN")
    df_r.filter(F.col("year") == 2023) \
        .select("region", "countries_count") \
        .orderBy(F.desc("countries_count")).show()

    # --- D. El Gran Hallazgo: Países "Contracorriente" ---
    # Países que empeoraron su intensidad mientras su región mejoraba
    print("\n[D] OUTLIERS CRÍTICOS: Empeoran mientras su región mejora")
    
    # 1. Obtener tendencia regional
    reg_trends = stats_reg.select("region", F.col("mejora_eficiencia_%").alias("reg_improvement"))
    
    # 2. Obtener tendencia país
    window_c = Window.partitionBy("country").orderBy("year")
    country_trends = df_c.filter(F.col("year").isin(1990, 2023)) \
        .withColumn("prev_int", F.lag("avg_carbon_intensity").over(window_c)) \
        .filter(F.col("year") == 2023) \
        .withColumn("c_improvement", ((F.col("prev_int") - F.col("avg_carbon_intensity")) / F.col("prev_int")) * 100) \
        .join(reg_trends, "region") \
        .filter(F.col("c_improvement") < 0) # El país empeoró (mejora negativa)
    
    country_trends.select("country", "region", 
                          F.round("c_improvement", 2).alias("Cambio_Pais_%"), 
                          F.round("reg_improvement", 2).alias("Cambio_Region_%")) \
                  .orderBy("c_improvement").show(10)

    print("\n" + "="*80)
    print("✅ ANÁLISIS TRIPLE COMPLETADO")
    print("="*80)
    spark.stop()

if __name__ == "__main__":
    generar_reporte_total()