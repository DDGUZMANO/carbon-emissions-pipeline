import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession

def generar_grafica():
    # 1. Configurar rutas
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    GOLD_PATH = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_trends")
    OUTPUT_IMAGE = os.path.join(BASE_DIR, "carbon_trends_plot.png")

    # 2. Iniciar Spark solo para leer y convertir a Pandas
    spark = SparkSession.builder.appName("Visualizacion").getOrCreate()
    
    print("Cargando datos de la capa Gold...")
    df_spark = spark.read.parquet(GOLD_PATH)
    
    # Convertimos a Pandas porque para 200 filas es más eficiente para graficar
    pdf = df_spark.toPandas()
    spark.stop()

    # 3. Preparar los datos (ordenar por año para la gráfica)
    pdf = pdf.sort_values("year")

    # 4. Configuración Estética (Estilo profesional)
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")
    
    # Crear la gráfica de líneas
    plot = sns.lineplot(
        data=pdf, 
        x="year", 
        y="avg_carbon_intensity", 
        hue="region", 
        marker="o",
        linewidth=2.5
    )

    # 5. Personalización de Títulos y Etiquetas
    plt.title("Evolución de la Intensidad de Carbono por Región (1990-2023)", fontsize=16, pad=20)
    plt.xlabel("Año", fontsize=12)
    plt.ylabel("Intensidad de Carbono (kg CO2 por $ PIB)", fontsize=12)
    plt.legend(title="Regiones", bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Ajustar diseño para que no se corte la leyenda
    plt.tight_layout()

    # 6. Guardar y Mostrar
    plt.savefig(OUTPUT_IMAGE)
    print(f"✅ Gráfica guardada exitosamente como: {OUTPUT_IMAGE}")
    plt.show()

if __name__ == "__main__":
    generar_grafica()