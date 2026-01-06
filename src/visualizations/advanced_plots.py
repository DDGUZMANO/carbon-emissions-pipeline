import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.common.config_spark import iniciar_contexto

def generar_dashboard_interactivo():
    # 1. Iniciar Spark con tu configuración personalizada
    spark = iniciar_contexto()
    
    # 2. Definir Rutas
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    PATH_COUNTRY = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_by_country")
    PATH_TRENDS = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_trends")
    
    # 3. Cargar Datos y convertir a Pandas
    # Plotly requiere Pandas para las animaciones y la interactividad fluida
    print("📊 Extrayendo datos de las capas Gold...")
    pdf_country = spark.read.parquet(PATH_COUNTRY).toPandas()
    pdf_trends = spark.read.parquet(PATH_TRENDS).toPandas()
    
    # Cerramos Spark ya que el resto es procesamiento local con Plotly
    spark.stop()

    # --- Limpieza para visualización ---
    pdf_country = pdf_country.sort_values(['country', 'year'])
    pdf_trends = pdf_trends.sort_values(['region', 'year'])

    # ==============================================================
    # VISUALIZACIÓN 1: El "Hans Rosling" Bubble Chart (Dinámica Global)
    # ==============================================================
    print("🎬 Generando animación temporal...")
    fig_anim = px.scatter(
        pdf_country, 
        x="avg_gdp_per_capita", 
        y="avg_carbon_intensity",
        animation_frame="year", 
        animation_group="country",
        size="avg_gdp_per_capita", 
        color="region", 
        hover_name="country",
        log_x=True, 
        size_max=45, 
        range_y=[0, 1.3], # Ajustado para captar la mayoría de intensidades
        title="<b>Dinámica Global: PIB vs Intensidad de Carbono (1990-2023)</b><br><sup>Las burbujas bajando indican mayor eficiencia energética</sup>",
        labels={
            "avg_gdp_per_capita": "PIB per Cápita (USD, Escala Log)",
            "avg_carbon_intensity": "Intensidad (kg CO2 / $)"
        },
        template="plotly_white"
    )
    
    # Guardar animación
    fig_anim.write_html(os.path.join(BASE_DIR, "viz_animacion_global.html"))

    # ==============================================================
    # VISUALIZACIÓN 2: Desacoplamiento Regional (Líneas de Tendencia)
    # ==============================================================
    print("📈 Generando comparativa regional...")
    fig_line = px.line(
        pdf_trends, 
        x="year", 
        y="avg_carbon_intensity", 
        color="region",
        line_dash="region",
        title="<b>Evolución de la Eficiencia Energética por Región</b><br><sup>Reducción de emisiones por cada dólar de PIB generado</sup>",
        labels={"avg_carbon_intensity": "Intensidad de Carbono", "year": "Año"},
        template="plotly_dark"
    )
    
    fig_line.update_layout(hovermode="x unified")
    fig_line.write_html(os.path.join(BASE_DIR, "viz_tendencias_regionales.html"))

    # ==============================================================
    # VISUALIZACIÓN 3: Heatmap de Eficiencia (Año Actual)
    # ==============================================================
    print("🌍 Generando mapa de calor de eficiencia...")
    df_2023 = pdf_country[pdf_country['year'] == 2023]
    
    fig_map = px.choropleth(
        df_2023,
        locations="iso_code",
        color="avg_carbon_intensity",
        hover_name="country",
        color_continuous_scale=px.colors.sequential.Viridis_r, # _r para que el verde sea "bueno"
        title="<b>Mapa Global de Intensidad de Carbono (2023)</b>",
        labels={"avg_carbon_intensity": "Intensidad CO2"},
        template="plotly_white"
    )
    
    fig_map.write_html(os.path.join(BASE_DIR, "viz_mapa_global_2023.html"))

    print("\n" + "="*50)
    print("✅ DASHBOARD GENERADO EXITOSAMENTE")
    print(f"Archivos creados en: {BASE_DIR}")
    print("- viz_animacion_global.html (¡Abre este primero!)")
    print("- viz_tendencias_regionales.html")
    print("- viz_mapa_global_2023.html")
    print("="*50)

if __name__ == "__main__":
    generar_dashboard_interactivo()