import os
import pandas as pd
import plotly.graph_objects as go
from src.common.config_spark import iniciar_contexto

def generar_dashboard_pestanas():
    # 1. Iniciar Spark y cargar datos
    spark = iniciar_contexto()
    
    # Definir rutas
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    PATH_TRENDS = os.path.join(BASE_DIR, "data", "processed", "gold", "carbon_trends")
    
    print("📊 Cargando datos desde la capa Gold...")
    # Leemos de Spark y pasamos a Pandas
    pdf_trends = spark.read.parquet(PATH_TRENDS).toPandas()
    spark.stop()

    # 2. Preparar la figura
    fig = go.Figure()
    regiones = sorted(pdf_trends['region'].unique())
    
    # 3. Añadir los trazos (PIB e Intensidad) para CADA región
    # Al principio los ponemos todos como invisibles
    for region in regiones:
        df_reg = pdf_trends[pdf_trends['region'] == region].sort_values('year')
        
        # Trazo de PIB (Eje Y1 - Izquierdo)
        fig.add_trace(go.Scatter(
            x=df_reg['year'], 
            y=df_reg['avg_gdp_per_capita'],
            name=f"PIB - {region}", 
            line=dict(color='#2ecc71', width=4),
            visible=False,
            hovertemplate="Año: %{x}<br>PIB: $%{y:,.0f}<extra></extra>"
        ))
        
        # Trazo de Intensidad (Eje Y2 - Derecho)
        fig.add_trace(go.Scatter(
            x=df_reg['year'], 
            y=df_reg['avg_carbon_intensity'],
            name=f"Intensidad - {region}", 
            line=dict(color='#e74c3c', width=4, dash='dot'),
            visible=False, 
            yaxis="y2",
            hovertemplate="Año: %{x}<br>Intensidad: %{y:.4f} kg/$<extra></extra>"
        ))

    # 4. Hacer visible la primera región por defecto (los dos primeros trazos)
    if len(fig.data) >= 2:
        fig.data[0].visible = True
        fig.data[1].visible = True

    # 5. Crear los "Botones" de navegación (Pestañas)
    botones = []
    for i, region in enumerate(regiones):
        # Creamos una lista de visibilidad: todos False excepto los 2 trazos de la región i
        visibilidad = [False] * len(fig.data)
        visibilidad[i*2] = True
        visibilidad[i*2 + 1] = True
        
        botones.append(dict(
            label=region,
            method="update",
            args=[{"visible": visibilidad},
                  {"title": f"<b>Desacoplamiento Económico: {region}</b>"}]
        ))

    # 6. Configurar el Layout con doble eje y el menú (CORREGIDO)
    fig.update_layout(
        updatemenus=[dict(
            active=0,
            buttons=botones,
            x=0.1, y=1.2,
            xanchor="left", yanchor="top",
            direction="down",
            showactive=True,
            bgcolor="#f8f9fa",
            bordercolor="#ced4da"
        )],
        # Eje Y Primario (PIB)
        yaxis=dict(
            title=dict(
                text="<b>PIB per Cápita (USD)</b>", 
                font=dict(color="#2ecc71", size=14)
            ),
            tickfont=dict(color="#2ecc71"),
            gridcolor="#f0f0f0"
        ),
        # Eje Y Secundario (Intensidad de Carbono)
        yaxis2=dict(
            title=dict(
                text="<b>Intensidad CO2 (kg/$ PIB)</b>", 
                font=dict(color="#e74c3c", size=14)
            ),
            tickfont=dict(color="#e74c3c"),
            overlaying="y",
            side="right",
            showgrid=False # Para no ensuciar la gráfica con dos rejillas
        ),
        template="plotly_white",
        title=dict(
            text=f"<b>Desacoplamiento Económico: {regiones[0]}</b>",
            x=0.5,
            xanchor="center",
            font=dict(size=20)
        ),
        hovermode="x unified",
        margin=dict(t=150, b=50, l=80, r=80),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
    )

    # 7. Guardar
    output_html = os.path.join(BASE_DIR, "dashboard_regiones_interactivo.html")
    fig.write_html(output_html)
    print(f"\n" + "="*50)
    print(f"✅ Dashboard generado exitosamente en:\n   {output_html}")
    print("="*50)

if __name__ == "__main__":
    generar_dashboard_pestanas()