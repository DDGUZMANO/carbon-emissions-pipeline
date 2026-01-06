import streamlit as st
import pandas as pd
import plotly.express as px
import os
import numpy as np

# 1. CONFIGURACIÓN DE LA PÁGINA
st.set_page_config(page_title="Carbon Data Intelligence", layout="wide", initial_sidebar_state="expanded")

# 2. DEFINICIÓN DE RUTAS
PATH_PAISES = "./data/processed/gold/carbon_by_country"
PATH_REGIONES = "./data/processed/gold/carbon_by_region"
PATH_TENDENCIAS = "./data/processed/gold/carbon_trends"

# 3. FUNCIÓN PARA CARGAR DATOS
@st.cache_data
def load_data(path):
    if os.path.exists(path):
        try:
            # Forzamos lectura con pyarrow y reseteamos el índice para recuperar 'year'
            df = pd.read_parquet(path, engine='pyarrow').reset_index()
            # Limpiamos nombres de columnas (minúsculas y sin espacios)
            df.columns = [str(c).lower().strip() for c in df.columns]
            return df
        except Exception as e:
            st.error(f"Error al leer {path}: {e}")
    return None

# Carga de datos
df_paises = load_data(PATH_PAISES)
df_regiones = load_data(PATH_REGIONES)
df_trends = load_data(PATH_TENDENCIAS)

# 4. INTERFAZ DE USUARIO (UI)
st.title("🌍 Carbon Emissions & Economic Intelligence")
st.markdown("---")

if df_paises is not None:
    # --- SIDEBAR: FILTROS ---
    st.sidebar.header("Configuración")
    
    # Verificamos que 'year' existe antes de usarlo
    if 'year' in df_paises.columns:
        year_list = sorted(df_paises['year'].unique(), reverse=True)
        selected_year = st.sidebar.selectbox("Selecciona un año", year_list)
        year_data = df_paises[df_paises['year'] == selected_year]
    else:
        st.error("No se encontró la columna 'year' en los datos.")
        st.stop()

    # --- FILA 1: MÉTRICAS RESUMEN (Usando tus columnas reales) ---
    col1, col2, col3 = st.columns(3)
    
    # Usamos avg_carbon_intensity
    carbon_val = year_data['avg_carbon_intensity'].mean() if 'avg_carbon_intensity' in year_data.columns else 0
    gdp_val = year_data['avg_gdp_per_capita'].mean() if 'avg_gdp_per_capita' in year_data.columns else 0
    
    col1.metric("Intensidad Carbono Promedio", f"{carbon_val:.4f}")
    col2.metric("Países Analizados", len(year_data))
    col3.metric("PIB per Cápita Promedio", f"${gdp_val:,.2f}")

    # --- FILA 2: MAPA Y TOP 10 ---
    st.markdown("---")
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader(f"Distribución Global de Intensidad de Carbono - {selected_year}")
        if 'iso_code' in year_data.columns and 'avg_carbon_intensity' in year_data.columns:

            max_color = year_data['avg_carbon_intensity'].quantile(0.95)
            fig_map = px.choropleth(
                year_data, 
                locations="iso_code", 
                color="avg_carbon_intensity",
                hover_name="country", 
                color_continuous_scale="YlOrRd",
                range_color=[0, max_color], # Esto hace que los países se diferencien mucho más
                title="Intensidad de Carbono por País"
            )
            st.plotly_chart(fig_map, use_container_width=True)
    
    with c2:
        st.subheader("Top 10 Países (Intensidad)")
        if 'avg_carbon_intensity' in year_data.columns:
            top_10 = year_data.nlargest(10, 'avg_carbon_intensity')
            fig_bar = px.bar(
                top_10, 
                x='avg_carbon_intensity', 
                y='country', 
                orientation='h', 
                color='avg_carbon_intensity',
                color_continuous_scale="Reds"
            )
            # Invertimos el eje Y para que el mayor esté arriba
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

# --- FILA 3: ANÁLISIS POR REGIÓN O TENDENCIAS ---
    if df_trends is not None:
        st.markdown("---")
        st.subheader("Evolución Histórica de la Intensidad")
        
        # 🔍 Diagnóstico dinámico para el gráfico de líneas
        # Buscamos una columna que contenga 'intensity' o 'co2'
        cols_disponibles = df_trends.columns.tolist()
        col_y = None
        for c in ['avg_carbon_intensity', 'carbon_intensity', 'co2']:
            if c in cols_disponibles:
                col_y = c
                break
        
        if col_y:
            fig_trends = px.line(
                df_trends, 
                x='year', 
                y=col_y, 
                color='region' if 'region' in cols_disponibles else None,
                title=f"Tendencia de {col_y} a través del tiempo",
                labels={col_y: "Intensidad", "year": "Año"}
            )
            # Mejoramos el diseño del gráfico de líneas
            fig_trends.update_layout(hovermode="x unified")
            st.plotly_chart(fig_trends, use_container_width=True)
        else:
            st.warning(f"No encontré una columna numérica para el gráfico de líneas. Columnas en tendencias: {cols_disponibles}")

# --- FILA 4: ANÁLISIS DE DESACOPLAMIENTO POR REGIÓN ---
st.markdown("---")
st.subheader("📈 Análisis de Desacoplamiento Económico")

if df_trends is not None:
    # 1. Creamos un selector de regiones basado en tus datos
    regiones_disponibles = sorted(df_trends['region'].unique())
    region_seleccionada = st.selectbox("Selecciona una Región para analizar", regiones_disponibles)

    # 2. Filtramos los datos por la región elegida
    df_region = df_trends[df_trends['region'] == region_seleccionada].sort_values('year')

    # 3. Normalizamos los datos (Índice base 100) para que sean comparables
    # Esto permite ver el crecimiento porcentual de ambas variables en la misma escala
    if not df_region.empty:
        first_year_gdp = df_region['avg_gdp_per_capita'].iloc[0]
        first_year_co2 = df_region['avg_carbon_intensity'].iloc[0]
        
        df_region['PIB Normalizado'] = (df_region['avg_gdp_per_capita'] / first_year_gdp) * 100
        df_region['Intensidad Normalizada'] = (df_region['avg_carbon_intensity'] / first_year_co2) * 100

        # 4. Creamos la gráfica con dos líneas
        fig_decoupling = px.line(
            df_region, 
            x='year', 
            y=['PIB Normalizado', 'Intensidad Normalizada'],
            title=f"Crecimiento Económico vs Intensidad de Carbono en {region_seleccionada} (Base 100)",
            labels={"value": "Índice (Base 100)", "year": "Año", "variable": "Indicador"},
            color_discrete_map={
                "PIB Normalizado": "#2ecc71",       # Verde para economía
                "Intensidad Normalizada": "#e74c3c"  # Rojo para carbono
            }
        )

        st.plotly_chart(fig_decoupling, use_container_width=True)
        
        st.info("""
        **¿Cómo leer esta gráfica?** Si la línea verde (PIB) sube mientras la roja (Intensidad) baja o se mantiene plana, 
        estamos ante un **desacoplamiento absoluto**, lo cual es el objetivo de una economía sostenible.
        """)