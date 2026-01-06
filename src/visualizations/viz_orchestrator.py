# Archivo: src/visualizations/viz_orchestrator.py
import sys

# Importamos tus funciones reales desde tus archivos reales
try:
    # 1. El que acabas de enviar
    from src.visualizations.advanced_plots import generar_dashboard_interactivo
    
    # 2. El del dashboard con pestañas de regiones
    from src.visualizations.plot_interactive_regions import generar_dashboard_pestanas
    
    # 3. El de Seaborn/Matplotlib
    from src.visualizations.plot_trends import generar_grafica

except ImportError as e:
    print(f"❌ Error al importar las funciones de visualización: {e}")
    sys.exit(1)

def ejecutar_visualizaciones():
    """
    Orquestador de la Capa de Visualización.
    Llama a todas las funciones de plotting de forma secuencial.
    """
    print("\n" + "="*50)
    print("🎨 INICIANDO GENERACIÓN DE DASHBOARDS Y GRÁFICOS")
    print("="*50)

    try:
        # Ejecución del Dashboard Avanzado (Burbujas, Mapas, Líneas)
        print("\n[Viz 1/3] Generando Dashboard Animado y Mapas (Plotly Express)...")
        generar_dashboard_interactivo()

        # Ejecución del Dashboard de Pestañas
        print("\n[Viz 2/3] Generando Dashboard de Desacoplamiento (Plotly Tabs)...")
        generar_dashboard_pestanas()

        # Ejecución de la gráfica estática
        print("\n[Viz 3/3] Generando Reporte de Tendencias Estático (Seaborn)...")
        generar_grafica()

        print("\n✅ TODA LA CAPA DE VISUALIZACIÓN HA SIDO COMPLETADA")
        print("="*50)

    except Exception as e:
        print(f"❌ Error durante la ejecución de las visualizaciones: {e}")
        raise e

if __name__ == "__main__":
    ejecutar_visualizaciones()