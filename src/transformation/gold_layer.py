# Archivo: src/transformation/gold_layer.py

import sys

# Importamos las funciones específicas de cada archivo dentro de la misma carpeta
try:
    from src.transformation.gold_carbon_by_country import crear_gold_por_pais
    from src.transformation.gold_carbon_by_region import crear_gold_por_region
    from src.transformation.gold_carbon_trends import crear_gold_por_tendencias
except ImportError as e:
    print(f"❌ Error al importar los módulos de transformación: {e}")
    sys.exit(1)

def ejecutar_gold():
    """
    Orquestador de la Capa Gold.
    Centraliza la ejecución de todas las tablas de hechos y dimensiones
    finales para el consumo del dashboard.
    """
    print("\n" + "="*50)
    print("🏆 INICIANDO PROCESAMIENTO DE CAPA GOLD")
    print("="*50)

    try:
        # 1. Procesar agregaciones por País
        print("\n[Sub-paso 3.1] Generando métricas por país...")
        crear_gold_por_pais()

        # 2. Procesar agregaciones por Región
        print("\n[Sub-paso 3.2] Generando métricas por región...")
        crear_gold_por_region()

        # 3. Procesar análisis de tendencias
        print("\n[Sub-paso 3.3] Generando análisis de tendencias...")
        crear_gold_por_tendencias()

        print("\n✅ CAPA GOLD COMPLETADA EXITOSAMENTE")
        print("="*50)

    except Exception as e:
        print(f"❌ Error en la orquestación de la capa Gold: {e}")
        raise e  # Re-lanzamos el error para que el main.py también sepa que falló

if __name__ == "__main__":
    # Esto permite ejecutar toda la capa Gold de forma independiente para pruebas
    ejecutar_gold()