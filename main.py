import sys
import time
import os

# 1. IMPORTACIONES
try:
    from src.ingestion.ingest_owid import ejecutar_ingesta
    from src.transformation.silver_carbon_intensity import ejecutar_silver
    from src.transformation.gold_layer import ejecutar_gold 
    from src.visualizations.viz_orchestrator import ejecutar_visualizaciones
except ImportError as e:
    print(f"\n❌ ERROR DE IMPORTACIÓN: Verifica las rutas de tus archivos.")
    print(f"👉 {e}")
    sys.exit(1)

def run_pipeline():
    inicio_total = time.time()
    
    print("\n" + "="*70)
    print("🚀 INICIANDO PIPELINE DE EMISIONES DE CARBONO (MEDALLION ARCHITECTURE)")
    print("="*70)

    try:
        # --- PASO 1: BRONZE ---
        print("\n🔹 [PASO 1/4] Ejecutando Capa BRONZE (Ingesta)...")
        ejecutar_ingesta()
        print("✅ Capa Bronze completada correctamente.")

        # --- PASO 2: SILVER ---
        print("\n🔹 [PASO 2/4] Ejecutando Capa SILVER (Limpieza)...")
        ejecutar_silver()
        print("✅ Capa Silver completada correctamente.")

        # --- PASO 3: GOLD ---
        print("\n🔹 [PASO 3/4] Ejecutando Capa GOLD (Transformación de Negocio)...")
        ejecutar_gold()
        print("✅ Capa Gold completada correctamente.")

        # --- PASO 4: VISUALIZACIÓN ---
        print("\n🔹 [PASO 4/4] Ejecutando Capa de VISUALIZACIÓN (Reportes)...")
        ejecutar_visualizaciones()
        print("✅ Todos los reportes han sido generados.")

        # --- RESUMEN FINAL ---
        fin_total = time.time()
        tiempo_total = round(fin_total - inicio_total, 2)
        
        print("\n" + "="*70)
        print(f"🎉 ¡ÉXITO! El pipeline ha finalizado sin errores.")
        print(f"⏱️ Tiempo total de ejecución: {tiempo_total} segundos")
        print("="*70)

    except Exception as e:
        # Este bloque captura CUALQUIER error en cualquiera de los pasos anteriores
        print("\n" + "!"*70)
        print(f"❌ ERROR CRÍTICO DETECTADO")
        print(f"📍 El pipeline se detuvo.")
        print(f"👉 Motivo: {str(e)}")
        print("!"*70)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()