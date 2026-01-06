# 🚀 Guía de Inicio: Pipeline de Emisiones

Sigue estos dos pasos cada vez que abras una nueva terminal en VS Code. Esto asegura que Spark encuentre Java, Hadoop y el Python de tu entorno virtual.

---

## 🔹 PASO 1: Activar el Entorno Virtual (venv)
Este paso le dice a tu ordenador que use las librerías instaladas en tu proyecto y no las globales del sistema.

1. Abre la terminal en VS Code.
2. Escribe y pulsa **Enter**:
   ```powershell
   .\venv\Scripts\activate

## Abrir python y ejecutar el siguiente

from config_spark import iniciar_contexto

# Esto activa Java, Hadoop y la sesión de Spark
spark = iniciar_contexto()