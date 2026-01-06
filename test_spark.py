import os
from config_spark import iniciar_contexto  # <--- Importamos tu configuración funcional

# 1. Iniciamos Spark con todas las rutas de Java/Hadoop corregidas
spark = iniciar_contexto()

# 2. Tu lógica de datos
data = [
    ("Spain", 2023, 2.1),
    ("Germany", 2023, 1.8),
    ("France", 2023, 1.5),
]

df = spark.createDataFrame(data, ["country", "year", "carbon_intensity"])
df.show()

# 3. Cerramos sesión
spark.stop()