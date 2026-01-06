# 🌍 Carbon Emissions Data Pipeline (Medallion Architecture)

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.4.0-orange.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.32-ff4b4b.svg)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-green.svg)

Este proyecto implementa un pipeline de datos **End-to-End** para analizar las emisiones de carbono globales y su relación con el crecimiento económico (PIB/GDP). Utiliza una **Arquitectura Medallion** procesada con **PySpark** y desplegada íntegramente mediante contenedores **Docker**.



## 🎯 Objetivo del Proyecto
Extraer, transformar y visualizar datos históricos de emisiones de $CO_2$ para identificar patrones de **"desacoplamiento económico"**: países que logran aumentar su riqueza (PIB) mientras reducen simultáneamente su huella de carbono.

## 🏗️ Arquitectura Técnica
El pipeline se divide en tres capas lógicas para asegurar la integridad, calidad y trazabilidad del dato:

* **Capa Bronze (Ingesta):** Extracción de datos crudos desde fuentes externas (OWID) y persistencia en formato bruto.
* **Capa Silver (Limpieza):** Filtrado de valores nulos, normalización de esquemas, tipado de datos y validación de códigos ISO de países.
* **Capa Gold (Negocio):** Agregaciones complejas por regiones geográficas, décadas y cálculo de métricas de **Intensidad de Carbono** (Emisiones / PIB).
* **Visualización:** Generación automática de dashboards interactivos en HTML y reportes estadísticos avanzados.

## 🛠️ Stack Tecnológico
* **Lenguaje:** Python 3.11
* **Motor de Procesamiento:** PySpark (Spark Engine 3.4.0)
* **Infraestructura:** Docker & Docker Compose
* **Visualización:** Streamlit & Plotly Express (Mapas de calor y gráficos dinámicos).
* **Lectura eficiente de parquet desde Spark a Pandas:** Pyarrow.
* **Entorno de Ejecución:** Java 11 (OpenJDK) para máxima estabilidad con Spark.
* **Almacenamiento:** Formato Parquet (columnar) para alta eficiencia.

## 🚀 Cómo Ejecutar
Este proyecto está completamente **dockerizado**, eliminando la necesidad de instalar Spark, Java o Hadoop localmente.

### Opción 1: Ejecución con Docker (Recomendado)
Ideal para evitar configuraciones locales de Spark o Java.

### Requisitos Previos
* Docker y Docker Compose instalados.

1.  **Clonar el repositorio:**
    ```bash
    git clone [https://github.com/DDGUZMANO/carbon-emissions-pipeline.git](https://github.com/DDGUZMANO/carbon-emissions-pipeline.git)
    cd carbon-emissions-pipeline
    ```

2.  **Lanzar el entorno:**
    ```bash
    docker-compose up --build
    ```
3. **Acceso:** * El pipeline procesará las capas Medallion automáticamente.
    * Una vez finalizado, accede al Dashboard interactivo en: `http://localhost:8501`

### Opción 2: Ejecución Local (Manual)

1.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```
2.  **Ejecutar el pipeline:**
    ```bash
    python main.py
    ```
3. **Lanzar el dashboard:**
    ```bash
    streamlit run app_dashboard.py
    ```

## 🧠 Decisiones de Ingeniería
* **Optimización de Visualización:** Implementación de escala de colores basada en el **percentil 95** para mitigar el efecto de outliers en el mapa global.
* **Normalización de Índices:** Uso de **Base 100** en análisis de tendencias para comparar el crecimiento porcentual del PIB frente a la intensidad de carbono.
* **Dockerización:** Uso de volúmenes y redes aisladas para garantizar la portabilidad total entre entornos.
* **Java 11 sobre 17:** Decisión técnica basada en la estabilidad del Garbage Collector de la JVM y la compatibilidad con Spark 3.4.

## 📊 Análisis de Resultados y Conclusiones

Tras ejecutar el pipeline, los datos procesados en la **Capa Gold** revelan insights críticos sobre la transición energética global:

### ⚡ El Desacoplamiento Económico
El análisis muestra una tendencia clara de **desacoplamiento** en economías avanzadas (especialmente en Europa y Norteamérica). Mientras que el PIB per cápita (GDP) continúa creciendo, la intensidad de carbono (emisiones por unidad de PIB) ha disminuido drásticamente desde los años 90. Esto valida la eficiencia de las políticas de transición energética.

<p align="center">
  <img src="img/desacople_europa.png" width="600" alt="Desacoplamiento económico">
</p>

### 🌍 Brecha Regional
* **Europa:** Lidera la reducción de intensidad de carbono, optimizando sus procesos industriales.
* **Asia:** Presenta el crecimiento más acelerado en emisiones totales, correlacionado con su explosión industrial, aunque empieza a mostrar picos de estabilización en la última década.
* **África:** Mantiene una intensidad de carbono baja por país, pero con una dependencia crítica de fuentes de energía externas para su desarrollo económico.

<p align="center">
  <img src="img/tendencias.png" width="600" alt="Tendencias">
</p>

### 📈 Métricas Clave Generadas
* **Media de Intensidad de Carbono por Década:** Permite observar la velocidad de descarbonización regional.
* **Relación GDP vs Co2:** Identifica qué países son más "eco-eficientes" (generan más riqueza con menos emisiones).


<p align="center">
  <img src="img/dinamica_vs_intensidad.png" width="600" alt="Dinamica de emisiones versus intensidad">
</p>
---

## 📂 Estructura del Proyecto

```text
carbon-emissions-pipeline/
├── img/                       # Capturas de pantalla y visualizaciones para el portfolio
├── src/                       # Código fuente modular
│   ├── ingestion/             # Capa Bronze: Extracción de datos (OWID)
│   ├── transformation/        # Capas Silver y Gold: Procesamiento y lógica de negocio
│   ├── analysis/              # Insights adicionales y métricas específicas
│   ├── visualizations/        # Generación de gráficos y dashboards interactivos
│   └── common/                # Configuraciones compartidas (Spark, utilidades)
├── Dockerfile                 # Definición de la imagen (Python + Java 11)
├── docker-compose.yml         # Orquestación de servicios
├── app_dashboard.py           # Dashboard interactivo final (Streamlit)
├── main.py                    # Script principal (Orquestador del pipeline)
├── requirements.txt           # Dependencias de Python
└── README.md                  # Documentación profesional


---
**Desarrollado por [Douglas Guzmán](https://github.com/DDGUZMANO)** - Proyecto de Ingeniería de Datos con enfoque en Sostenibilidad y Arquitectura Medallion.