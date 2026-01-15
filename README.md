# Proyecto 3 — Predicción Meteorológica Big Data

Autores: **Jiajiao Xu** y **Jordi Vidal**  
Proyecto: **P3 Meteo BigData**  
Ciudad objetivo: **Barcelona** (AEMET OpenData)

Proyecto de análisis y predicción meteorológica desarrollado como parte del **Proyecto 3 – Big Data**, utilizando datos oficiales de **AEMET OpenData**, un pipeline Big Data con **Apache Spark**, modelos de **Machine Learning supervisado** y un **dashboard interactivo en Streamlit**.


## Descripción general

El sistema implementa un flujo completo de datos que abarca:

- Ingesta de datos desde una API oficial
- Proceso ETL con Apache Spark
- Almacenamiento optimizado en formato Parquet
- Modelado predictivo (clasificación y regresión)
- Evaluación mediante métricas y backtesting
- Visualización interactiva orientada a usuario final

El proyecto se centra en el municipio de **Barcelona**, utilizando como referencia la estación meteorológica **Barcelona – Fabra (0200E)**.


## Datos utilizados

- **Fuente:** AEMET OpenData  
- **Tipo de datos históricos:** Climatología diaria por estación (observaciones)
- **Rango temporal del histórico:**  
  - Desde: **2022-01-01**  
  - Hasta: **2025-12-31**
- **Variables principales:**
  - Temperatura máxima, mínima y media
  - Humedad relativa media
  - Precipitación diaria
  - Fecha de observación
  - Identificación de estación

Las **predicciones** se generan dinámicamente para los **7 días posteriores a la fecha de ejecución**, por lo que pueden aparecer fechas del año siguiente (por ejemplo, 2026).


## Modelos predictivos

El sistema utiliza un enfoque **modular**, con modelos supervisados diferenciados según la naturaleza de la variable:

### 🌧️ Predicción de lluvia (clasificación)
- Modelo: Clasificación supervisada
- Salida: Probabilidad de lluvia
- Métrica principal: **ROC-AUC**
- Interpretación orientada a usuario:
  - `< 40%` → No
  - `40–60%` → Posible
  - `> 60%` → Probable

### Predicción de variables continuas (regresión)
- Variables: temperatura, humedad y precipitación
- Modelo avanzado: **Random Forest**
- Feature engineering:
  - Variables temporales
  - Lags históricos
  - Medias móviles (rolling)
- Métrica principal: **MAE**

### Evaluación
- Backtesting temporal sobre los últimos **120 días**
- Resultados aproximados:
  - Temperatura máxima: MAE ≈ 1.8 °C
  - Temperatura mínima: MAE ≈ 1.4 °C
  - Humedad media: MAE ≈ 9 %
  - Lluvia: AUC ≈ 0.75


## Estructura del proyecto

```text
P3_METEO_BIGDATA/
│
├─ data/
│  ├─ raw/                       # Datos originales (JSON AEMET)
│  │  └─ aemet/
│  │     └─ clima_diaria/
│  │        └─ aemet_*.json
│  │
│  ├─ processed/                 # Datos procesados (Parquet)
│  │  └─ aemet/
│  │     ├─ clima_diaria_parquet/
│  │     └─ municipio_diaria_parquet/
│  │        (opcional: predicción oficial AEMET 8 días)
│  │
│  └─ predictions/               # Resultados de los modelos (CSV)
│     ├─ forecast_advanced_7d.csv
│     ├─ temp_max_forecast.csv   (legacy)
│     └─ rain_forecast.csv       (legacy)
│
├─ fetch_aemet_barcelona.py
├─ spark_etl_aemet.py
├─ rain_train_predict.py
├─ model_train_predict.py            # Modelo baseline
├─ model_advanced_train_predict.py   # Modelo avanzado actual
├─ app_streamlit.py
├─ run_pipeline.sh
├─ .gitignore
└─ README.md
```


## ⚙️ Requisitos del sistema

- Python **3.10** o superior
- Java **JDK 17**
- Apache Spark (modo local)
- Sistema operativo: Windows / macOS / Linux


## Activar entorno virtual (venv)
Antes de ejecutar el proyecto es obligatorio crear y activar un entorno virtual de Python, para aislar las dependencias del sistema.

  - 1. Crear el entorno virtual (solo la primera vez)
       `python -m venv .venv`
        Esto creará una carpeta `.venv/` con el entorno virtual del proyecto.

  - 2. Activar el entorno virtual
      macOS / Linux
      `source .venv/bin/activate`
      Windows (PowerShell)
      `.venv\Scripts\Activate.ps1`

      Cuando el entorno esté activo, el terminal mostrará algo similar a:
      `(.venv)`

  - 3. Detener Streamlit
        Para cerrar la aplicación Streamlit, usa:
        CTRL + C  en el terminal donde se esté ejecutando.

  - 4. Salir del entorno virtual
        Cuando hayas terminado de trabajar con el proyecto:
        `deactivate` - Esto cerrará el entorno virtual y devolverá el terminal al estado normal.
      


## Configuración de la API Key (AEMET)

Para acceder a AEMET OpenData es necesario configurar una **variable de entorno** con tu clave personal.
Es opcional, la clave esta incluido dentro del py fetch.

### macOS / Linux
`export AEMET_API_KEY="TU_API_KEY"`

### Windows (PowerShell)
`setx AEMET_API_KEY "TU_API_KEY"`



## Ejecución del sistema

Instalar request
   - `pip install requests`

Ejecución manual (pipeline completo)
   - `python fetch_aemet_barcelona.py
      python spark_etl_aemet.py
      python rain_train_predict.py
      python model_advanced_train_predict.py
      streamlit run app_streamlit.py`

Ejecución automatizada

El proyecto incluye el script:
   - `./run_pipeline.sh`

Este script ejecuta todo el pipeline, desde la descarga de datos hasta la actualización del dashboard.


## Dashboard

El cuadro de mandos desarrollado con Streamlit permite:

   - Visualizar indicadores clave (KPIs)
   - Consultar la predicción meteorológica a 7 días
   - Analizar la evolución temporal de las variables
   - Interpretar la probabilidad de lluvia mediante rangos (No / Posible /              Probable)
   - Acceder opcionalmente a una vista analítica con los datos completos


## Pruebas realizadas

   - Prueba de adquisición de datos (API AEMET)
   - Prueba de proceso ETL con Spark
   - Validación de calidad de datos
   - Evaluación de modelos mediante backtesting
   - Verificación de generación de predicciones y visualización


## Conclusiones

El proyecto implementa con éxito un sistema completo de predicción meteorológica basado en una arquitectura Big Data, integrando procesamiento distribuido, modelado predictivo evaluado y visualización interpretativa.

La solución es estable, reproducible y escalable, y puede ampliarse fácilmente a nuevos municipios, variables o entornos cloud.


## Mejoras futuras

- Ampliación del histórico de datos
- Inclusión de nuevos municipios
- Evaluación de modelos más avanzados
- Automatización mediante tareas programadas
- Despliegue en la nube

