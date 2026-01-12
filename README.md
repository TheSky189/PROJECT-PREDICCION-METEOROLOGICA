# P3 Meteo BigData (Barcelona) — Predicción Meteorológica con AEMET + Spark ETL + Modelos

Autores: **Jiajiao Xu** y **Jordi Vidal**  
Proyecto: **P3 Meteo BigData**  
Ciudad objetivo: **Barcelona** (AEMET OpenData)


## 1. Objetivo del proyecto

El objetivo principal es construir un **pipeline Big Data** capaz de:

1) **Extraer datos meteorológicos históricos** (temperatura, precipitación, humedad, etc.) desde **AEMET OpenData** durante varios años.  
2) **Procesar y estructurar** los datos con un flujo **ETL** (Raw JSON → Spark → Parquet).  
3) Entrenar **modelos predictivos** basados en años anteriores para realizar predicción de:
   - **Temperatura (máxima / mínima / media)**  
   - **Probabilidad de lluvia** (clasificación)  
   - **Cantidad de precipitación (mm)** (regresión, solo si llueve)  
   - **Humedad media**

4) Visualizar resultados en un **Dashboard con Streamlit**.


## 2. Fuente de datos

- **AEMET OpenData**
- Datos históricos descargados (ejemplo):
  - `RAW`: `data/raw/aemet/clima_diaria/aemet_*.json`
  - `PARQUET`: `data/processed/aemet/clima_diaria_parquet`
  - Rango confirmado:
    - `min_dt = 2022-01-01`
    - `max_dt = 2025-12-31`


## 3. Arquitectura del pipeline

### 3.1 Flujo general

**AEMET → Raw JSON → Spark ETL → Parquet → Modelos → Predicción → Dashboard**

- **Ingesta** (Raw):
  - `fetch_aemet_barcelona.py` descarga los JSON mensuales desde AEMET.
- **ETL**:
  - `spark_etl_aemet.py` transforma/limpia los JSON y genera Parquet particionado.
- **Modelos**:
  - `model_advanced_train_predict.py` entrena y genera predicciones para los próximos 7 días.
- **Dashboard**:
  - `app_streamlit.py` visualiza datos y predicciones.


## 4. Estructura del proyecto

`P3_METEO_BIGDATA/
├─ data/
│ ├─ raw/
│ │ └─ aemet/
│ │ └─ clima_diaria/
│ │ └─ aemet_*.json
│ ├─ processed/
│ │ └─ aemet/
│ │ ├─ clima_diaria_parquet/
│ │ └─ municipio_diaria_parquet/ (opcional: predicción oficial AEMET 8 días)
│ └─ predictions/
│ ├─ forecast_advanced_7d.csv
│ ├─ temp_max_forecast.csv (legacy)
│ └─ rain_forecast.csv (legacy)
│
├─ fetch_aemet_barcelona.py
├─ spark_etl_aemet.py
├─ model_advanced_train_predict.py
├─ app_streamlit.py
├─ run_pipeline.sh
├─ .gitignore
└─ README.md`


## 5. Modelado y predicción (avanzado)

### 5.1 Variables disponibles (dataset histórico)

Columnas principales:
- `temp_max`, `temp_min`, `temp_med`
- `precip` (mm)
- `hum_med` (%)
- `dt` (fecha)

### 5.2 Features utilizados

Se generan **features temporales + retardos (lags)** y medias móviles (rolling), por ejemplo:
- Día de la semana, mes
- Lags de 1/2/3/7 días para temp/precip/humedad
- Rolling mean (7 días) para variables clave

### 5.3 Salidas del modelo

Se genera un fichero:
- `data/predictions/forecast_advanced_7d.csv`

Contiene:
- `pred_temp_max`, `pred_temp_min`, `pred_temp_med`
- `pred_hum_med`
- `rain_prob` (probabilidad)
- `rain_level` (Posible / Probable / Alta)
- `rain_pred` (0/1 con umbral)
- `pred_precip_mm` (mm esperados si llueve)
- `rain_icon` (icono UI)

### 5.4 Evaluación (Backtest)

Ejemplo de salida:
- Temp (últimos 120 días):
  - MAE temp_max ~ 1.8
  - MAE temp_min ~ 1.4
  - MAE temp_med ~ 1.5
- Lluvia (últimos 120 días):
  - AUC ~ 0.75
- Precipitación (mm, solo días con lluvia):
  - MAE ~ 5 mm


## 6. Cómo ejecutar el proyecto

### 6.1 Requisitos

- Python 3.11+ (entorno virtual recomendado)
- Java (si Spark lo requiere en tu entorno)
- Librerías principales:
  - `pandas`, `requests`, `pyspark`, `streamlit`, `scikit-learn`

### 6.2 Variables de entorno

Configura tu API Key de AEMET:
`export AEMET_API_KEY="TU_API_KEY"`

6.3 Paso a paso
Descargar histórico (ejemplo 2022–2025):

python fetch_aemet_barcelona.py
ETL con Spark → Parquet:
`python spark_etl_aemet.py`

Entrenar + predecir (modelo avanzado):
`python model_advanced_train_predict.py`

Lanzar dashboard:
`streamlit run app_streamlit.py`

7. Notas importantes (GitHub / tamaño datos)
No subir .venv/ a GitHub.

No subir datasets pesados (data/raw, data/processed) si ocupan mucho.

Mantener el pipeline reproducible: al clonar el repo, se puede regenerar todo ejecutando los scripts.

8. Licencia
Uso académico (proyecto de clase).
Datos fuente: AEMET OpenData.

