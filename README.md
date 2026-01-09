# Proyecto 3 — Sistema de Predicción Meteorológica (Big Data)

Proyecto orientado al procesamiento, análisis y predicción de datos meteorológicos utilizando un enfoque de Big Data.  
Incluye ingestión de datos reales (AEMET), procesamiento ETL con Spark, entrenamiento de modelos de predicción y visualización mediante un dashboard interactivo.


## Autores
- **Jiajiao Xu**
- **Jordi Vidal**


## Objetivo del proyecto
El objetivo de este proyecto es diseñar e implementar un sistema completo de análisis meteorológico que permita:

- Ingestar datos meteorológicos reales desde fuentes oficiales.
- Procesar grandes volúmenes de datos mediante técnicas de Big Data.
- Almacenar los datos procesados en formatos eficientes.
- Analizar y visualizar información meteorológica relevante.
- Generar predicciones meteorológicas a corto plazo.
- Presentar los resultados mediante un dashboard interactivo.


## Tecnologías utilizadas
- **Python**
- **Apache Spark (PySpark)** para procesos ETL
- **Streamlit** para visualización interactiva
- **pandas / numpy** para análisis de datos
- **scikit-learn** para entrenamiento y predicción
- **Formato Parquet** para almacenamiento optimizado
- **Git & GitHub** para control de versiones


## Estructura del proyecto

```

P3_METEO_BIGDATA
├─ .github/                           # Configuración GitHub
├─ .venv/                             # Entorno virtual
├─ dashboards/                        # Recursos del dashboard
├─ data/
│  ├─ raw/                            # Datos crudos (AEMET)
│  ├─ processed/                      # Datos procesados
│  └─ predictions/                    # Resultados de predicción
├─ processed/
│  └─ aemet/
│     └─ municipio_diaria_parquet/    # Datos finales en formato Parquet
├─ app_streamlit.py                   # Dashboard interactivo
├─ fetch_aemet_barcelona.py           # Ingesta de datos desde AEMET
├─ spark_etl_aemet.py                 # Proceso ETL con Spark
├─ train_predict.py                   # Entrenamiento y predicción
├─ model_train_predict.py             # Lógica del modelo ML
├─ run_pipeline.sh                    # Ejecución completa del pipeline
├─ .gitignore
└─ README.md

````


## Flujo del sistema (Pipeline de datos)

1. **Ingesta de datos**
   - Script: `fetch_aemet_barcelona.py`
   - Obtiene datos meteorológicos reales desde la API de AEMET.

2. **Procesamiento ETL**
   - Script: `spark_etl_aemet.py`
   - Limpieza, transformación y estructuración de los datos.
   - Almacenamiento en formato Parquet para optimizar rendimiento.

3. **Entrenamiento y predicción**
   - Scripts: `train_predict.py`, `model_train_predict.py`
   - Entrenamiento de modelos de predicción meteorológica.
   - Generación de predicciones a corto plazo.

4. **Visualización**
   - Script: `app_streamlit.py`
   - Dashboard interactivo con filtros, tablas y gráficos.


## Instalación

### Requisitos
- Python 3.10 o superior
- Java (necesario para Apache Spark)
- pip

### Crear entorno virtual
```bash
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows
````

### Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## Ejecución del proyecto

### Opción 1: Ejecutar todo el pipeline automáticamente

```bash
bash run_pipeline.sh
```

### Opción 2: Ejecutar el sistema por fases

```bash
python fetch_aemet_barcelona.py
python spark_etl_aemet.py
python train_predict.py
streamlit run app_streamlit.py
```

---

## Dashboard

El dashboard desarrollado con Streamlit permite:

* Seleccionar rangos temporales (desde / hasta).
* Mostrar tablas completas de datos meteorológicos.
* Visualizar series temporales.
* Analizar correlaciones entre variables.
* Consultar predicciones meteorológicas generadas por el modelo.

---

## Consideraciones técnicas

* El uso de **Parquet** permite mejorar el rendimiento y la escalabilidad.
* El pipeline está diseñado de forma **modular**, facilitando futuras ampliaciones.
* El modelo de predicción puede evolucionar incorporando más variables o algoritmos más avanzados.

### Dashboard features

- Automatic visualization of short-term meteorological predictions (AEMET)
- Manual data refresh button to update the complete data pipeline
- 7-day summary aligned with the current date
- Interactive charts and optional table inspection




