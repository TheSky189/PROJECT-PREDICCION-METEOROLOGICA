import glob
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, regexp_replace, lit, when
)

# Lee TODOS los JSON históricos descargados (2022-2025)
RAW_GLOB = "data/raw/aemet/clima_diaria/aemet_*.json"

# Nuevo parquet de históricos (para evitar mezclar con el antiguo de predicción)
OUT_DIR = "data/processed/aemet/clima_diaria_parquet"

def main():
    spark = (
        SparkSession.builder
        .appName("P3-AEMET-ETL-HISTORICO")
        .master("local[*]")
        .getOrCreate()
    )

    files = glob.glob(RAW_GLOB)
    if not files:
        print(f"❌ No se encontraron ficheros RAW: {RAW_GLOB}")
        print("👉 Solución:")
        print("   1) Exporta la API Key: export AEMET_API_KEY=\"TU_API_KEY\"")
        print("   2) Ejecuta: python fetch_aemet_barcelona.py")
        print("   3) Vuelve a ejecutar: python spark_etl_aemet.py")
        sys.exit(1)

    print(f"✅ RAW encontrados: {len(files)}")
    print(f"Ejemplo: {files[0]}")


    # 1) Leer JSON (son listas de dicts -> Spark lo maneja)
    df = spark.read.option("multiLine", "true").json(RAW_GLOB)

    if df.rdd.isEmpty():
        raise SystemExit(f"No se encontraron ficheros RAW con patrón: {RAW_GLOB}")

    print("Schema RAW:")
    df.printSchema()
    df.show(5, truncate=False)

    # 2) Normalización de campos
    # En AEMET, algunos números vienen como string con coma decimal, ej: "12,3"
    # También puede venir "Ip" (inapreciable) en precipitación.
    def to_double_safe(c):
        return regexp_replace(col(c).cast("string"), ",", ".").cast("double")

    # Campos típicos (pueden variar según estación):
    # fecha, tmax, tmin, tmed, prec, hrMedia, ...
    # Vamos a crear columnas estándar para el modelo:
    # - temp_max, temp_min, temp_med, precip
    # - dt (date)
    base = df

    # Si alguna columna no existe, Spark lanzará error al seleccionarla.
    # Por eso usamos when(col is not null) y revisa tu schema si hay nombres distintos.
    base = base.withColumn("dt", to_date(col("fecha")))

    # Temperaturas
    base = base.withColumn("temp_max", to_double_safe("tmax"))
    base = base.withColumn("temp_min", to_double_safe("tmin"))
    base = base.withColumn("temp_med", to_double_safe("tmed"))

    # Precipitación: puede venir "Ip" o vacío -> lo convertimos a 0.0 o null (elige)
    prec_str = col("prec").cast("string")
    base = base.withColumn(
        "precip",
        when(prec_str.isNull(), None)
        .when(prec_str == "Ip", lit(0.0))
        .otherwise(regexp_replace(prec_str, ",", ".").cast("double"))
    )

    # Humedad media (si existe)
    if "hrMedia" in base.columns:
        base = base.withColumn("hum_med", to_double_safe("hrMedia"))
    else:
        base = base.withColumn("hum_med", lit(None).cast("double"))

    # 3) Seleccionar columnas finales (mínimas para dashboard/modelo)
    out = base.select(
        "dt", "fecha",
        col("temp_max"),
        col("temp_min"),
        col("temp_med"),
        col("precip"),
        col("hum_med"),
        col("indicativo").alias("station_id"),
        col("nombre").alias("station_name"),
        col("provincia")
    )

    # 4) Guardar parquet (overwrite para empezar limpio)
    os.makedirs(OUT_DIR, exist_ok=True)
    (
        out.write.mode("overwrite")
        .partitionBy("dt")
        .parquet(OUT_DIR)
    )

    print("\nETL OK")
    print("RAW:", RAW_GLOB)
    print("PARQUET:", OUT_DIR)

    out.selectExpr("min(dt) as min_dt", "max(dt) as max_dt").show()
    out.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
