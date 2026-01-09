import glob
import json
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

RAW_GLOB = "data/raw/aemet/municipio_diaria/barcelona_08019_*.json"
OUT_DIR = "data/processed/aemet/municipio_diaria_parquet"

def latest_raw_file() -> str:
    files = sorted(glob.glob(RAW_GLOB))
    if not files:
        raise SystemExit("No se encontraron ficheros RAW en data/raw/...")
    return files[-1]

def to_int_or_none(x):
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, float)):
            return int(x)
        if isinstance(x, str) and x.strip() != "":
            return int(float(x))
    except Exception:
        return None
    return None

def pick_value(v):
    """
    AEMET devuelve campos con formatos distintos según el día:
    - int/str -> valor directo
    - list[{"value": "..."}] o list[str] -> cogemos el primero
    - dict con {"value": "..."} o {"maxima":..,"minima":..} -> elegimos una heurística
    """
    if v is None:
        return None

    # directo
    if isinstance(v, (int, float, str, bool)):
        return v

    # lista
    if isinstance(v, list):
        if len(v) == 0:
            return None
        first = v[0]
        if isinstance(first, dict) and "value" in first:
            return first["value"]
        return first

    # diccionario
    if isinstance(v, dict):
        if "value" in v:
            return v["value"]
        # si es un rango, preferimos media simple si existe max/min
        if "maxima" in v and "minima" in v:
            mx = to_int_or_none(v.get("maxima"))
            mn = to_int_or_none(v.get("minima"))
            if mx is not None and mn is not None:
                return int((mx + mn) / 2)
        return None

    return None

raw_path = latest_raw_file()

with open(raw_path, "r", encoding="utf-8") as f:
    payload = json.load(f)

root = payload[0] if isinstance(payload, list) and payload else payload

municipio_nombre = root.get("nombre", "Barcelona")
municipio_id = str(root.get("id", "08019")).zfill(5)
fecha_carga = datetime.now().isoformat(timespec="seconds")

dias = root.get("prediccion", {}).get("dia", [])

rows = []
for d in dias:
    fecha = d.get("fecha")

    # temperatura (normalmente dict con maxima/minima)
    t = d.get("temperatura")
    t_max = to_int_or_none(t.get("maxima")) if isinstance(t, dict) else None
    t_min = to_int_or_none(t.get("minima")) if isinstance(t, dict) else None

    # humedadRelativa (puede venir raro)
    h = d.get("humedadRelativa")
    h_max = None
    h_min = None
    if isinstance(h, dict):
        h_max = to_int_or_none(h.get("maxima"))
        h_min = to_int_or_none(h.get("minima"))
    else:
        # si viene como lista/otro, lo intentamos convertir a un único valor
        hv = to_int_or_none(pick_value(h))
        h_max = hv
        h_min = hv

    # probPrecipitacion suele ser lista (por periodos). cogemos el máximo del día
    pp = d.get("probPrecipitacion", [])
    probs = []
    if isinstance(pp, list):
        for item in pp:
            if isinstance(item, dict):
                probs.append(to_int_or_none(item.get("value")))
            else:
                probs.append(to_int_or_none(item))
    prob_precip_max = max([p for p in probs if p is not None], default=None)

    # estado del cielo suele ser lista con descripciones/valores (cogemos el primero)
    ec = d.get("estadoCielo")
    estado_cielo = str(pick_value(ec)) if ec is not None else None

    rows.append({
        "fecha": fecha,
        "municipio_id": municipio_id,
        "municipio_nombre": municipio_nombre,
        "fecha_carga": fecha_carga,
        "temp_max": t_max,
        "temp_min": t_min,
        "hum_max": h_max,
        "hum_min": h_min,
        "prob_precip_max": prob_precip_max,
        "estado_cielo": estado_cielo,
    })

# Definimos schema fijo (evita errores de inferencia)
schema = StructType([
    StructField("fecha", StringType(), True),
    StructField("municipio_id", StringType(), True),
    StructField("municipio_nombre", StringType(), True),
    StructField("fecha_carga", StringType(), True),
    StructField("temp_max", IntegerType(), True),
    StructField("temp_min", IntegerType(), True),
    StructField("hum_max", IntegerType(), True),
    StructField("hum_min", IntegerType(), True),
    StructField("prob_precip_max", IntegerType(), True),
    StructField("estado_cielo", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("P3-AEMET-ETL")
    .master("local[*]")
    .getOrCreate()
)

df = spark.createDataFrame(rows, schema=schema)

from pyspark.sql.functions import to_date, col, least, lit
df2 = df.withColumn("dt", to_date(col("fecha")))

# Normalizar probabilidad a [0, 100]
df2 = df2.withColumn("prob_precip_max", least(col("prob_precip_max"), lit(100)))

os.makedirs(OUT_DIR, exist_ok=True)
(
    df2.write.mode("append")
    .partitionBy("dt")
    .parquet(OUT_DIR)
)


print("ETL OK")
print("RAW:", raw_path)
print("PARQUET:", OUT_DIR)
df2.show(truncate=False)

spark.stop()
