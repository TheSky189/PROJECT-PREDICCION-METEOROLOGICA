import os
import sys
import glob
import platform

os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, lit, when


RAW_DIR = os.path.join("data", "raw", "aemet", "clima_diaria")
RAW_GLOB = os.path.join(RAW_DIR, "aemet_*.json")


def is_windows() -> bool:
    return platform.system().lower().startswith("win")


def to_double_safe(colname: str):
    return regexp_replace(col(colname).cast("string"), ",", ".").cast("double")


def main():
    spark = (
        SparkSession.builder
        .appName("AEMET_ETL")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "spark-warehouse"))
        .getOrCreate()
    )

    files = glob.glob(RAW_GLOB)
    if not files:
        print(f"❌ No se encontraron ficheros RAW: {RAW_GLOB}")
        print("👉 Ejecuta primero: python fetch_aemet_barcelona.py")
        spark.stop()
        sys.exit(1)

    print(f"✅ RAW encontrados: {len(files)}")
    print(f"Ejemplo: {files[0]}")

    df = spark.read.option("multiLine", "true").json(files)

    sample = df.limit(1).collect()
    if len(sample) == 0:
        spark.stop()
        raise SystemExit(f"❌ No hay datos al leer los JSON: {RAW_GLOB}")

    print("Schema RAW:")
    df.printSchema()
    df.show(5, truncate=False)

    base = df.withColumn("dt", to_date(col("fecha")))

    base = base.withColumn("temp_max", to_double_safe("tmax"))
    base = base.withColumn("temp_min", to_double_safe("tmin"))
    base = base.withColumn("temp_med", to_double_safe("tmed"))

    prec_str = col("prec").cast("string")
    base = base.withColumn(
        "precip",
        when(prec_str.isNull(), lit(None).cast("double"))
        .when(prec_str == "Ip", lit(0.0))
        .otherwise(regexp_replace(prec_str, ",", ".").cast("double")),
    )

    if "hrMedia" in base.columns:
        base = base.withColumn("hum_med", to_double_safe("hrMedia"))
    else:
        base = base.withColumn("hum_med", lit(None).cast("double"))

    out = base.select(
        "dt", "fecha",
        col("temp_max"),
        col("temp_min"),
        col("temp_med"),
        col("precip"),
        col("hum_med"),
        col("indicativo").alias("station_id"),
        col("nombre").alias("station_name"),
        col("provincia"),
    )

    out_dir = os.path.join("data", "processed", "aemet")
    os.makedirs(out_dir, exist_ok=True)

    if is_windows():
        # Windows: avoid Hadoop writer (winutils) -> use pandas output
        pdf = out.orderBy("dt").toPandas()
        out_path = os.path.join(out_dir, "clima_diaria_processed.csv")
        pdf.to_csv(out_path, index=False, encoding="utf-8")
        print("\n✅ ETL OK (WINDOWS, pandas CSV)")
        print("SALIDA:", out_path)
    else:
        parquet_dir = os.path.join(out_dir, "clima_diaria_parquet")
        (out.write
            .mode("overwrite")
            .partitionBy("dt")
            .parquet(parquet_dir)
        )
        print("\n✅ ETL OK (PARQUET)")
        print("SALIDA:", parquet_dir)

    out.selectExpr("min(dt) as min_dt", "max(dt) as max_dt").show()
    out.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
