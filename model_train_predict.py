import os
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.linear_model import LinearRegression

PARQUET_DIR = "data/processed/aemet/municipio_diaria_parquet"
OUT_PATH = "data/predictions/temp_max_forecast.csv"

df = pd.read_parquet(PARQUET_DIR)
df["dt"] = pd.to_datetime(df["dt"])
df = df.sort_values("dt").dropna(subset=["temp_max"])

# Necesitamos mínimo 5 puntos para que tenga sentido
if len(df) < 5:
    raise SystemExit("Necesitas más días acumulados (>=5) para entrenar un modelo simple.")

# Feature: índice temporal
X = np.arange(len(df)).reshape(-1, 1)
y = df["temp_max"].astype(float).values

model = LinearRegression()
model.fit(X, y)

# Predicción 3 días
horizon = 3
X_future = np.arange(len(df), len(df) + horizon).reshape(-1, 1)
pred = model.predict(X_future)

last_date = df["dt"].max()
future_dates = [last_date + timedelta(days=i) for i in range(1, horizon + 1)]

pred_df = pd.DataFrame({
    "dt": future_dates,
    "pred_temp_max": np.round(pred, 1)
})

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
pred_df.to_csv(OUT_PATH, index=False)

print("Predicción guardada en:", OUT_PATH)
print(pred_df.to_string(index=False))
