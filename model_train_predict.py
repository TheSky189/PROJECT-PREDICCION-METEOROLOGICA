import os
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

# Lee el parquet histórico procesado por Spark ETL (2022-2025)
PARQUET_DIR = "data/processed/aemet/clima_diaria_parquet"
OUT_PATH = "data/predictions/temp_max_forecast.csv"

df = pd.read_parquet(PARQUET_DIR)
df["dt"] = pd.to_datetime(df["dt"])
df = df.sort_values("dt").dropna(subset=["temp_max"])

if len(df) < 60:
    raise SystemExit("Datos insuficientes. Recomendado: >= 60 días para un modelo estacional simple.")

# ====== Features estacionales ======
# Día del año (1..366)
doy = df["dt"].dt.dayofyear.astype(int)

# Componentes seno/coseno para capturar estacionalidad anual
df["sin_doy"] = np.sin(2 * np.pi * doy / 365.25)
df["cos_doy"] = np.cos(2 * np.pi * doy / 365.25)

# Año como tendencia lenta (opcional)
df["year"] = df["dt"].dt.year.astype(int)

X = df[["sin_doy", "cos_doy", "year"]].values
y = df["temp_max"].astype(float).values

# ====== Validación rápida: últimos 30 días como test ======
test_days = 30
if len(df) > test_days:
    X_train, X_test = X[:-test_days], X[-test_days:]
    y_train, y_test = y[:-test_days], y[-test_days:]
else:
    X_train, y_train = X, y
    X_test, y_test = X, y

model = LinearRegression()
model.fit(X_train, y_train)

if len(df) > test_days:
    pred_test = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred_test)
    print(f"MAE (últimos {test_days} días): {mae:.2f} °C")

# ====== Predicción futuro ======
horizon = 7 # próximos 7 días
base_date = pd.Timestamp.today().normalize()  # calcula desde hoy
future_dates = [base_date + timedelta(days=i) for i in range(0, horizon)]


future_doy = np.array([d.dayofyear for d in future_dates])
X_future = np.column_stack([
    np.sin(2 * np.pi * future_doy / 365.25),
    np.cos(2 * np.pi * future_doy / 365.25),
    np.array([d.year for d in future_dates])
])

pred = model.predict(X_future)

pred_df = pd.DataFrame({
    "dt": future_dates,
    "pred_temp_max": np.round(pred, 1)
})

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
pred_df.to_csv(OUT_PATH, index=False)

print("Predicción guardada en:", OUT_PATH)
print(pred_df.to_string(index=False))
