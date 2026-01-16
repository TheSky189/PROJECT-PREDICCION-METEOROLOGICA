import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import timedelta

PARQUET_DIR = "data/processed/aemet/municipio_diaria_parquet"

df = pd.read_parquet(PARQUET_DIR)
df["dt"] = pd.to_datetime(df["dt"])
df = df.sort_values("dt")

# solo filas con temp_max no nulo
y = df["temp_max"].astype(float).values
X = np.arange(len(df)).reshape(-1, 1)

model = LinearRegression()
model.fit(X, y)

# predicción para los próximos 3 días
future_steps = 3
X_future = np.arange(len(df), len(df) + future_steps).reshape(-1, 1)
pred = model.predict(X_future)

last_date = df["dt"].max()
future_dates = [last_date + timedelta(days=i) for i in range(1, future_steps + 1)]

out = pd.DataFrame({
    "dt": future_dates,
    "pred_temp_max": pred.round(1)
})

print("=== Predicción (tendencia) temp_max ===")
print(out.to_string(index=False))
