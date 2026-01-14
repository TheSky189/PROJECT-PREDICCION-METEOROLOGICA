import os
import pandas as pd
import numpy as np
from datetime import timedelta
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

PARQUET_DIR = "data/processed/aemet/clima_diaria_parquet"
OUT_PATH = "data/predictions/rain_forecast.csv"

# ====== Cargar histórico ======
df = pd.read_parquet(PARQUET_DIR)
df["dt"] = pd.to_datetime(df["dt"])
df = df.sort_values("dt").dropna(subset=["dt"])

# Necesitamos precipitación para definir lluvia
if "precip" not in df.columns:
    raise SystemExit("No existe la columna 'precip' en el parquet. Revisa el ETL.")

# ====== Definir etiqueta: lluvia (1/0) ======
# Nota: si precip es NaN -> lo tratamos como 0 para el label (puedes decidir otra política)
prec = pd.to_numeric(df["precip"], errors="coerce").fillna(0.0)
df["lluvia"] = (prec > 0.0).astype(int)

# ====== Features estacionales (sin/cos) ======
doy = df["dt"].dt.dayofyear.astype(int)
df["sin_doy"] = np.sin(2 * np.pi * doy / 365.25)
df["cos_doy"] = np.cos(2 * np.pi * doy / 365.25)
df["year"] = df["dt"].dt.year.astype(int)

X = df[["sin_doy", "cos_doy", "year"]].values
y = df["lluvia"].values

# ====== Split temporal: últimos 90 días como test ======
test_days = 90
if len(df) <= test_days + 30:
    test_days = max(30, len(df)//5)

X_train, X_test = X[:-test_days], X[-test_days:]
y_train, y_test = y[:-test_days], y[-test_days:]

# ====== Modelo: Logistic Regression (clasificación) ======
# class_weight balanceado para que no se "olvide" de los días de lluvia
model = LogisticRegression(max_iter=2000, class_weight="balanced")
model.fit(X_train, y_train)

# ====== Evaluación ======
proba_test = model.predict_proba(X_test)[:, 1]
pred_test = (proba_test >= 0.5).astype(int)

acc = accuracy_score(y_test, pred_test)
# ROC AUC solo si hay ambas clases en test
auc = roc_auc_score(y_test, proba_test) if len(np.unique(y_test)) > 1 else float("nan")

rain_rate = y.mean()
print(f"Rain rate histórico (lluvia=1): {rain_rate:.3f}")
print(f"Test últimos {test_days} días -> Accuracy: {acc:.3f} | ROC-AUC: {auc:.3f}")

# ====== Predicción futuro (horizonte 7 días) ======
horizon = 7
base_date = pd.Timestamp.today().normalize()
future_dates = [base_date + timedelta(days=i) for i in range(0, horizon)]


future_doy = np.array([d.dayofyear for d in future_dates])
X_future = np.column_stack([
    np.sin(2 * np.pi * future_doy / 365.25),
    np.cos(2 * np.pi * future_doy / 365.25),
    np.array([d.year for d in future_dates])
])

proba_future = model.predict_proba(X_future)[:, 1]
pred_future = (proba_future >= 0.5).astype(int)

# ====== Crear niveles (3 niveles) ======
# Umbrales recomendados:
# - Baja: < 0.40
# - Media: 0.40–0.60
# - Alta: > 0.60
def prob_to_level(p: float) -> str:
    if p < 0.48:
        return "Baja"
    elif p <= 0.52:
        return "Media"
    else:
        return "Alta"


def level_to_icon(level: str) -> str:
    return {"Baja": "☀️", "Media": "🌦️", "Alta": "🌧️"}.get(level, "🌦️")

levels = [prob_to_level(float(p)) for p in proba_future]
icons = [level_to_icon(lv) for lv in levels]

out = pd.DataFrame({
    "dt": future_dates,
    "rain_prob": np.round(proba_future, 3),
    "rain_pred": pred_future,
    "rain_level": levels,
    "rain_icon": icons
})


os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
out.to_csv(OUT_PATH, index=False)

print("Predicción lluvia guardada en:", OUT_PATH)
print(out.to_string(index=False))
