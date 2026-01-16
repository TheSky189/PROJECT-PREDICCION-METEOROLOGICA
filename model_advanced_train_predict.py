import os
import numpy as np
import pandas as pd
from datetime import timedelta

from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import mean_absolute_error, roc_auc_score


def load_processed_data():
    # Windows output (CSV)
    csv_path = os.path.join("data", "processed", "aemet", "clima_diaria_processed.csv")
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, parse_dates=["dt"])
        return df

    # mac/linux output (parquet)
    pq_dir = os.path.join("data", "processed", "aemet", "clima_diaria_parquet")
    if os.path.exists(pq_dir):
        df = pd.read_parquet(pq_dir)
        if "dt" in df.columns:
            df["dt"] = pd.to_datetime(df["dt"])
        return df

    raise FileNotFoundError(
        "No se encontró dataset procesado. Ejecuta primero:\n"
        "python fetch_aemet_barcelona.py && python spark_etl_aemet.py"
    )


OUT = "data/predictions/forecast_advanced_7d.csv"

HORIZON = 7
LAGS = [1, 2, 3, 7]
ROLL = 7

BASE_COLS = ["temp_max", "temp_min", "temp_med", "precip", "hum_med"]


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    doy = df["dt"].dt.dayofyear.astype(int)
    df["sin_doy"] = np.sin(2 * np.pi * doy / 365.25)
    df["cos_doy"] = np.cos(2 * np.pi * doy / 365.25)
    df["year"] = df["dt"].dt.year.astype(int)
    return df


def add_lag_roll_features(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        for l in LAGS:
            df[f"{c}_lag{l}"] = df[c].shift(l)
        df[f"{c}_roll{ROLL}_mean"] = df[c].rolling(ROLL).mean()
        df[f"{c}_roll{ROLL}_std"] = df[c].rolling(ROLL).std()
        if c == "precip":
            df[f"{c}_roll{ROLL}_sum"] = df[c].rolling(ROLL).sum()
    return df


def build_feature_table(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df = df_raw.copy()
    df = df.sort_values("dt").reset_index(drop=True)

    for c in BASE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = add_time_features(df)
    df = add_lag_roll_features(df, BASE_COLS)

    feat_cols = ["sin_doy", "cos_doy", "year"]
    feat_cols += [c for c in df.columns if any(x in c for x in ["_lag", "_roll"])]

    df_feat = df.dropna(subset=feat_cols + BASE_COLS).copy().reset_index(drop=True)
    return df_feat, feat_cols


def make_regressor():
    return RandomForestRegressor(
        n_estimators=500,
        random_state=42,
        min_samples_leaf=2,
        n_jobs=-1
    )


def make_classifier():
    return RandomForestClassifier(
        n_estimators=500,
        random_state=42,
        min_samples_leaf=2,
        class_weight="balanced",
        n_jobs=-1
    )


def temporal_split(df: pd.DataFrame, test_days: int = 120):
    if len(df) < test_days + 60:
        test_days = max(60, len(df) // 5)
    train = df.iloc[:-test_days].copy()
    test = df.iloc[-test_days:].copy()
    return train, test, test_days


def backtest(df_feat: pd.DataFrame, feat_cols: list[str], models: dict):
    train, test, test_days = temporal_split(df_feat, test_days=120)

    Xtr = train[feat_cols].values
    Xte = test[feat_cols].values

    for target in ["temp_max", "temp_min", "temp_med", "hum_med"]:
        ytr = train[target].values
        yte = test[target].values
        m = models[target]
        m.fit(Xtr, ytr)
        pred = m.predict(Xte)
        mae = mean_absolute_error(yte, pred)
        print(f"Backtest {target} últimos {test_days} días -> MAE: {mae:.2f}")

    ytr_r = (train["precip"].fillna(0.0).values > 0.0).astype(int)
    yte_r = (test["precip"].fillna(0.0).values > 0.0).astype(int)

    clf = models["rain_clf"]
    clf.fit(Xtr, ytr_r)
    proba = clf.predict_proba(Xte)[:, 1]
    auc = roc_auc_score(yte_r, proba) if len(np.unique(yte_r)) > 1 else float("nan")
    print(f"Backtest lluvia últimos {test_days} días -> AUC: {auc:.3f} | Rain rate test: {yte_r.mean():.3f}")

    rain_train = train[train["precip"] > 0.0].copy()
    rain_test = test[test["precip"] > 0.0].copy()
    if len(rain_train) >= 200 and len(rain_test) >= 30:
        Xtr_p = rain_train[feat_cols].values
        Xte_p = rain_test[feat_cols].values
        ytr_p = np.log1p(rain_train["precip"].values)
        yte_p = rain_test["precip"].values

        mp = models["precip_reg"]
        mp.fit(Xtr_p, ytr_p)
        pred_log = mp.predict(Xte_p)
        pred_mm = np.expm1(pred_log)
        mae_p = mean_absolute_error(yte_p, pred_mm)
        print(f"Backtest precip(mm) (solo días con lluvia) -> MAE: {mae_p:.2f} mm")
    else:
        print("Backtest precip(mm): no hay suficientes días de lluvia para evaluar bien (OK).")


def decide_rain_level(p: float) -> tuple[str, int, str]:
    if p >= 0.65:
        return "Probable", 1, "🌧️"
    elif p >= 0.40:
        return "Posible", 0, "🌦️"
    else:
        return "No", 0, "☀️"


def recursive_forecast(df_raw: pd.DataFrame, feat_cols: list[str], models: dict) -> pd.DataFrame:
    hist = df_raw.copy().sort_values("dt").reset_index(drop=True)

    hist["dt"] = pd.to_datetime(hist["dt"])
    for c in BASE_COLS:
        hist[c] = pd.to_numeric(hist[c], errors="coerce")

    base_date = pd.Timestamp.today().normalize()
    future_dates = [base_date + timedelta(days=i) for i in range(HORIZON)]

    last_dt = hist["dt"].max()
    if base_date > last_dt:
        gap = (base_date - last_dt).days
        for i in range(1, gap + 1):
            hist = pd.concat([hist, pd.DataFrame([{"dt": last_dt + timedelta(days=i)}])], ignore_index=True)

    for c in BASE_COLS:
        if c not in hist.columns:
            hist[c] = np.nan

    out_rows = []

    for d in future_dates:
        if not (hist["dt"] == d).any():
            hist = pd.concat([hist, pd.DataFrame([{"dt": d}])], ignore_index=True)

        hist = hist.sort_values("dt").reset_index(drop=True)

        tmp = hist.copy()
        tmp = add_time_features(tmp)
        tmp = add_lag_roll_features(tmp, BASE_COLS)

        tmp[feat_cols] = tmp[feat_cols].ffill().fillna(0.0)

        row = tmp[tmp["dt"] == d].iloc[0]
        Xd = row[feat_cols].values.reshape(1, -1)

        pred_tmax = float(models["temp_max"].predict(Xd)[0])
        pred_tmin = float(models["temp_min"].predict(Xd)[0])
        pred_tmed = float(models["temp_med"].predict(Xd)[0])
        pred_hum = float(models["hum_med"].predict(Xd)[0])

        rain_prob = float(models["rain_clf"].predict_proba(Xd)[0, 1])
        rain_level, rain_pred, rain_icon = decide_rain_level(rain_prob)

        pred_precip_mm = 0.0
        if rain_prob >= 0.40:
            pred_log = float(models["precip_reg"].predict(Xd)[0])
            pred_precip_mm = max(0.0, float(np.expm1(pred_log)))

        out_rows.append({
            "dt": d.date().isoformat(),
            "pred_temp_max": round(pred_tmax, 1),
            "pred_temp_min": round(pred_tmin, 1),
            "pred_temp_med": round(pred_tmed, 1),
            "pred_hum_med": round(pred_hum, 1),
            "rain_prob": round(rain_prob, 3),
            "rain_level": rain_level,
            "rain_pred": rain_pred,
            "pred_precip_mm": round(pred_precip_mm, 2),
            "rain_icon": rain_icon
        })

        idx = hist.index[hist["dt"] == d][0]
        hist.at[idx, "temp_max"] = pred_tmax
        hist.at[idx, "temp_min"] = pred_tmin
        hist.at[idx, "temp_med"] = pred_tmed
        hist.at[idx, "hum_med"] = pred_hum
        hist.at[idx, "precip"] = pred_precip_mm

    return pd.DataFrame(out_rows)


def main():
    df = load_processed_data()
    df["dt"] = pd.to_datetime(df["dt"])
    df = df.sort_values("dt")

    df_feat, feat_cols = build_feature_table(df)

    print("Columnas base:", BASE_COLS)
    print("Nº features:", len(feat_cols))
    print("Rango datos:", df_feat["dt"].min().date(), "->", df_feat["dt"].max().date())

    models = {
        "temp_max": make_regressor(),
        "temp_min": make_regressor(),
        "temp_med": make_regressor(),
        "hum_med": make_regressor(),
        "rain_clf": make_classifier(),
        "precip_reg": make_regressor()
    }

    backtest(df_feat, feat_cols, models)

    Xall = df_feat[feat_cols].values
    for target in ["temp_max", "temp_min", "temp_med", "hum_med"]:
        models[target].fit(Xall, df_feat[target].values)

    y_rain_all = (df_feat["precip"].fillna(0.0).values > 0.0).astype(int)
    models["rain_clf"].fit(Xall, y_rain_all)

    rain_df = df_feat[df_feat["precip"] > 0.0].copy()
    if len(rain_df) < 200:
        print("Aviso: pocos días con lluvia para precip_reg, se entrena igualmente.")
    y_log = np.log1p(rain_df["precip"].values)
    models["precip_reg"].fit(rain_df[feat_cols].values, y_log)

    pred = recursive_forecast(df, feat_cols, models)

    os.makedirs("data/predictions", exist_ok=True)
    pred.to_csv(OUT, index=False)
    print("✅ Forecast avanzado guardado en:", OUT)
    print(pred.head(7).to_string(index=False))


if __name__ == "__main__":
    main()
