import glob
import pandas as pd
import streamlit as st
from datetime import datetime
import os, glob
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=5 * 60 * 1000, key="auto_refresh")



PARQUET_DIR = "data/processed/aemet/municipio_diaria_parquet"
PRED_CSV = "data/predictions/temp_max_forecast.csv"


# ============ UI (CSS) ============
st.set_page_config(page_title="P3 Meteo | Barcelona", page_icon="🌦️", layout="wide")

st.markdown("""
<style>
/* General */
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
h1, h2, h3 { letter-spacing: -0.02em; }
.small-muted { color: rgba(255,255,255,0.65); font-size: 0.9rem; }

/* Top bar */
.topbar {
  padding: 18px 18px;
  border-radius: 18px;
  background: linear-gradient(135deg, rgba(70,130,180,0.35), rgba(0,0,0,0.2));
  border: 1px solid rgba(255,255,255,0.08);
}
.topbar-title { font-size: 1.6rem; font-weight: 750; }
.topbar-sub { margin-top: 2px; }

/* Cards */
.card {
  padding: 16px 16px;
  border-radius: 18px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
}
.card-kpi {
  padding: 14px 14px;
  border-radius: 18px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
}
.kpi-label { font-size: 0.85rem; color: rgba(255,255,255,0.65); }
.kpi-value { font-size: 1.35rem; font-weight: 750; margin-top: 2px; }

/* Forecast cards */
.fcard {
  padding: 14px 14px;
  border-radius: 18px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  height: 165px;
}
.fdate { font-weight: 750; font-size: 1.0rem; }
.fline { margin-top: 6px; font-size: 0.95rem; }
.badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(70,130,180,0.20);
  border: 1px solid rgba(70,130,180,0.35);
  font-size: 0.85rem;
}
</style>
""", unsafe_allow_html=True)

# ============ Helpers ============
ESTADO_MAP = {
    "11": "Despejado",
    "12": "Poco nuboso",
    "13": "Intervalos nubosos",
    "14": "Nuboso",
    "15": "Muy nuboso",
    "16": "Cubierto",
    "43": "Lluvia",
    "44": "Lluvia + nubes",
    "45": "Tormenta",
}

def estado_readable(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return ESTADO_MAP.get(s, s)

def emoji_by_precip(p):
    try:
        p = int(p)
    except:
        return "☁️"
    if p >= 60:
        return "🌧️"
    if p >= 30:
        return "🌦️"
    return "☀️"

# ============ Load data ============
files = glob.glob(f"{PARQUET_DIR}/dt=*/**/*.parquet", recursive=True)
if not files:
    st.error("No se encontraron archivos Parquet. Ejecuta primero: python spark_etl_aemet.py")
    st.stop()

df = pd.read_parquet(PARQUET_DIR)
# --- Fix: asegurar que dt es datetime (a veces se lee como Categorical) ---
if "dt" in df.columns:
    # Convertir a string primero para evitar Categorical issues
    df["dt"] = pd.to_datetime(df["dt"].astype(str), errors="coerce")
else:
    # Si dt no viene como columna (por partición), intentamos recuperarlo desde el índice si aplica
    df = df.reset_index()
    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"].astype(str), errors="coerce")

# Eliminar filas sin fecha válida
df = df.dropna(subset=["dt"])
df["dt"] = pd.to_datetime(df["dt"])
df = df.sort_values("dt")

# Ensure columns
for col in ["temp_max","temp_min","hum_max","hum_min","prob_precip_max","estado_cielo","municipio_nombre","fecha_carga"]:
    if col not in df.columns:
        df[col] = None

df["estado_cielo"] = df["estado_cielo"].apply(estado_readable)

# ============ Top bar ============
muni = df["municipio_nombre"].dropna().iloc[0] if len(df) else "Barcelona"
last_load = df["fecha_carga"].dropna().iloc[0] if df["fecha_carga"].notna().any() else ""
st.markdown(f"""
<div class="topbar">
  <div class="topbar-title">🌦️ P3 Meteo | {muni}</div>
  <div class="topbar-sub small-muted">
    Pipeline: AEMET → Raw JSON → Spark ETL → Parquet → Dashboard · Última carga: <span class="badge">{last_load}</span>
  </div>
</div>
""", unsafe_allow_html=True)

st.write("")

# ============ Filters ============
# Calcular fechas mínimas y máximas de forma segura
min_dt = pd.to_datetime(df["dt"].astype(str), errors="coerce").min()
max_dt = pd.to_datetime(df["dt"].astype(str), errors="coerce").max()

with st.container():
    c_refresh, c_toggle = st.columns([1, 3])

    with c_refresh:
        if st.button("🔄 Actualizar datos (AEMET)"):
            with st.spinner("Actualizando datos desde AEMET..."):
                os.system("python fetch_aemet_barcelona.py")
                os.system("python spark_etl_aemet.py")
                os.system("python model_train_predict.py")
            st.success("Datos actualizados correctamente")

            try:
                st.rerun()
            except AttributeError:
                st.experimental_rerun()

    with c_toggle:
        show_table = st.toggle("Mostrar tabla completa", value=False)
    




#mask = (df["dt"].dt.date >= start) & (df["dt"].dt.date <= end)
#dff = df.loc[mask].copy()
dff = df.copy()

# Eliminar duplicados por fecha (quedarnos con la última carga)
if "fecha_carga" in dff.columns:
    dff = dff.sort_values("fecha_carga")
dff = dff.drop_duplicates(subset=["dt"], keep="last")
dff = dff.sort_values("dt")


# ============ KPI Row ============
k1, k2, k3, k4 = st.columns(4)
today = datetime.now().strftime("%Y-%m-%d")
next_day = dff.iloc[0] if not dff.empty else None


def kpi(col, label, value):
    with col:
        st.markdown(f"""
        <div class="card-kpi">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
        </div>
        """, unsafe_allow_html=True)

kpi(k1, "Fecha de hoy", today)
kpi(k2, "Días disponibles", f"{len(dff)}")
if next_day is not None:
    kpi(k3, "Próximo día (Tº min/max)", f"{next_day['temp_min']}° / {next_day['temp_max']}°")
    kpi(k4, "Próximo día (precip max)", f"{next_day['prob_precip_max']}%")
else:
    kpi(k3, "Próximo día (Tº min/max)", "-")
    kpi(k4, "Próximo día (precip max)", "-")

st.write("")

# =========================
# Predicción KPI (si existe)
# =========================
import os

PRED_CSV = "data/predictions/temp_max_forecast.csv"

if os.path.exists(PRED_CSV):
    pred_df = pd.read_csv(PRED_CSV)
    pred_df["dt"] = pd.to_datetime(pred_df["dt"])

    st.subheader("🔮 Predicción temperatura máxima (modelo)")
    p1, p2, p3 = st.columns(3)

    def pred_kpi(col, label, value):
        with col:
            st.markdown(f"""
            <div class="card-kpi">
              <div class="kpi-label">{label}</div>
              <div class="kpi-value">{value}°C</div>
            </div>
            """, unsafe_allow_html=True)

    if len(pred_df) >= 1:
        pred_kpi(p1, "Mañana", round(float(pred_df.iloc[0]["pred_temp_max"]), 1))
    if len(pred_df) >= 2:
        pred_kpi(p2, "En 2 días", round(float(pred_df.iloc[1]["pred_temp_max"]), 1))
    if len(pred_df) >= 3:
        pred_kpi(p3, "En 3 días", round(float(pred_df.iloc[2]["pred_temp_max"]), 1))

    st.write("")


# ============ Forecast Cards ============
st.subheader("🗓️ Resumen (7 días)")

from datetime import date
hoy = pd.to_datetime(date.today()).normalize()

dff_sorted = dff.sort_values("dt").copy()
top7 = dff_sorted[dff_sorted["dt"] >= hoy].head(7)

if top7.empty:
    top7 = dff_sorted.head(7)

cols = st.columns(7 if len(top7) >= 7 else max(1, len(top7)))
for i in range(len(cols)):
    if i >= len(top7):
        cols[i].empty()
        continue
    row = top7.iloc[i]
    icon = emoji_by_precip(row["prob_precip_max"])
    with cols[i]:
        st.markdown(f"""
        <div class="fcard">
          <div class="fdate">{row['dt'].strftime('%d/%m')}</div>
          <div class="fline">{icon} <b>{row['temp_min']}°</b> / <b>{row['temp_max']}°</b></div>
          <div class="fline">💧 {row['hum_min']}% / {row['hum_max']}%</div>
          <div class="fline">🌧️ {row['prob_precip_max']}%</div>
          <div class="fline"><span class="badge">{row['estado_cielo']}</span></div>
        </div>
        """, unsafe_allow_html=True)


# ============ Charts ============
st.subheader("📈 Visualización")
c1, c2 = st.columns(2)

with c1:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.write("Temperatura observada (máx / mín)")

    # Serie real
    chart_real = dff.set_index("dt")[["temp_max", "temp_min"]].copy()
    st.line_chart(chart_real)

    st.caption("Datos observados / previstos por AEMET (máx y mín diarios).")

    # ======================
    # Predicción separada
    # ======================
    import os
    import pandas as pd

    PRED_CSV = "data/predictions/temp_max_forecast.csv"

    if os.path.exists(PRED_CSV):
        st.write("Predicción temperatura máxima (modelo)")
        pred_df = pd.read_csv(PRED_CSV)
        pred_df["dt"] = pd.to_datetime(pred_df["dt"])
        pred_df = pred_df.set_index("dt")[["pred_temp_max"]]

        st.line_chart(pred_df)

        st.caption(
            "Predicción generada mediante un modelo de regresión lineal simple. "
            "Con pocos datos históricos, la tendencia es estable; mejora al acumular más datos."
        )

    st.markdown('</div>', unsafe_allow_html=True)



with c2:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.write("Probabilidad máxima de precipitación (%)")
    chart_pp = dff.set_index("dt")[["prob_precip_max"]]
    st.bar_chart(chart_pp)
    st.markdown('</div>', unsafe_allow_html=True)

st.write("")

# Optional table
if show_table:
    st.subheader("📋 Tabla de datos (vista analítica)")
    st.caption("Vista detallada de los datos procesados tras aplicar los filtros de fecha.")

    cols = [
        "dt",
        "temp_min",
        "temp_max",
        "hum_min",
        "hum_max",
        "prob_precip_max",
        "estado_cielo",
        "fecha_carga",
    ]

    st.dataframe(
        dff[cols]
        .sort_values("dt")
        .reset_index(drop=True),
        use_container_width=True
    )


st.caption("Nota: estado del cielo es una conversión básica de códigos AEMET para fines de visualización.")
