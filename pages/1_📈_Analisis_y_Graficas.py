import os
import glob
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

# ============================================================
# Config
# ============================================================
st.set_page_config(page_title="Análisis y Gráficas", page_icon="📈", layout="wide")


# --- Sidebar Header (para multipage) ---
st.markdown("""
<style>

div[data-testid="stSidebarNav"] {
    padding-top: 8px;
}

div[data-testid="stSidebarNav"] > ul > li:first-child {
    margin-bottom: 12px;
}

div[data-testid="stSidebarNav"] > ul > li:first-child > div {
    background: rgba(255,255,255,0.08);
    border-radius: 10px;
    padding: 6px 10px;
    font-size: 13px;
    opacity: 0.75;
}

div[data-testid="stSidebarNav"] li a {
    border-radius: 10px;
    padding: 8px 10px;
    margin: 4px 0;
    transition: background 0.15s ease;
}

div[data-testid="stSidebarNav"] li a:hover {
    background: rgba(255,255,255,0.08);
}

div[data-testid="stSidebarNav"] li a[aria-current="page"] {
    background: linear-gradient(
        135deg,
        rgba(70,130,180,0.35),
        rgba(70,130,180,0.18)
    );
    font-weight: 600;
    border-left: 4px solid #5DA9E9;
    padding-left: 12px;
}

div[data-testid="stSidebarNav"] li a svg {
    margin-right: 6px;
}
</style>
""", unsafe_allow_html=True)


st.sidebar.markdown("""
<div class="sidebar-fixed-header">
  <div class="wrap">
    <img src="app/static/logo" style="display:none"/>
    <div style="font-size:0;"></div>
  </div>
</div>
""", unsafe_allow_html=True)

# Header real (con st.sidebar.image)
with st.sidebar:
    # este bloque se "verá" arriba gracias al CSS fixed
    st.image("document/logo_weather.png", width=78)
    st.markdown('<div class="title">METEO DASHBOARD</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub">Predicción meteorológica<br/>Big Data & IA</div>', unsafe_allow_html=True)
    st.markdown("---")

# ============================================================
# Minimal CSS (para que .card funcione también en esta página)
# ============================================================
st.markdown("""
<style>
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
.small-muted { color: rgba(255,255,255,0.65); font-size: 0.92rem; }
.card {
  padding: 16px 16px;
  border-radius: 18px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  margin-bottom: 12px;
}
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

st.title("📈 Análisis y Visualización de Datos (EDA)")
st.caption("Exploración de datos históricos AEMET: tendencias, distribución y correlaciones.")

# ============================================================
# Load data (igual que en tu app principal: Parquet o CSV)
# ============================================================
PARQUET_DIR = "data/processed/aemet/clima_diaria_parquet"
CSV_FALLBACK = "data/processed/aemet/clima_diaria_processed.csv"

df = None
data_source = "Desconocido"

# Try Parquet first
if os.path.exists(PARQUET_DIR):
    parquet_files = glob.glob(os.path.join(PARQUET_DIR, "**", "*.parquet"), recursive=True)
    if parquet_files:
        df = pd.read_parquet(PARQUET_DIR)
        data_source = "Parquet (Spark)"

# Fallback to CSV
if df is None:
    if os.path.exists(CSV_FALLBACK):
        df = pd.read_csv(CSV_FALLBACK, parse_dates=["dt"])
        data_source = "CSV (Windows fallback)"
    else:
        st.error("❌ No se encontraron datos procesados. Ejecuta: python spark_etl_aemet.py")
        st.stop()

if "dt" not in df.columns:
    st.error("❌ Columna 'dt' no encontrada")
    st.stop()

# Normalizar dt y ordenar
df["dt"] = pd.to_datetime(df["dt"].astype(str), errors="coerce")
df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

# Asegurar columnas esperadas (si falta alguna, la creamos)
base_cols = ["temp_max", "temp_min", "temp_med", "precip", "hum_med"]
for c in base_cols:
    if c not in df.columns:
        df[c] = pd.NA

# Convertir a numérico
for c in base_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# ============================================================
# Sidebar: filtros
# ============================================================
st.sidebar.header("Filtros")
min_dt = df["dt"].min().date()
max_dt = df["dt"].max().date()

desde = st.sidebar.date_input("Desde", value=min_dt, min_value=min_dt, max_value=max_dt)
hasta = st.sidebar.date_input("Hasta", value=max_dt, min_value=min_dt, max_value=max_dt)

mask = (df["dt"].dt.date >= desde) & (df["dt"].dt.date <= hasta)
dff = df.loc[mask].copy()

st.sidebar.markdown("---")
dropna_mode = st.sidebar.checkbox("Ocultar filas sin datos (NaN)", value=True)
if dropna_mode:
    dff = dff.dropna(subset=["temp_max", "temp_min", "temp_med", "precip", "hum_med"], how="all")

# ============================================================
# KPIs rápidos (cards) - usando contenedores reales
# ============================================================
k1, k2, k3, k4 = st.columns(4)

with k1:
    with st.container(border=True):
        st.markdown("**📅 Rango**")
        st.write(f"{desde} → {hasta}")

        # Fuente (más discreto)
        fuente = "AEMET · CSV/Parquet"
        if "Windows fallback" in " ".join(["CSV_FALLBACK"]):  # no afecta, es solo texto
            pass
        st.caption(f"Fuente: {fuente}")

with k2:
    with st.container(border=True):
        st.markdown("**🧾 Registros**")
        st.write(f"{len(dff):,}")
        st.caption("Filtrado por rango de fechas")

with k3:
    with st.container(border=True):
        st.markdown("**🌡️ Temp media**")
        val = dff["temp_med"].mean()
        st.write(f"{val:.1f} °C" if pd.notna(val) else "-")
        st.caption("Promedio en el rango")

with k4:
    with st.container(border=True):
        st.markdown("**🌧️ Días con lluvia**")
        if dff["precip"].notna().any():
            st.write(f"{int((dff['precip'].fillna(0) > 0).sum()):,}")
        else:
            st.write("-")
        st.caption("precipitación > 0 mm")

st.markdown("---")

# ============================================================
# 1) Serie temporal temperatura (línea)
# ============================================================
st.subheader("1) Evolución temporal: Temperatura (máx/min/med)")
st.caption("Por qué: un gráfico de línea es ideal para observar tendencia y estacionalidad a lo largo del tiempo.")

line_df = dff[["dt", "temp_max", "temp_min", "temp_med"]].dropna(subset=["dt"], how="any").copy()
fig_line = px.line(
    line_df,
    x="dt",
    y=["temp_max", "temp_min", "temp_med"],
    labels={"value": "Temperatura (°C)", "dt": "Fecha", "variable": "Serie"},
)
st.plotly_chart(fig_line, use_container_width=True)

# ============================================================
# 2) Precipitación diaria (barras)
# ============================================================
st.subheader("2) Precipitación diaria (mm)")
st.caption("Por qué: barras permiten comparar cantidades por fecha y detectar picos de lluvia.")

pp = dff[["dt", "precip"]].copy()
pp["precip"] = pp["precip"].fillna(0.0)
fig_pp = px.bar(pp, x="dt", y="precip", labels={"precip": "Precipitación (mm)", "dt": "Fecha"})
st.plotly_chart(fig_pp, use_container_width=True)

# ============================================================
# 3) Boxplot por mes (distribución)
# ============================================================
st.subheader("3) Distribución por mes (Boxplot de temperatura media)")
st.caption("Por qué: boxplot muestra mediana, dispersión y outliers; perfecto para comparar meses.")

tmp = dff[["dt", "temp_med"]].dropna().copy()
tmp["mes"] = tmp["dt"].dt.month.astype(int)
tmp["mes_nombre"] = tmp["dt"].dt.strftime("%b")  # Jan, Feb...
tmp = tmp.sort_values("mes")

fig_box = px.box(tmp, x="mes_nombre", y="temp_med", labels={"temp_med": "Temp media (°C)", "mes_nombre": "Mes"})
st.plotly_chart(fig_box, use_container_width=True)

# ============================================================
# 4) Scatter: correlación
# ============================================================
st.subheader("4) Relación entre variables (Scatter)")
st.caption("Por qué: scatter ayuda a ver relación/correlación y patrones entre dos variables.")

cA, cB = st.columns(2)
vars_opts = ["temp_med", "temp_max", "temp_min", "hum_med", "precip"]

with cA:
    x_var = st.selectbox("Eje X", vars_opts, index=0)
with cB:
    # seleccion default diferente
    default_y = 1 if vars_opts[1] != x_var else 2
    y_var = st.selectbox("Eje Y", vars_opts, index=min(default_y, len(vars_opts)-1))

# si el usuario elige la misma variable: no da error, muestra una advertencia y grafica y=x (una línea)
if x_var == y_var:
    st.warning("Eje X y Eje Y son iguales. Se muestra la variable contra sí misma (línea y = x).")

    sc = dff[["dt", x_var]].dropna().copy()
    sc = sc.rename(columns={x_var: "x"})
    sc["y"] = sc["x"]

    fig_sc = px.scatter(sc, x="x", y="y", hover_data=["dt"], labels={"x": x_var, "y": y_var})
    st.plotly_chart(fig_sc, use_container_width=True)

else:
    sc = dff[["dt", x_var, y_var]].dropna().copy()
    fig_sc = px.scatter(sc, x=x_var, y=y_var, hover_data=["dt"], labels={x_var: x_var, y_var: y_var})
    st.plotly_chart(fig_sc, use_container_width=True)

# ============================================================
# 5) Matriz de correlación (heatmap)
# ============================================================
st.subheader("5) Matriz de correlación (Heatmap)")
st.caption("Por qué: un heatmap resume de forma visual la relación lineal entre múltiples variables.")

corr_df = dff[base_cols].copy()
corr = corr_df.corr(numeric_only=True).fillna(0.0)

fig_hm = px.imshow(
    corr,
    text_auto=True,
    labels=dict(x="Variables", y="Variables", color="Correlación"),
)
st.plotly_chart(fig_hm, use_container_width=True)

# ============================================================
# Conclusiones
# ============================================================
st.markdown("---")
st.subheader("✅ Conclusiones del análisis")
st.write("""
- Se observan patrones estacionales claros en la temperatura (ciclos anuales).
- La precipitación presenta mayor variabilidad y picos aislados, por lo que es útil modelarla como probabilidad + cantidad.
- La correlación ayuda a justificar la selección de variables y el enfoque de modelización.
""")
