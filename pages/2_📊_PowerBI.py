import os
from pathlib import Path
import streamlit as st

st.set_page_config(
    page_title="Power BI · Visualización",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Power BI · Visualización de datos meteorológicos")
st.caption("Gráficos generados en Power BI y exportados como imágenes para su análisis e interpretación.")

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

st.markdown("""
En esta sección se muestran **visualizaciones creadas en Power BI** a partir de los datos
meteorológicos procesados previamente.

El objetivo de estas visualizaciones es:
- Presentar los datos de forma **clara y visual**.
- Facilitar la **comparación temporal** (por años y por meses).
- Analizar **relaciones entre variables climáticas** como temperatura, precipitación y humedad.
- Complementar los gráficos interactivos de Streamlit con un enfoque más **analítico y de dashboard**.
""")

st.divider()

# ==========================
# Directorio de imágenes
# ==========================
IMG_DIR = Path("document/powerbi")

def show_img(filename: str, caption: str, explanation: str):
    path = IMG_DIR / filename
    if path.exists():
        st.image(str(path), use_container_width=True, caption=caption)
        st.markdown(explanation)
    else:
        st.warning(f"No se encuentra la imagen: {path}")

# ==========================
# Análisis anual
# ==========================
st.subheader("📅 Comparación anual")

st.markdown("""
Estos gráficos muestran un **resumen anual** de las principales variables meteorológicas.
Permiten comparar cómo varían los valores entre distintos años y detectar
posibles cambios o patrones a largo plazo.
""")

tabs = st.tabs(["2022", "2023", "2024", "2025"])

with tabs[0]:
    show_img(
        "pbi_2022.png",
        "Resumen climático 2022",
        "Incluye valores medios de temperatura y precipitación del año 2022. "
        "Sirve como referencia base para comparar con años posteriores."
    )

with tabs[1]:
    show_img(
        "pbi_2023.png",
        "Resumen climático 2023",
        "Permite observar variaciones respecto a 2022, destacando posibles cambios en temperatura y lluvia."
    )

with tabs[2]:
    show_img(
        "pbi_2024.png",
        "Resumen climático 2024",
        "Ayuda a identificar tendencias intermedias y posibles anomalías climáticas."
    )

with tabs[3]:
    show_img(
        "pbi_2025.png",
        "Resumen climático 2025",
        "Muestra los datos más recientes disponibles y facilita la comparación con años anteriores."
    )

st.divider()

# ==========================
# Análisis mensual
# ==========================
st.subheader("📈 Análisis por meses")

col1, col2 = st.columns(2)

with col1:
    show_img(
        "pbi_temp_mes.png",
        "Temperatura media por mes (°C)",
        "Este gráfico muestra la evolución mensual de la temperatura media. "
        "Es útil para identificar la **estacionalidad** y los meses más cálidos o fríos."
    )

    show_img(
        "pbi_scatter_temp_hum.png",
        "Relación entre temperatura y humedad",
        "Diagrama de dispersión que permite analizar si existe relación entre "
        "la temperatura media y la humedad media."
    )

with col2:
    show_img(
        "pbi_precip_mes.png",
        "Precipitación media por mes (mm)",
        "Permite visualizar la distribución de las lluvias a lo largo del año, "
        "identificando los meses más secos y más lluviosos."
    )

    show_img(
        "tiempo_medio.png",
        "Temperatura media global",
        "Tarjeta KPI que resume el valor medio de la temperatura en todo el periodo analizado."
    )

st.divider()

# ==========================
# Cierre
# ==========================
st.subheader("🧾 Interpretación general")

st.markdown("""
Las visualizaciones de Power BI permiten:
- Sintetizar grandes volúmenes de datos en gráficos comprensibles.
- Detectar patrones temporales y estacionales.
- Analizar relaciones entre distintas variables climáticas.
- Comunicar los resultados de forma clara y visual.

Estas gráficas complementan el análisis exploratorio y ayudan a interpretar
los datos meteorológicos desde una perspectiva más visual.
""")
