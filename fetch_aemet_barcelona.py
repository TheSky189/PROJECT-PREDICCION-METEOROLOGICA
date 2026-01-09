import os
import json
import time
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.getenv("AEMET_API_KEY")
if not API_KEY:
    raise SystemExit("Falta AEMET_API_KEY en variables de entorno")

MUNICIPIO = "08019"  # Barcelona

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,                # 重试次数
        backoff_factor=1.5,     # 1.5, 3, 4.5... 秒
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = make_session()

# 1) Primera llamada: devuelve URL en campo "datos"
url1 = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/diaria/{MUNICIPIO}?api_key={API_KEY}"
print("Request 1: AEMET diaria municipio", MUNICIPIO)

try:
    r1 = session.get(url1, timeout=90)   # ✅ timeout 拉长到 90s
    r1.raise_for_status()
except requests.exceptions.RequestException as e:
    raise SystemExit(f"Error en Request 1 (AEMET): {e}")

meta = r1.json()
if "datos" not in meta:
    raise SystemExit(f"AEMET no devolvió 'datos'. Respuesta: {meta}")

datos_url = meta["datos"]
print("Datos URL:", datos_url)

# 2) Segunda llamada: JSON real
try:
    r2 = session.get(datos_url, timeout=90)
    r2.raise_for_status()
except requests.exceptions.RequestException as e:
    raise SystemExit(f"Error en Request 2 (datos_url): {e}")

data = r2.json()

# Guardar JSON crudo
out_dir = "data/raw/aemet/municipio_diaria"
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs(out_dir, exist_ok=True)

out_path = f"{out_dir}/barcelona_08019_{ts}.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(" OK guardado:", out_path)
print(" Registros:", len(data))
