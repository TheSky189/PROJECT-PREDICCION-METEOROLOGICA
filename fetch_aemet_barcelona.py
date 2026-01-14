import os
import json
import time
import requests
from datetime import datetime, date
from calendar import monthrange
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.getenv("AEMET_API_KEY")
if not API_KEY:
    raise SystemExit("Falta AEMET_API_KEY en variables de entorno")

BASE = "https://opendata.aemet.es/opendata/api"

# Se recomienda usar Fabra: más estable/histórico (aunque El Prat también vale)
ESTACION = "0200E"  # Barcelona / Fabra (recomendado)
# ESTACION = "0201D"  # El Prat (si quieres)

RAW_DIR = "data/raw/aemet/clima_diaria"
os.makedirs(RAW_DIR, exist_ok=True)

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=2.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

session = make_session()

def get_datos(url: str):
    """
    AEMET:
    1) llamada al endpoint -> JSON con 'datos' (URL real)
    2) segunda llamada a 'datos' -> lista de registros
    """
    # timeout: (connect, read)
    r1 = session.get(url, params={"api_key": API_KEY}, timeout=(30, 180))
    r1.raise_for_status()
    meta = r1.json()
    datos_url = meta.get("datos")
    if not datos_url:
        raise RuntimeError(f"No viene 'datos' en respuesta: {meta}")

    # pequeña pausa antes de descargar el payload grande
    time.sleep(1.2)

    r2 = session.get(datos_url, timeout=(30, 240))
    r2.raise_for_status()
    return r2.json()

def build_url(fechaini: str, fechafin: str) -> str:
    return (
        f"{BASE}/valores/climatologicos/diarios/datos/"
        f"fechaini/{fechaini}/fechafin/{fechafin}/estacion/{ESTACION}"
    )

def iter_month_ranges(start_year: int, end_year: int):
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            last_day = monthrange(y, m)[1]
            yield y, m, date(y, m, 1), date(y, m, last_day)

def main():

    start_year = 2022
    end_year = 2025

    total_files = 0

    for y, m, d1, d2 in iter_month_ranges(start_year, end_year):
        fechaini = f"{d1.isoformat()}T00:00:00UTC"
        fechafin = f"{d2.isoformat()}T23:59:59UTC"
        url = build_url(fechaini, fechafin)

        print(f"Descargando {y}-{m:02d} ({ESTACION}) ...")

        # reintentos: si hay timeout o limitación, esperar más tiempo
        for attempt in range(1, 6):
            try:
                data = get_datos(url)
                break
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
                wait = 10 * attempt
                print(f" Timeout (intento {attempt}/5). Esperando {wait}s... {e}")
                time.sleep(wait)
            except requests.exceptions.HTTPError as e:
                # 429/5XX Posiblemente caiga aquí también
                wait = 15 * attempt
                print(f" HTTPError (intento {attempt}/5). Esperando {wait}s... {e}")
                time.sleep(wait)
        else:
            print(f" Falló definitivamente {y}-{m:02d}. Continuando con el siguiente mes.")
            continue

        out_path = f"{RAW_DIR}/aemet_{ESTACION}_{y}_{m:02d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        print(f"✅ OK {y}-{m:02d}: {len(data)} registros -> {out_path}")
        total_files += 1

        # evitar AEMET rate limit
        time.sleep(2.5)

    print(f"\nFIN. Archivos guardados: {total_files}")

if __name__ == "__main__":
    main()
