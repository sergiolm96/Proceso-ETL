# scripts/etl/extract.py

import requests
from requests.adapters import HTTPAdapter, Retry

def fetch_weather_data(lat, lon, start, end):
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        "&hourly=temperature_2m,relative_humidity_2m"
        "&timezone=auto"
    )

    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        # No abortamos todo el ETL, devolvemos None para indicar fallo
        print(f"❌ Error al obtener datos de ({lat},{lon}): {e}")
        return None
