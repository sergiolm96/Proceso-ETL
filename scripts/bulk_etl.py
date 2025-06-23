import time
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# ------------------------------
# Configuración y entorno
# ------------------------------
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = os.getenv("GCP_DATASET_ID")
TABLE_NAME = os.getenv("GCP_TABLE_NAME")
CREDENTIALS_PATH = os.getenv("GCP_CREDENTIALS_PATH")

TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# ------------------------------
# Lista de ciudades (nombre, latitud, longitud)
# ------------------------------
CITIES = [
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"name": "Barcelona", "lat": 41.3851, "lon": 2.1734},
    {"name": "Valencia", "lat": 39.4699, "lon": -0.3763},
    {"name": "Sevilla", "lat": 37.3891, "lon": -5.9845},
    {"name": "Bilbao", "lat": 43.2630, "lon": -2.9350},
    {"name": "Zaragoza", "lat": 41.6488, "lon": -0.8891},
    {"name": "Málaga", "lat": 36.7213, "lon": -4.4214},
    {"name": "Palma", "lat": 39.5696, "lon": 2.6502},  # Palma de Mallorca
    {"name": "Las Palmas", "lat": 28.1235, "lon": -15.4363},  # Las Palmas de Gran Canaria
    {"name": "Santa Cruz de Tenerife", "lat": 28.4636, "lon": -16.2518},
    {"name": "Santander", "lat": 43.4623, "lon": -3.8099},
    {"name": "Santiago de Compostela", "lat": 42.8782, "lon": -8.5448},
    {"name": "Cáceres", "lat": 39.4760, "lon": -6.3722},
    {"name": "León", "lat": 42.5987, "lon": -5.5671},
    {"name": "Salamanca", "lat": 40.9701, "lon": -5.6635},
    {"name": "Guadalajara", "lat": 40.6333, "lon": -3.1667},
]


# ------------------------------
# Rango de fechas semanal
# ------------------------------
START_DATE = datetime.strptime("2025-01-01", "%Y-%m-%d")
END_DATE = datetime.strptime("2025-06-22", "%Y-%m-%d")
WEEK_RANGE = [
    (START_DATE + timedelta(days=i), min(START_DATE + timedelta(days=i + 6), END_DATE))
    for i in range(0, (END_DATE - START_DATE).days + 1, 7)
]

# ------------------------------
# Funciones auxiliares
# ------------------------------
def fetch_weather_data(lat, lon, start_date, end_date):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=temperature_2m,relative_humidity_2m"
        f"&timezone=auto"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def transform_weather_data(raw_data, city_name):
    hourly = raw_data.get("hourly", {})
    df = pd.DataFrame(hourly)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"])
    df["latitude"] = raw_data.get("latitude")
    df["longitude"] = raw_data.get("longitude")
    df["city"] = city_name
    return df

def load_to_bigquery(df):
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"✅ Cargadas {len(df)} filas en {TABLE_ID}")

# ------------------------------
# Ejecución principal
# ------------------------------
def main():
    total_filas = 0

    for city in CITIES:
        for start, end in WEEK_RANGE:
            start_str = start.strftime("%Y-%m-%d")
            end_str = end.strftime("%Y-%m-%d")
            print(f"📡 Descargando {city['name']} - Semana: {start_str} a {end_str}")

            try:
                raw = fetch_weather_data(city["lat"], city["lon"], start_str, end_str)
                df = transform_weather_data(raw, city["name"])

                if not df.empty:
                    load_to_bigquery(df)
                    total_filas += len(df)
                else:
                    print("⚠️ Datos vacíos")

            except Exception as e:
                print(f"❌ Error con {city['name']} ({start_str} - {end_str}): {e}")

            time.sleep(1)  

    print(f"\n✅ Proceso completo. Total de filas cargadas: {total_filas}")

if __name__ == "__main__":
    main()
