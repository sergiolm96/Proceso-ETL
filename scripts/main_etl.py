from datetime import datetime, timedelta
from scripts.config import get_config
from scripts.etl.extract import fetch_weather_data
from scripts.etl.transform import transform_weather_data
from scripts.etl.load import load_to_bigquery
import pandas as pd
import time

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

def main():
    cfg = get_config()

    # Día anterior
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    all_data = []

    for city in CITIES:

        try:
            raw = fetch_weather_data(city["lat"], city["lon"], date_str, date_str)
            if raw is None:
                continue
            df = transform_weather_data(raw, city["name"])

            if not df.empty:
                all_data.append(df)
            else:
                print("⚠️ Sin datos")

        except Exception as e:
            print(f"❌ Error con {city['name']}: {e}")

        time.sleep(0.5)  # Puedes reducirlo si quieres más velocidad

    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        load_to_bigquery(
            df=full_df,
            project_id=cfg["project_id"],
            dataset_id=cfg["dataset_id"],
            table_name=cfg["table_name"],
            credentials_path=cfg["credentials_path"]
        )
        print(f"\n✅ Proceso completado. Total de filas insertadas: {len(full_df)}")
    else:
        print("⚠️ No se insertaron datos. DataFrames vacíos.")

if __name__ == "__main__":
    main()
