import sys
import os

# Agrega el path del proyecto raíz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.config import get_config
from scripts.etl.extract import fetch_weather_data
import json

if __name__ == "__main__":
    cfg = get_config()
    raw_data = fetch_weather_data(
        latitude=cfg["latitude"],
        longitude=cfg["longitude"],
        start_date=cfg["start_date"],
        end_date=cfg["end_date"]
    )

    print(json.dumps(raw_data, indent=2)[:1000])  # Para inspección parcial
