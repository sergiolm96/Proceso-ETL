import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.config import get_config
from scripts.etl.extract import fetch_weather_data
from scripts.etl.transform import transform_weather_data

if __name__ == "__main__":
    cfg = get_config()
    raw_data = fetch_weather_data(
        latitude=cfg["latitude"],
        longitude=cfg["longitude"],
        start_date=cfg["start_date"],
        end_date=cfg["end_date"]
    )
    
    df = transform_weather_data(raw_data)
    print(df.head())
    