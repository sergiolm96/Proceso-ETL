import pandas as pd

def transform_weather_data(raw_data: dict, city: str) -> pd.DataFrame:
    hourly = raw_data.get("hourly", {})
    df = pd.DataFrame(hourly)

    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"])
    df["latitude"] = raw_data.get("latitude")
    df["longitude"] = raw_data.get("longitude")
    df["city"] = city

    return df
