import os
from dotenv import load_dotenv

# Solo carga .env si se está ejecutando fuera de Docker
if os.getenv("DOCKER_ENV") != "true":
    load_dotenv()

def get_config():
    config = {
        "project_id": os.getenv("GCP_PROJECT_ID"),
        "dataset_id": os.getenv("GCP_DATASET_ID"),
        "table_name": os.getenv("GCP_TABLE_NAME"),
        "credentials_path": os.getenv("GCP_CREDENTIALS_PATH"),
    }

    # Validación básica
    for k, v in config.items():
        if not v:
            raise ValueError(f"Missing config variable: {k}")

    return config
