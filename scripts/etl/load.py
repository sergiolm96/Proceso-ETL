from pandas_gbq import to_gbq
from google.oauth2 import service_account

def load_to_bigquery(df, project_id, dataset_id, table_name, credentials_path):
    table_id = f"{dataset_id}.{table_name}"
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    to_gbq(
        dataframe=df,
        destination_table=table_id,
        project_id=project_id,
        credentials=credentials,
        if_exists="append"
    )

    print(f"✅ {len(df)} filas cargadas en {project_id}.{table_id}")

