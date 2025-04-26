
from prefect import flow, task
from datetime import datetime
import pandas as pd

@task
def extract():
    # Simulación de extracción desde SQL Server
    print("Extrayendo datos...")
    data = {'id': [1, 2], 'nombre': ['producto1', 'producto2'], 'cantidad': [100, 200]}
    df = pd.DataFrame(data)
    return df

@task
def transform(df):
    print("Transformando datos...")
    df['fecha_proceso'] = datetime.now()
    return df

@task
def load(df):
    print("Cargando datos en PostgreSQL...")
    print(df.to_string(index=False))

@flow(name="sync_diarco_flow")
def sync_diarco_flow():
    df_raw = extract()
    df_transformed = transform(df_raw)
    load(df_transformed)

if __name__ == "__main__":
    sync_diarco_flow()
