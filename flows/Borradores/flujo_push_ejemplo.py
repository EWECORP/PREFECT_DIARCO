from prefect import flow, task
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

@task
def extraer_datos_sqlserver():
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=data_sync;UID=sa;PWD=yourStrong(!)Password"
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql("SELECT TOP 100 * FROM ventas", conn)
    return df

@task
def cargar_datos_postgres(df):
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/diarco_data")
    df.to_sql("ventas_sync", engine, if_exists="replace", index=False)

@flow
def flujo_push_ejemplo():
    df = extraer_datos_sqlserver()
    cargar_datos_postgres(df)

if __name__ == "__main__":
    flujo_push_ejemplo()