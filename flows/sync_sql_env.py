
from prefect import flow, task
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar variables del archivo .env
load_dotenv()

@task
def extract_from_sql_server():
    print("Extrayendo datos desde SQL Server...")
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(conn_str)

    query = """
    SELECT TOP 10 * FROM Productos
    """
    df = pd.read_sql(query, engine)
    return df

@task
def load_to_postgres(df):
    print("Cargando datos en PostgreSQL...")
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASSWORD")

    pg_conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    pg_engine = create_engine(pg_conn_str)

    df.to_sql("productos_tmp", pg_engine, if_exists="replace", index=False)
    print("Carga exitosa.")

@flow(name="sync_sql_env")
def sync_sql_env():
    df = extract_from_sql_server()
    load_to_postgres(df)

if __name__ == "__main__":
    sync_sql_env()
