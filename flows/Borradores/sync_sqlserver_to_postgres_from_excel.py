
import os
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Conexiones
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

sqlserver_bases = {
    "DiarcoP": f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/DiarcoP?driver=ODBC+Driver+17+for+SQL+Server",
    "DiarcoEst": f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/DiarcoEst?driver=ODBC+Driver+17+for+SQL+Server"
}

pg_conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
pg_engine = create_engine(pg_conn_str)

@task
def leer_config_excel(path):
    df = pd.read_excel(path)
    df = df.fillna("")
    return df.to_dict(orient="records")

@task
def extraer_y_cargar_filtro(base, tabla, filtro):
    try:
        print(f"🔄 Procesando {base}.{tabla} ...")
        sql_engine = create_engine(sqlserver_bases[base])
        query = f"SELECT * FROM {tabla}"
        if filtro:
            query += f" WHERE {filtro}"
        print(f"📝 Ejecutando: {query}")
        df = pd.read_sql(query, sql_engine)
        print(f"✅ {len(df)} registros extraídos de {tabla}.")
        df.to_sql(tabla.lower(), pg_engine, schema="src", if_exists="replace", index=False)
        print(f"📥 Cargado en PostgreSQL → src.{tabla.lower()}")
    except Exception as e:
        print(f"❌ Error en {tabla}: {e}")

@flow(name="sync_sqlserver_to_postgres_from_excel")
def sync_sqlserver_to_postgres_from_excel():
    config_path = "config/tablas_para_sincronizar.xlsx"
    registros = leer_config_excel(config_path)
    for fila in registros:
        extraer_y_cargar_filtro(fila["Base de Datos"], fila["Tabla / Vista"], fila["Filtro"])

if __name__ == "__main__":
    sync_sqlserver_to_postgres_from_excel()
