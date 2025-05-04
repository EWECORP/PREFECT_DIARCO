
import os
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables del entorno
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Conexiones
sql_engine_str = f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}?driver=ODBC+Driver+17+for+SQL+Server"
sql_engine = create_engine(sql_engine_str)

pg_conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
pg_engine = create_engine(pg_conn_str)

@task
def replicar_en_sqlserver(linkedserver, base, tabla, esquema_destino, filtro=""):
    nombre_destino = f"{esquema_destino}.{tabla}"
    query = f"SELECT * INTO {nombre_destino} FROM [{linkedserver}].[{base}].[dbo].[{tabla}]"
    if filtro:
        query += f" WHERE {filtro}"
    print(f"🛠 Ejecutando SQL: {query}")
    with sql_engine.connect() as conn:
        conn.execute(text(f"IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}"))
        conn.execute(text(query))
    print(f"✅ Tabla {nombre_destino} replicada localmente.")

@task
def cargar_en_postgres(esquema_origen, tabla):
    df = pd.read_sql(f"SELECT * FROM {esquema_origen}.{tabla}", sql_engine)
    print(f"📦 {len(df)} registros leídos de {esquema_origen}.{tabla}")
    df.to_sql(tabla.lower(), pg_engine, schema="src", if_exists="replace", index=False)
    print(f"📥 Cargado en PostgreSQL: src.{tabla.lower()}")

@flow(name="replicar_y_cargar_t050_articulos")
def replicar_y_cargar_t050_articulos():
    replicar_en_sqlserver("PROD_DiarcoP", "DiarcoP", "T050_ARTICULOS", "replica_diarcop", "M_BAJA = 'N'")
    cargar_en_postgres("replica_diarcop", "T050_ARTICULOS")

if __name__ == "__main__":
    replicar_y_cargar_t050_articulos()
