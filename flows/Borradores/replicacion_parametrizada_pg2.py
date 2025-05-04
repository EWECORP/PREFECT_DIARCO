
import os
import pandas as pd
import psycopg2 as pg2
from prefect import flow, task
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables del entorno
load_dotenv()

# Conexión a SQL Server
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

# Conexión a PostgreSQL
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

print(f"SQL://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")
print(f"PG:/{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

sql_engine_str = f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
data_sync = create_engine(sql_engine_str)

def open_pg2_conn():
    return pg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

@task
def leer_configuracion(path):
    df = pd.read_excel(path).fillna("")
    return df.to_dict(orient="records")

@task
def replicar_en_sqlserver(linkedserver, base, tabla, filtro, esquema_destino):
    nombre_destino = f"{esquema_destino}.{tabla}"
    query = f"SELECT * INTO {nombre_destino} FROM [{linkedserver}].[{base}].[dbo].[{tabla}]"
    if filtro:
        query += f" WHERE {filtro}"
    print(f"🛠 SQL EXEC: {query}")
    with data_sync.connect() as conn:
        conn.execute(text(f"IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}"))
        conn.execute(text(query))
    print(f"✅ Réplica local en {nombre_destino}")

@task
def cargar_en_postgres_pg2(esquema_origen, esquema_destino, tabla):
    df = pd.read_sql(f"SELECT * FROM {esquema_origen}.{tabla}", data_sync)
    print(f"📦 {len(df)} filas leídas de {esquema_origen}.{tabla}")

    conn = open_pg2_conn()
    cur = conn.cursor()
    columnas = ','.join(df.columns)
    values_template = ','.join(['%s'] * len(df.columns))

    cur.execute(f"DROP TABLE IF EXISTS {esquema_destino}.{tabla.lower()} CASCADE")
    create_table_query = f"CREATE TABLE {esquema_destino}.{tabla.lower()} AS SELECT * FROM {esquema_origen}.{tabla} WHERE 1=0"
    cur.execute(create_table_query)
    conn.commit()

    for _, row in df.iterrows():
        cur.execute(
            f"INSERT INTO {esquema_destino}.{tabla.lower()} ({columnas}) VALUES ({values_template})",
            tuple(row)
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"📥 Cargado en PostgreSQL → {esquema_destino}.{tabla.lower()}")

@flow(name="replicacion_parametrizada_pg2")
def replicacion_parametrizada_pg2():
    config_path = "config/tablas_para_replicar.xlsx"
    registros = leer_configuracion(config_path)
    for row in registros:
        replicar_en_sqlserver(
            row["LinkedServer"],
            row["Base"],
            row["Tabla"],
            row["Filtro"],
            row["EsquemaSql"]
        )
        cargar_en_postgres_pg2(row["EsquemaSql"], row["EsquemaPg"], row["Tabla"])

if __name__ == "__main__":
    replicacion_parametrizada_pg2()
