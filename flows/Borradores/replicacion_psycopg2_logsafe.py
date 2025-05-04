
import os
import sys
import pandas as pd
import psycopg2 as pg2
import logging
from prefect import flow, task
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Asegurar salida en utf-8 compatible si está disponible
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Configurar logger
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Handler archivo
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/replicacion_psycopg2.log", encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Handler consola
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

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

sql_engine_str = f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
data_sync = create_engine(sql_engine_str)

def open_pg_conn():
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
    logger.info(f"Preparando DROP TABLE: IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}")
    logger.info(f"Preparando SELECT INTO: {query}")
    raw_conn = data_sync.raw_connection()
    cursor = raw_conn.cursor()
    cursor.execute(f"IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}")
    cursor.execute(query)
    raw_conn.commit()
    cursor.close()
    raw_conn.close()
    logger.info(f"Tabla replicada localmente: {nombre_destino}")

@task
def cargar_en_postgres_psycopg2(esquema_origen, esquema_destino, tabla):
    df = pd.read_sql(f"SELECT * FROM {esquema_origen}.{tabla}", data_sync)
    logger.info(f"{len(df)} filas leídas desde {esquema_origen}.{tabla}")

    conn = open_pg_conn()
    cur = conn.cursor()

    table_name = f"{esquema_destino}.{tabla.lower()}"
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))

    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    create_sql = f"CREATE TABLE {table_name} AS SELECT * FROM {esquema_origen}.{tabla} WHERE 1=0"
    cur.execute(create_sql)

    for row in df.itertuples(index=False, name=None):
        cur.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", row)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Datos cargados en PostgreSQL → {table_name}")

@flow(name="replicacion_psycopg2_logsafe")
def replicacion_psycopg2_logsafe():
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
        cargar_en_postgres_psycopg2(row["EsquemaSql"], row["EsquemaPg"], row["Tabla"])

if __name__ == "__main__":
    replicacion_psycopg2_logsafe()
