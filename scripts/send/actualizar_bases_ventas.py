# actualizar_bases_ventas.py

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from prefect import flow, task, get_run_logger
from datetime import datetime

from flujo_maestro_replica_datos import flujo_maestro, generar_nombre_archivo

# ====================== CONFIGURACIÓN Y LOGGING ======================
load_dotenv()

# Variables de entorno
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

SP_NAME = "[dbo].[SP_BASE_PRODUCTOS_SUCURSAL]"
TABLE_DESTINO = "src.base_productos_vigentes"

# Logging
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/actualizar_base_ventas.log", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ====================== CONEXIONES ======================
sql_engine = create_engine(
    f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)

def open_pg_conn():
    return pg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

# ====================== FUNCIONES ======================
@task
def obtener_fecha_max_f_venta(tabla_pg: str) -> str:
    """Obtiene la fecha máxima de f_venta desde una tabla en el esquema src."""
    conn = open_pg_conn()  # <- CORREGIDO: se ejecuta la función
    cursor = conn.cursor()
    query = f"SELECT MAX(f_venta) FROM src.{tabla_pg}"
    cursor.execute(query)
    resultado = cursor.fetchone()
    cursor.close()
    conn.close()

    if resultado and resultado[0]:
        fecha_max = resultado[0]
        print(f"🗓 Fecha máxima en {tabla_pg}: {fecha_max}")
        return fecha_max.strftime('%Y-%m-%d')
    else:
        raise ValueError(f"No se pudo determinar la fecha máxima de ventas para {tabla_pg}.")

@flow(name="actualizar_bases_ventas")
def actualizar_bases_ventas():
    tablas = [
        ("t702_est_vtas_por_articulo", "T702_EST_VTAS_POR_ARTICULO"),
        ("t702_est_vtas_por_articulo_dbarrio", "T702_EST_VTAS_POR_ARTICULO_DBARRIO")
    ]

    for tabla_pg, tabla_sql in tablas:
        print(f"🔄 Procesando tabla {tabla_pg}...")
        fecha_max = obtener_fecha_max_f_venta(tabla_pg)
        filtro_sql = f"F_VENTA > '{fecha_max}'"
        print(f"🧪 Filtro aplicado: {filtro_sql}")

        print(f"🚀 Iniciando replicación para {tabla_sql} con filtro: {filtro_sql}")
        flujo_maestro(
            esquema="repl",
            tabla=tabla_sql,
            filtro_sql=filtro_sql
        )

    print("✅ Actualización de ambas bases finalizada.")

if __name__ == "__main__":
    actualizar_bases_ventas()
