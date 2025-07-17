# actualizar_tablas_maestras.py
# OBJETIVO: Actualizar las tablas maestras de productos y sucursales en PostgreSQL desde SQL Server.
# AUTOR: [EWE]
# Las tablas ya existen. TRUNCA las tablas destino y las vuelve a llenar.

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
file_handler = logging.FileHandler("logs/actualizar_tablas_maestras.log", encoding="utf-8")
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
def vaciar_tabla(tabla_pg: str) -> None:
    """Vacía la tabla especificada en el esquema src de PostgreSQL."""
    logger = get_run_logger()
    try:
        conn = open_pg_conn()
        cursor = conn.cursor()
        query = f"TRUNCATE TABLE src.{tabla_pg} RESTART IDENTITY CASCADE"
        cursor.execute(query)
        conn.commit()
        logger.info(f"✅ Tabla 'src.{tabla_pg}' vaciada correctamente.")
    except Exception as e:
        logger.info(f"❌ Error al vaciar la tabla 'src.{tabla_pg}': {e}")
        raise
    finally:
        cursor.close()
        conn.close()

@flow(name="actualizar_tablas_maestras")
def actualizar_tablas_maestras():
    logger = get_run_logger()
    tablas = [
        ("t050_articulos", "T050_ARTICULOS"),
        ("t051_articulos_sucursal", "T051_ARTICULOS_SUCURSAL"),
        ("t020_proveedor", "T020_PROVEEDOR"),
        ("t052_articulos_proveedor", "T052_ARTICULOS_PROVEEDOR"),
        ("t060_stock", "T060_STOCK"),
        ("t100_empresa_suc", "T100_EMPRESA_SUC")
    ]

    for tabla_pg, tabla_sql in tablas:

        try:
            logger.info(f"🔄 Procesando tabla {tabla_pg}...")
            vaciar_tabla.submit(tabla_pg)
            flujo_maestro(esquema="repl", tabla=tabla_sql, filtro_sql="1=1")
            logger.info(f"✅ Tabla {tabla_pg} actualizada con éxito.")
        except Exception as e:
            logger.error(f"❌ Error procesando {tabla_pg}: {e}")

    print("✅ Actualización de Tablas Maestras finalizada.")

if __name__ == "__main__":
    actualizar_tablas_maestras()
