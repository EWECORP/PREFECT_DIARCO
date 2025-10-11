# actualizar_tablas_tabulares.py
# OBJETIVO: Actualizar las tablas tabulares de stock y ofertas en PostgreSQL desde SQL Server.
# OBSERVACIÓN: Las tablas tienen estructura por AÑO/MES y múltiples columnas por día.
#              El reemplazo de datos incompletos se hace por DELETE e INSERT.
# AUTOR: [EWE]

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta

from flujo_maestro_replica_datos import flujo_maestro, generar_nombre_archivo

# ====================== CONFIGURACIÓN Y LOGGING ======================
load_dotenv()

# Variables de entorno
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_USER     = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
PG_HOST      = os.getenv("PG_HOST")
PG_PORT      = os.getenv("PG_PORT")
PG_DB        = os.getenv("PG_DB")
PG_USER      = os.getenv("PG_USER")
PG_PASSWORD  = os.getenv("PG_PASSWORD")

# Logging manual (solo en ejecución local, Prefect usa su propio logger en ejecución programada)
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/actualizar_tablas_tabulares.log", encoding="utf-8")
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

# ====================== TAREA: ELIMINAR REGISTROS ======================
@task
def vaciar_registros_tabla(tabla_pg: str, valor_filtro: int) -> None:
    """
    Elimina registros de la tabla en PostgreSQL cuyo (C_ANIO * 100 + C_MES) sea mayor o igual al filtro.
    Protege contra SQL Injection y garantiza cierre de recursos.
    """
    logger = get_run_logger()
    whitelist = {"t710_estadis_stock", "t710_estadis_precios", "t710_estadis_oferta_folder"}
    if tabla_pg not in whitelist:
        raise ValueError(f"Tabla no permitida: {tabla_pg}")

    try:
        conn = open_pg_conn()
        cursor = conn.cursor()
        #query = f"DELETE FROM src.{tabla_pg} WHERE (C_ANIO * 100 + C_MES) >= %s"  # Evitar f-string para SQL Injection
        query = "DELETE FROM src.{tabla} WHERE (C_ANIO * 100 + C_MES) >= %s".format(tabla=tabla_pg)
        logger.info(f"🗑️ Borrando datos de 'src.{tabla_pg}' desde periodo: {valor_filtro}")
        cursor.execute(query, (valor_filtro,))
        conn.commit()
        logger.info(f"✅ Registros eliminados correctamente en 'src.{tabla_pg}'.")
    except Exception as e:
        logger.error(f"❌ Error al eliminar datos de 'src.{tabla_pg}': {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# ====================== FLUJO PRINCIPAL ======================
@flow(name="actualizar_tablas_tabulares")
def actualizar_tablas_tabulares():
    """
    Orquesta la actualización de tablas tabulares:
    - Elimina registros recientes
    - Ejecuta flujo de exportación/importación desde SQL Server a PostgreSQL
    """
    logger = get_run_logger()
    
    tablas = [
        ("repl", "t710_estadis_stock", "T710_ESTADIS_STOCK"),
        ("repl", "t710_estadis_precios", "T710_ESTADIS_PRECIOS"),
        ("repl", "t710_estadis_oferta_folder", "T710_ESTADIS_OFERTA_FOLDER")
    ]

    for origen, tabla_pg, tabla_sql in tablas:
        try:
            logger.info(f"🔄 Procesando tabla: {tabla_pg}")
            t0 = datetime.now()

            # Calcular filtro (ejemplo: 202507 para julio 2025)
            fecha_desde = datetime.today() - timedelta(days=1)
            valor_filtro = fecha_desde.year * 100 + fecha_desde.month
            logger.info(f"🧪 Filtro aplicado: C_ANIO * 100 + C_MES >= {valor_filtro}")

            # Paso 1: Eliminar registros recientes
            vaciar_registros_tabla.submit(tabla_pg, valor_filtro)

            # Paso 2: Ejecutar flujo de carga desde SQL Server
            flujo_maestro(esquema=origen, tabla=tabla_sql, filtro_sql=f"C_ANIO * 100 + C_MES >= {valor_filtro}")

            t1 = datetime.now()
            logger.info(f"✅ Tabla {tabla_pg} actualizada exitosamente en {round((t1 - t0).total_seconds(), 1)}s.")
        except Exception as e:
            logger.error(f"❌ Error procesando {tabla_pg}: {e}")

    print("✅ Finalizó la actualización de tablas tabulares.")

# ====================== EJECUCIÓN LOCAL ======================
if __name__ == "__main__":
    actualizar_tablas_tabulares()

