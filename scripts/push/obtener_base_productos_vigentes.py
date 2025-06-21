# obtener_base_productos_vigentes.py

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from datetime import datetime
from prefect import flow, task, get_run_logger
import ast
from typing import Optional

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
file_handler = logging.FileHandler("logs/replicacion_psycopg2.log", encoding="utf-8")
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

# ====================== ESQUEMA DEFINIDO MANUALMENTE ======================

ESQUEMA_BASE_PRODUCTOS = {
    "c_sucu_empr": "INTEGER",
    "c_articulo": "INTEGER",
    "c_proveedor_primario": "INTEGER",
    "abastecimiento": "INTEGER",
    "cod_cd": "VARCHAR",
    "habilitado": "INTEGER",
    "fecha_registro": "TIMESTAMP",
    "fecha_baja": "TIMESTAMP",
    "unid_transferencia": "INTEGER",
    "q_unid_transferencia": "INTEGER",
    "pedido_min": "DOUBLE PRECISION",
    "frente_lineal": "INTEGER",
    "capacid_gondola": "INTEGER",
    "stock_minimo": "DOUBLE PRECISION",
    "cod_comprador": "INTEGER",
    "q_factor_compra": "INTEGER",
    "promocion": "INTEGER",
    "active_for_purchase": "INTEGER",     
    "active_for_sale": "INTEGER",
    "active_on_mix": "INTEGER",
    "delivered_id": "VARCHAR",
    "product_base_id": "VARCHAR",
    "own_production": "INTEGER",
    "full_capacity_pallet": "INTEGER",
    "number_of_layers": "INTEGER",
    "number_of_boxes_per_layer": "INTEGER",
    "fuente_origen": "VARCHAR",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER"
}

def crear_sentencia_create(schema_dict: dict, table_name: str) -> str:
    columnas_sql = ", ".join([f'"{col}" {tipo}' for col, tipo in schema_dict.items()])
    return f'CREATE TABLE {table_name} ({columnas_sql})'

def insert_dataframe_postgres(df: pd.DataFrame, table_fullname: str):
    df.columns = [col.lower() for col in df.columns]          # Minúsculas
    df = df.where(pd.notnull(df), None)                       # Nulos
    df = df.astype(object)                                    # Tipos Python nativos

    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {table_fullname} CASCADE')
            create_sql = crear_sentencia_create(ESQUEMA_BASE_PRODUCTOS, table_fullname)
            cur.execute(create_sql)

            columnas = ', '.join([f'"{col}"' for col in df.columns])
            insert_sql = f'INSERT INTO {table_fullname} ({columnas}) VALUES %s'
            values = [tuple(row) for row in df.itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()

# ====================== TAREAS PREFECT ======================
@task(name="cargar_base_productos_pg")
def cargar_base_productos():
    logger = get_run_logger()
    query = f"EXEC {SP_NAME}"
    logger.info("🟡 Iniciando lectura de SQL Server...")

    try:
        df = pd.read_sql(query, sql_engine)
        if df.empty:
            logger.warning("⚠️ No se recuperaron registros desde el SP")
            return df
        df["FUENTE_ORIGEN"] = "SP_BASE_PRODUCTOS_SUCURSAL"
        df["FECHA_EXTRACCION"] = datetime.now()
        df["ESTADO_SINCRONIZACION"] = 0
    
        # Adecuación de columnas numéricas
        df["C_ARTICULO"] = pd.to_numeric(df["C_ARTICULO"], errors="coerce").astype("Int64")
        df["C_PROVEEDOR_PRIMARIO"] = pd.to_numeric(df["C_PROVEEDOR_PRIMARIO"], errors="coerce").astype("Int64")
        df["Q_UNID_TRANSFERENCIA"] = pd.to_numeric(df["Q_UNID_TRANSFERENCIA"], errors="coerce").astype("Int64")
        df["PEDIDO_MIN"] = pd.to_numeric(df["PEDIDO_MIN"], errors="coerce").astype("Float64")
        df["FULL_CAPACITY_PALLET"] = pd.to_numeric(df["FULL_CAPACITY_PALLET"], errors="coerce").astype("Int64")
        df["NUMBER_OF_LAYERS"] = pd.to_numeric(df["NUMBER_OF_LAYERS"], errors="coerce").astype("Int64")
        df["NUMBER_OF_BOXES_PER_LAYER"] = pd.to_numeric(df["NUMBER_OF_BOXES_PER_LAYER"], errors="coerce").astype("Float64")
        df["COD_COMPRADOR"] = pd.to_numeric(df["COD_COMPRADOR"], errors="coerce").astype("Int64")
        df["Q_FACTOR_COMPRA"] = pd.to_numeric(df["Q_FACTOR_COMPRA"], errors="coerce").astype("Int64")

        # Adecuación de fechas
        df["FECHA_REGISTRO"] = pd.to_datetime(df["FECHA_REGISTRO"], errors="coerce")
        df["FECHA_BAJA"] = pd.to_datetime(df["FECHA_BAJA"], errors="coerce")

        logger.info(f"✅ {len(df)} registros leídos desde SQL Server.")
    except Exception as e:
        logger.error(f"❌ Error durante la lectura: {e}")
        raise

    try:
        insert_dataframe_postgres(df, TABLE_DESTINO)
        logger.info(f"📦 Datos cargados en PostgreSQL: {TABLE_DESTINO}")
    except Exception as e:
        logger.error(f"❌ Error al insertar en PostgreSQL: {e}")
        raise

    return df

@flow(name="obtener_base_productos_vigentes")
def capturar_base_articulos():
    log = get_run_logger()
    try:
        df_resultado = cargar_base_productos.with_options(name="Carga Base Productos Vigentes").submit().result()
        log.info(f"✅ Proceso completado: {len(df_resultado)} filas cargadas")
    except Exception as e:
        log.error(f"🔥 Error general en el flujo: {e}")

# ====================== EJECUCIÓN MANUAL ======================
if __name__ == "__main__":
    args = sys.argv[1:]
    lista_ids = ast.literal_eval(args[0]) if args else None
    capturar_base_articulos()
    logger.info("🏁 Proceso Finalizado.")
