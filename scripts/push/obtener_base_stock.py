# obtener_base_stock.py

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

SP_NAME = "[dbo].[SP_BASE_STOCK]"
TABLE_DESTINO = "src.base_stock_sucursal"

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

ESQUEMA_BASE_STOCK = {
    "codigo_articulo": "INTEGER",
    "codigo_sucursal": "INTEGER",
    "codigo_proveedor": "INTEGER",
    "precio_venta": "DOUBLE PRECISION",
    "precio_costo": "DOUBLE PRECISION",
    "factor_venta": "INTEGER",
    "m_vende_por_peso": "VARCHAR",
    "venta_unidades_1q": "DOUBLE PRECISION",
    "venta_unidades_2q": "DOUBLE PRECISION",
    "venta_mes_unidades": "DOUBLE PRECISION",
    "venta_mes_valorizada": "DOUBLE PRECISION",
    "dias_stock": "INTEGER",
    "fecha_stock": "TIMESTAMP",
    "stock": "DOUBLE PRECISION",
    "transfer_pendiente": "DOUBLE PRECISION",
    "pedido_pendiente": "DOUBLE PRECISION",
    "promocion": "INTEGER",
    "lote": "VARCHAR",
    "validez_lote": "TIMESTAMP",
    "stock_reserva": "DOUBLE PRECISION",
    "validez_promocion": "INTEGER",
    "q_dias_stock": "INTEGER",
    "q_dias_sobre_stock": "INTEGER",
    "i_lista_calculado": "DOUBLE PRECISION",
    "pedido_sgm": "DOUBLE PRECISION",
    "fuente_origen": "VARCHAR",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER"
}


def crear_sentencia_create(schema_dict: dict, table_name: str) -> str:
    columnas_sql = ", ".join([f'"{col}" {tipo}' for col, tipo in schema_dict.items()])
    return f'CREATE TABLE {table_name} ({columnas_sql})'

def insert_dataframe_postgres(df: pd.DataFrame, table_fullname: str):
    df.columns = [col.lower() for col in df.columns]
    
    # Conversión completa a tipos nativos y manejo de NAs
    df = df.astype(object).where(pd.notnull(df), None)
    df.rename(columns={col: col.lower() for col in df.columns}, inplace=True)


    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {table_fullname} CASCADE')
            create_sql = crear_sentencia_create(ESQUEMA_BASE_STOCK, table_fullname)
            cur.execute(create_sql)

            columnas = ', '.join([f'"{col}"' for col in df.columns])
            insert_sql = f'INSERT INTO {table_fullname} ({columnas}) VALUES %s'
            values = [tuple(row) for row in df.itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()


# ====================== TAREAS PREFECT ======================
@task(name="cargar_base_stock_sucursal_pg")
def cargar_base_stock_sucursal_pg():
    logger = get_run_logger()
    query = f"EXEC {SP_NAME}"
    logger.info("🟡 Iniciando lectura de SQL Server...")

    try:
        df = pd.read_sql(query, sql_engine)
        if df.empty:
            logger.warning("⚠️ No se recuperaron registros desde el SP")
            return df
        df["FUENTE_ORIGEN"] = "SP_BASE_STOCK"
        df["FECHA_EXTRACCION"] = datetime.now()
        df["ESTADO_SINCRONIZACION"] = 0
    
        # Adecuación de columnas numéricas
        df["Codigo_Articulo"] = pd.to_numeric(df["Codigo_Articulo"], errors="coerce").astype("Int64")
        df["Codigo_Sucursal"] = pd.to_numeric(df["Codigo_Sucursal"], errors="coerce").astype("Int64")
        df["Codigo_Proveedor"] = pd.to_numeric(df["Codigo_Proveedor"], errors="coerce").astype("Int64")
        df["Precio_Venta"] = pd.to_numeric(df["Precio_Venta"], errors="coerce").astype("Float64")
        df["Precio_Costo"] = pd.to_numeric(df["Precio_Costo"], errors="coerce").astype("Float64")
        df["Factor_Venta"] = pd.to_numeric(df["Factor_Venta"], errors="coerce").astype("Int64") 
        df["M_Vende_Por_Peso"] = df["M_Vende_Por_Peso"].astype("string")      
        df["Venta_Unidades_1Q"] = pd.to_numeric(df["Venta_Unidades_1Q"], errors="coerce").astype("Float64")
        df["Venta_Unidades_2Q"] = pd.to_numeric(df["Venta_Unidades_2Q"], errors="coerce").astype("Float64")
        df["Venta_Mes_Unidades"] = pd.to_numeric(df["Venta_Mes_Unidades"], errors="coerce").astype("Float64")
        df["Venta_Mes_Valorizada"] = pd.to_numeric(df["Venta_Mes_Valorizada"], errors="coerce").astype("Float64")
        df["Dias_Stock"] = pd.to_numeric(df["Dias_Stock"], errors="coerce").astype("Float64")
        df["FECHA_STOCK"] = pd.to_datetime(df["FECHA_STOCK"], errors="coerce")
        df["STOCK"] = pd.to_numeric(df["STOCK"], errors="coerce").astype("Float64")
        df["TRANSFER_PENDIENTE"] = pd.to_numeric(df["TRANSFER_PENDIENTE"], errors="coerce").astype("Float64")
        df["PEDIDO_PENDIENTE"] = pd.to_numeric(df["PEDIDO_PENDIENTE"], errors="coerce").astype("Float64")
        df["PROMOCION"] = pd.to_numeric(df["PROMOCION"], errors="coerce").astype("Int64") 
        df["LOTE"] = df["LOTE"].astype("string").str.strip()
        df["VALIDEZ_LOTE"] = pd.to_datetime(df["VALIDEZ_LOTE"], errors="coerce")
        df["STOCK_RESERVA"] = pd.to_numeric(df["STOCK_RESERVA"], errors="coerce").astype("Float64")       
        df["VALIDEZ_PROMOCION"] = pd.to_numeric(df["VALIDEZ_PROMOCION"], errors="coerce").astype("Int64")
        df["Q_DIAS_STOCK"] = pd.to_numeric(df["Q_DIAS_STOCK"], errors="coerce").astype("Int64")
        df["Q_DIAS_SOBRE_STOCK"] = pd.to_numeric(df["Q_DIAS_SOBRE_STOCK"], errors="coerce").astype("Int64")
        df["I_LISTA_CALCULADO"] = pd.to_numeric(df["I_LISTA_CALCULADO"], errors="coerce").astype("Float64") 
        df["Pedido_SGM"] = pd.to_numeric(df["Pedido_SGM"], errors="coerce").astype("Float64") 


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

from typing import Optional

@flow(name="obtener_base_stock_sucursal")
def capturar_base_stock(lista_ids: Optional[list] = None):
    log = get_run_logger()
    try:
        df_resultado = cargar_base_stock_sucursal_pg.with_options(name="Carga Base Stock Sucursal").submit().result()
        log.info(f"✅ Proceso completado: {len(df_resultado)} filas cargadas")
    except Exception as e:
        log.error(f"🔥 Error general en el flujo: {e}")

# ====================== EJECUCIÓN MANUAL ======================
if __name__ == "__main__":
    args = sys.argv[1:]
    lista_ids = ast.literal_eval(args[0]) if args else None
    capturar_base_stock()
    logger.info("🏁 Proceso Finalizado.")
