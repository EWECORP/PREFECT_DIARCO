
import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
import logging
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Configurar logging
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

# Cargar variables de entorno
load_dotenv()
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Crear engine SQL Server
def open_sql_conn():
    print(f"Conectando a SQL Server: {SQL_SERVER}")
    print(f"Conectando a SQL Server: {SQL_DATABASE}") 
    return create_engine(f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")

def open_pg_conn():
    return pg2.connect(dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT)

def infer_postgres_types(df):
    type_map = {
        "int64": "BIGINT",
        "int32": "INTEGER",
        "float64": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "object": "TEXT"
    }
    col_defs = [f"{col} {type_map.get(str(df[col].dtype), 'TEXT')}" for col in df.columns]
    return ", ".join(col_defs)


# FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
@task(name="cargar_ventas_proveedor_pg")
def cargar_ventas_proveedores(lista_ids):
    ids = ','.join(map(str, lista_ids))
    print(f"-> Generando datos de ventas para ID: {ids}")

    query = f"""
        SELECT V.[F_VENTA] as Fecha
            ,V.[C_ARTICULO] as Codigo_Articulo
            ,V.[C_SUCU_EMPR] as Sucursal
            ,V.[I_PRECIO_VENTA] as Precio
            ,V.[I_PRECIO_COSTO] as Costo
            ,V.[Q_UNIDADES_VENDIDAS] as Unidades
            ,V.[C_FAMILIA] as Familia
            ,A.[C_RUBRO] as Rubro
            ,A.[C_SUBRUBRO_1] as SubRubro
            ,A.[C_PROVEEDOR_PRIMARIO] as c_proveedor_primario
            ,LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(A.N_ARTICULO, CHAR(9), ''), CHAR(13), ''), CHAR(10), ''))) as Nombre_Articulo
            ,A.[C_CLASIFICACION_COMPRA] as Clasificacion
            ,GETDATE() as Fecha_Procesado
            ,0 as Marca_Procesado
        FROM [repl].[T702_EST_VTAS_POR_ARTICULO] V
        LEFT JOIN [repl].[T050_ARTICULOS] A 
            ON V.C_ARTICULO = A.C_ARTICULO
        WHERE A.[C_PROVEEDOR_PRIMARIO] IN ({ids}) AND V.F_VENTA >= '20240101' AND A.M_BAJA ='N'
        ORDER BY V.F_VENTA;
    """

    data_sync = open_sql_conn()
    chunk_size = 50000
    table_name = "src.Base_Forecast_Ventas"
    insert_sql = None
    total_rows = 0

    with data_sync.connect() as connection, open_pg_conn() as conn:
        cur = conn.cursor()

        for i, chunk in enumerate(pd.read_sql(query, connection, chunksize=chunk_size)):
            if insert_sql is None:
                columns = ', '.join(chunk.columns)
                cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
                create_sql = f"CREATE TABLE {table_name} ({infer_postgres_types(chunk)})"
                cur.execute(create_sql)
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            values_chunk = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values_chunk)
            conn.commit()
            total_rows += len(values_chunk)
            logger.info(f"Proveedor {ids} - Bloque {i+1}: Insertados {len(values_chunk)} registros")

        cur.close()
        conn.close()

    logger.info(f"Datos cargados en PostgreSQL → {table_name}, total {total_rows} registros")
    return total_rows


@flow(name="capturar_ventas_proveedores")
def capturar_ventas_proveedores(lista_ids):
    log = get_run_logger()
    try:
        filas_ventas = cargar_ventas_proveedores.with_options(name="Carga Ventas").submit(lista_ids).result()
        log.info(f"Ventas: {filas_ventas} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando ventas: {e}")


if __name__ == "__main__":
    ids = list(map(int, sys.argv[1:]))  # ← lee los proveedores como argumentos
    capturar_ventas_proveedores(ids)
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
