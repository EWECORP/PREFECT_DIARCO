import os
import sys
import time
import csv
import pandas as pd
import psycopg2 as pg2
from io import StringIO
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

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

def open_sql_conn():
    return create_engine(
        f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
    )

def open_pg_conn():
    return pg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

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

def dataframe_to_csv_buffer(df: pd.DataFrame) -> StringIO:
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    return buffer

def copy_from_buffer(conn, df: pd.DataFrame, table_name: str):
    buffer = dataframe_to_csv_buffer(df)
    schema, table = table_name.split(".")
    with conn.cursor() as cur:
        copy_sql = f"""
            COPY "{schema}"."{table}"
            FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\\N')
        """
        cur.copy_expert(copy_sql, buffer)
    conn.commit()

@task(name="cargar_ventas_proveedor_pg")
def cargar_ventas_proveedores(lista_ids, modo='copy', archivo_log='benchmark_resultados.csv'):
    ids = ','.join(map(str, lista_ids))
    logger.info(f"-> Generando datos de ventas para ID: {ids}")

    query = f"""
        SELECT V.[F_VENTA] as Fecha,
               V.[C_ARTICULO] as Codigo_Articulo,
               V.[C_SUCU_EMPR] as Sucursal,
               V.[I_PRECIO_VENTA] as Precio,
               V.[I_PRECIO_COSTO] as Costo,
               V.[Q_UNIDADES_VENDIDAS] as Unidades,
               V.[C_FAMILIA] as Familia,
               A.[C_RUBRO] as Rubro,
               A.[C_SUBRUBRO_1] as SubRubro,
               A.[C_PROVEEDOR_PRIMARIO] as c_proveedor_primario,
               LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(A.N_ARTICULO, CHAR(9), ''), CHAR(13), ''), CHAR(10), ''))) as Nombre_Articulo,
               A.[C_CLASIFICACION_COMPRA] as Clasificacion,
               GETDATE() as Fecha_Procesado,
               0 as Marca_Procesado
        FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO] V
        LEFT JOIN [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T050_ARTICULOS] A 
            ON V.C_ARTICULO = A.C_ARTICULO
        WHERE A.[C_PROVEEDOR_PRIMARIO] IN ({ids}) AND V.F_VENTA >= '20240101' AND A.M_BAJA = 'N'
        ORDER BY V.F_VENTA;
    """

    data_sync = open_sql_conn()
    chunk_size = 50000
    table_name = "src.Base_Forecast_Ventas"
    total_rows = 0
    tiempos_chunk = []

    with data_sync.connect() as connection, open_pg_conn() as conn:
        tiempo_total_inicio = time.time()

        # Recuperar el primer chunk
        chunk_iter = pd.read_sql(query, connection, chunksize=chunk_size)
        try:
            first_chunk = next(chunk_iter)
        except StopIteration:
            logger.warning("No se recuperaron datos del SQL Server.")
            return 0

        # Crear la tabla en PostgreSQL
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS src")
            cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            create_sql = f'CREATE TABLE {table_name} ({infer_postgres_types(first_chunk)})'
            cur.execute(create_sql)
            conn.commit()

        # Procesar primer chunk
        t0 = time.time()
        copy_from_buffer(conn, first_chunk, table_name)
        t1 = time.time()
        tiempos_chunk.append((1, len(first_chunk), round(t1 - t0, 2)))
        logger.info(f"Bloque 1: {len(first_chunk)} registros en {round(t1 - t0, 2)} seg.")
        total_rows += len(first_chunk)

        # Procesar los siguientes chunks
        for i, chunk in enumerate(chunk_iter, start=2):
            t0 = time.time()
            copy_from_buffer(conn, chunk, table_name)
            t1 = time.time()
            tiempos_chunk.append((i, len(chunk), round(t1 - t0, 2)))
            logger.info(f"Bloque {i}: {len(chunk)} registros en {round(t1 - t0, 2)} seg.")
            total_rows += len(chunk)

        duracion_total = round(time.time() - tiempo_total_inicio, 2)
        logger.info(f"Total tiempo: {duracion_total} seg. Total filas: {total_rows}")

    # Guardar resultados del benchmark
    with open(archivo_log, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Chunk", "Registros", "Duración (s)"])
        writer.writerows(tiempos_chunk)
        writer.writerow([])
        writer.writerow(["TOTAL", total_rows, duracion_total])

    return total_rows

@flow(name="capturar_ventas_proveedores")
def capturar_ventas_proveedores(lista_ids: list = [20, 1074]):
    log = get_run_logger()
    try:
        log.info("Ejecutando en modo COPY")
        filas_copy = cargar_ventas_proveedores.with_options(name="Carga COPY").submit(
            lista_ids, modo='copy', archivo_log='benchmark_copy.csv').result()
        log.info(f"COPY finalizado: {filas_copy} filas insertadas")
    except Exception as e:
        log.error(f"Error en benchmarking: {e}")

if __name__ == "__main__":
    capturar_ventas_proveedores()
    logger.info("--------------->  Flujo de replicación FINALIZADO.")

