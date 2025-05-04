
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
sql_engine_str = f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
data_sync = create_engine(sql_engine_str)

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

@task(name="cargar_ventas_proveedor_pg")
def cargar_ventas_proveedores(lista_ids):
    ids = ','.join(map(str, lista_ids))
    print(f"-> Generando datos cd ventas para ID: {ids}")

    # ----------------------------------------------------------------
    # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
    # ----------------------------------------------------------------
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
            ,LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(A.N_ARTICULO, CHAR(9), ''), CHAR(13), ''), CHAR(10), ''))) as Nombre_Articulo
            ,A.[C_CLASIFICACION_COMPRA] as Clasificacion
            ,GETDATE() as Fecha_Procesado
            ,0 as Marca_Procesado
        
        FROM [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T702_EST_VTAS_POR_ARTICULO] V
        LEFT JOIN [DCO-DBCORE-P02].[DiarcoEst].[dbo].[T050_ARTICULOS] A 
            ON V.C_ARTICULO = A.C_ARTICULO
        WHERE A.[C_PROVEEDOR_PRIMARIO] IN ({ids}) AND V.F_VENTA >= '20230101' AND A.M_BAJA ='N'
        ORDER BY V.F_VENTA ;
        """
        
    # logger.info(f"---->  QUERY: {query}")
    
    df = pd.read_sql(query, data_sync)
    logger.info(f"{len(df)} filas leídas del Proveedor {ids}")
    conn = open_pg_conn()
    cur = conn.cursor()
    table_name = f"src.Base_Forecast_Ventas"
    columns = ', '.join(df.columns)
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    create_sql = f"CREATE TABLE {table_name} ({infer_postgres_types(df)})"
    cur.execute(create_sql)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
    execute_values(cur, insert_sql, values, page_size=5000)
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Datos cargados en PostgreSQL → {table_name}")
    return df


@flow(name="capturar_ventas_proveedores")
def capturar_ventas_proveedores(lista_ids: list = [1074, 190, 20, 174]):
    log = get_run_logger()
    try:
        filas_ventas = cargar_ventas_proveedores.with_options(name="Carga Ventas").submit(lista_ids).result()
        log.info(f"Ventas: {filas_ventas} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando ventas: {e}")


if __name__ == "__main__":
    capturar_ventas_proveedores()
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
