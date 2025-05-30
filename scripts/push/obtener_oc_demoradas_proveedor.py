
# Rutina para obtener Base_Forecast_Oc_Demoradas por PROVEEDOR (Parámetros)

import os
import sys
import pandas as pd
import psycopg2 as pg2
from psycopg2.extras import execute_values
import logging
from prefect import flow, task, get_run_logger
from prefect.filesystems import LocalFileSystem
from sqlalchemy import create_engine
from dotenv import load_dotenv

storage = LocalFileSystem(basepath="D:/services/ETL_DIARCO/flows") #D:\Services\ETL_DIARCO\flows  "D:/services/ETL_DIARCO/flows

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
# SQL DMZ - Acceso a la base de datos de producción
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")
#Testing
SQLT_DRIVER= os.getenv("SQLT_DRIVER")
SQLT_SERVER= os.getenv("SQLT_SERVER")
SQLT_USER= os.getenv("SQLT_USER")
SQLT_PASSWORD= os.getenv("SQLT_PASSWORD")
SQLT_DATABASE= os.getenv("SQLT_DATABASE")
SQLT_PORT=os.getenv("SQLT_PORT")
# PostgreSQL
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

    
@task(name="cargar_oc_demoradas_proveedores_pg")
def cargar_oc_demoradas_proveedores_pg(lista_ids):
    ids = ','.join(map(str, lista_ids))
    print(f"-> Generando datos para ID: {ids}")
    # ----------------------------------------------------------------
    # FILTRA Ordenes de Compra Demoradas por Proveedor
    # ----------------------------------------------------------------
    query = f"""              
        SELECT  [C_OC]
            ,[U_PREFIJO_OC]
            ,[U_SUFIJO_OC]      
            ,[U_DIAS_LIMITE_ENTREGA]
            , DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]) as FECHA_LIMITE
            , DATEDIFF (DAY, DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]), GETDATE()) as Demora
            ,[C_PROVEEDOR] as Codigo_Proveedor
            ,[C_SUCU_COMPRA] as Codigo_Sucursal
            ,[C_SUCU_DESTINO]
            ,[C_SUCU_DESTINO_ALT]
            ,[C_SITUAC]
            ,[F_SITUAC]
            ,[F_ALTA_SIST]
            ,[F_EMISION]
            ,[F_ENTREGA]    
            ,[C_USUARIO_OPERADOR]    
            
        FROM [repl].[T080_OC_CABE]  
        WHERE [C_SITUAC] = 1
        AND C_PROVEEDOR IN ( {ids} )
        AND DATEADD(DAY, [U_DIAS_LIMITE_ENTREGA], [F_ENTREGA]) < GETDATE();
    """

    # logger.info(f"---->  QUERY: {query}")
    data_sync = open_sql_conn()    
    df = pd.read_sql(query, data_sync)
    logger.info(f"{len(df)} filas leídas de los Proveedores {ids}")
    # Reemplazar en PostgreSQL la Base de Estimación para FORECAST
    conn = open_pg_conn()
    cur = conn.cursor()
    table_name = f"src.Base_Forecast_Oc_Demoradas"
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


@flow(name="capturar_oc_demoradas_proveedores")
def capturar_oc_demoradas_proveedores(lista_ids):
    log = get_run_logger()
    try:
        filas_art = cargar_oc_demoradas_proveedores_pg.with_options(name="Carga Stock").submit(lista_ids).result()
        log.info(f"OC Demoradas: {filas_art} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando registros: {e}")

if __name__ == "__main__":
    ids = list(map(int, sys.argv[1:]))  # ← lee los proveedores como argumentos
    capturar_oc_demoradas_proveedores(ids)
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
