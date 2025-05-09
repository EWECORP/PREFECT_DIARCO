
# Rutina para obtener Base_Forecast_Stock de los artículos por PROVEEDOR (Parámetros)

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

    
@task(name="cargar_stock_proveedores_pg")
def cargar_stock_proveedores_pg(lista_ids):
    ids = ','.join(map(str, lista_ids))
    print(f"-> Generando datos para ID: {ids}")
    # ----------------------------------------------------------------
    # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
    # ----------------------------------------------------------------
    query = f"""              
        SELECT 
            A.[C_PROVEEDOR_PRIMARIO] AS Codigo_Proveedor,
            S.[C_ARTICULO] AS Codigo_Articulo,
            S.[C_SUCU_EMPR] AS Codigo_Sucursal,
            S.[I_PRECIO_VTA] AS Precio_Venta,
            S.[I_COSTO_ESTADISTICO] AS Precio_Costo,
            S.[Q_FACTOR_VTA_SUCU] AS Factor_Venta,
            ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO AS Stock_Unidades, -- Stock Cierre Día Anterior            
            (R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] AS Venta_Unidades_30_Dias, -- OJO convertida desde BULTOS DIARCO            
            (ST.Q_UNID_ARTICULO + ST.Q_PESO_ARTICULO) * S.[I_COSTO_ESTADISTICO] AS Stock_Valorizado, -- Stock Cierre Día Anterior            
            (R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) * S.[Q_FACTOR_VTA_SUCU] * S.[I_COSTO_ESTADISTICO] AS Venta_Valorizada,
            
            CASE 
                WHEN (ISNULL(R.[Q_VENTA_30_DIAS], 0) + ISNULL(R.[Q_VENTA_15_DIAS], 0)) * ISNULL(S.[Q_FACTOR_VTA_SUCU], 0) * ISNULL(S.[I_COSTO_ESTADISTICO], 0) = 0 THEN NULL
                ELSE 
                    ROUND(
                        ((ISNULL(ST.Q_UNID_ARTICULO,0) + ISNULL(ST.Q_PESO_ARTICULO,0)) * ISNULL(S.[I_COSTO_ESTADISTICO],0)) / 
                        NULLIF(
                            (ISNULL(R.[Q_VENTA_30_DIAS],0) + ISNULL(R.[Q_VENTA_15_DIAS],0)) * ISNULL(S.[Q_FACTOR_VTA_SUCU],0) * ISNULL(S.[I_COSTO_ESTADISTICO],0),
                            0
                        ), 
                        0
                    ) * 30
            END AS Dias_Stock,
            
            S.[F_ULTIMA_VTA],            
            S.[Q_VTA_ULTIMOS_15DIAS] * S.[Q_FACTOR_VTA_SUCU] AS VENTA_UNIDADES_1Q, -- OJO esto está en BULTOS DIARCO
            S.[Q_VTA_ULTIMOS_30DIAS] * S.[Q_FACTOR_VTA_SUCU] AS VENTA_UNIDADES_2Q -- OJO esto está en BULTOS DIARCO

        FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
        INNER JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
            ON A.[C_ARTICULO] = S.[C_ARTICULO]
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T060_STOCK] ST
            ON ST.C_ARTICULO = S.[C_ARTICULO] 
            AND ST.C_SUCU_EMPR = S.[C_SUCU_EMPR]
        LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION] R
            ON R.[C_ARTICULO] = S.[C_ARTICULO]
            AND R.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]

        WHERE 
            S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
            AND A.M_BAJA = 'N'          -- Activo en Maestro Artículos
            AND A.[C_PROVEEDOR_PRIMARIO] IN ( {ids} ) -- Solo del Proveedor

        ORDER BY 
            S.[C_ARTICULO],
            S.[C_SUCU_EMPR];
    """

    # logger.info(f"---->  QUERY: {query}")
    data_sync = open_sql_conn()    
    df = pd.read_sql(query, data_sync)
    logger.info(f"{len(df)} filas leídas de los Proveedores {ids}")
    # Reemplazar en PostgreSQL la Base de Estimación para FORECAST
    conn = open_pg_conn()
    cur = conn.cursor()
    table_name = f"src.Base_Forecast_Stock"
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


@flow(name="capturar_stock_proveedores")
def capturar_stock_proveedores(lista_ids: list = [ 190, 2676, 3835, 6363,  20, 1074]):
    log = get_run_logger()
    try:
        filas_art = cargar_stock_proveedores_pg.with_options(name="Carga Stock").submit(lista_ids).result()
        log.info(f"Stock Artículos: {filas_art} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando artículos: {e}")

if __name__ == "__main__":
    capturar_stock_proveedores()
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
