
# Rutina para actualizar el STOCK HISTORICO de los artículso a partir de la ESTADISTICA DE STOCK

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

# Crear engine SQL Server TESTING
def open_sql_testing_conn():    
    return create_engine(f"mssql+pyodbc://{SQLT_USER}:{SQLT_PASSWORD}@{SQLT_SERVER}/{SQLT_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server")    

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

    
@task(name="replicar_stock_DMZ")
def replicar_estadistica_stock_dmz():
    # ----------------------------------------------------------------
    # CARGA TABLA VACIA POR PRIMERA VEZ  (OJO = 16.787.577)
    # ACTUALIZAR TABLA FILTRANDO MES ANTERIOR y ACTUAL
    # ----------------------------------------------------------------

    # Actualizar la tabla [repl].[T710_ESTADIS_STOCK] Mientras no haya CDC
    query2 = f"""
    USE [data-sync]
    GO
    
    DELETE FROM [repl].[T710_ESTADIS_STOCK]
        WHERE C_ANIO = 2025
        AND C_MES >= MONTH(DATEADD(MONTH, -1, GETDATE()))
    GO

    INSERT INTO [repl].[T710_ESTADIS_STOCK]
        ([C_ANIO],[C_MES],[C_SUCU_EMPR],[C_ARTICULO],[Q_DIA1],[Q_DIA2],[Q_DIA3],[Q_DIA4],[Q_DIA5],[Q_DIA6],[Q_DIA7],[Q_DIA8],[Q_DIA9]
        ,[Q_DIA10],[Q_DIA11],[Q_DIA12],[Q_DIA13],[Q_DIA14],[Q_DIA15],[Q_DIA16],[Q_DIA17],[Q_DIA18],[Q_DIA19],[Q_DIA20]
        ,[Q_DIA21],[Q_DIA22],[Q_DIA23],[Q_DIA24],[Q_DIA25],[Q_DIA26],[Q_DIA27],[Q_DIA28],[Q_DIA29],[Q_DIA30],[Q_DIA31]
        ,[Fecha_Proceso],[procesado_ok])

        SELECT [C_ANIO],[C_MES],[C_SUCU_EMPR],[C_ARTICULO],[Q_DIA1],[Q_DIA2],[Q_DIA3],[Q_DIA4],[Q_DIA5],[Q_DIA6],[Q_DIA7],[Q_DIA8],[Q_DIA9]
        ,[Q_DIA10],[Q_DIA11],[Q_DIA12],[Q_DIA13],[Q_DIA14],[Q_DIA15],[Q_DIA16],[Q_DIA17],[Q_DIA18],[Q_DIA19],[Q_DIA20]
        ,[Q_DIA21],[Q_DIA22],[Q_DIA23],[Q_DIA24],[Q_DIA25],[Q_DIA26],[Q_DIA27],[Q_DIA28],[Q_DIA29],[Q_DIA30],[Q_DIA31]
        ,GETDATE() AS Fecha_Proceso
        ,CAST(0 AS BIT) AS Procesado

        FROM [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_STOCK]
        WHERE C_ANIO = 2025
        AND C_MES >= MONTH(DATEADD(MONTH, -1, GETDATE()))
    GO
    """
    data_sync = open_sql_conn()
    with data_sync.connect() as connection:
        connection.execute(query2)
        connection.commit()  # Confirmar la transacción
        
    return("Carga de datos de estadística de stock DMZ finalizada.")
        
@task(name="cargar_datos_historicos_stock_DMZ")
def cargar_historico_stock_articulos_dmz():  
    # Obtener la fecha de procesos más reciente
    query1 = f"""
    SELECT DATEADD(DAY, -1, MAX([Fecha_Procesos])) FROM [repl].[Historico_Stock_Sucursal]
    """
    data_sync = open_sql_conn()
    with data_sync.connect() as connection:
        result = connection.execute(query1)
        fecha_procesos = result.scalar()  # Obtiene el primer valor de la primera fila
        
    logger.info(f"Fecha de procesos más reciente: {fecha_procesos}")
    print(f"Fecha de procesos más reciente: {fecha_procesos}")
    
    # Obtener Registros de la tabla [data-sync].[repl].[T710_ESTADIS_STOCK]
    query3 = f"""
    INSERT INTO [repl].[Historico_Stock_Sucursal]
        ([Anio],[Mes],[Dia],[Sucursal],[Articulo],[Cantidad],[Fecha_Stock],[Fecha_Procesos],[Procesado])

      SELECT *
        FROM (
            SELECT 
                C_ANIO AS Anio,
                C_MES AS Mes,
                CAST(REPLACE(Dia, 'Q_DIA', '') AS INT) AS Dia, -- Extraemos el número de día del nombre de la columna
                C_SUCU_EMPR AS Sucursal,
                C_ARTICULO AS Articulo,
                Cantidad,
                TRY_CAST(CONCAT(C_ANIO, '-', RIGHT('0' + CAST(C_MES AS VARCHAR), 2), '-', RIGHT('0' + CAST(REPLACE(Dia, 'Q_DIA', '') AS VARCHAR), 2)) AS DATE) AS Fecha_Stock,
                GETDATE() AS Fecha_Procesos,
                CAST(0 AS BIT) AS Procesado
            FROM (
                SELECT C_ANIO, C_MES, C_SUCU_EMPR, C_ARTICULO, 
                    Q_DIA1, Q_DIA2, Q_DIA3, Q_DIA4, Q_DIA5, Q_DIA6, Q_DIA7, Q_DIA8, Q_DIA9, 
                    Q_DIA10, Q_DIA11, Q_DIA12, Q_DIA13, Q_DIA14, Q_DIA15, Q_DIA16, Q_DIA17, Q_DIA18, Q_DIA19, 
                    Q_DIA20, Q_DIA21, Q_DIA22, Q_DIA23, Q_DIA24, Q_DIA25, Q_DIA26, Q_DIA27, Q_DIA28, Q_DIA29, 
                    Q_DIA30, Q_DIA31
                FROM [data-sync].[repl].[T710_ESTADIS_STOCK]
                WHERE C_ANIO = 2025 AND C_MES >= MONTH(DATEADD(MONTH, -1, GETDATE()))
            ) AS DataOrigen
            UNPIVOT (
                Cantidad FOR Dia IN (
                    Q_DIA1, Q_DIA2, Q_DIA3, Q_DIA4, Q_DIA5, Q_DIA6, Q_DIA7, Q_DIA8, Q_DIA9, 
                    Q_DIA10, Q_DIA11, Q_DIA12, Q_DIA13, Q_DIA14, Q_DIA15, Q_DIA16, Q_DIA17, Q_DIA18, Q_DIA19, 
                    Q_DIA20, Q_DIA21, Q_DIA22, Q_DIA23, Q_DIA24, Q_DIA25, Q_DIA26, Q_DIA27, Q_DIA28, Q_DIA29, 
                    Q_DIA30, Q_DIA31
                )
            ) AS DataTransformada
        ) AS DatosFiltrados
        WHERE Fecha_Procesos > {fecha_procesos}];
        """
    with data_sync.connect() as connection:
        connection.execute(query3)
        connection.commit()  # Confirmar la transacción
        logger.info("Carga de datos de historico de stock finalizada.")
        print("Carga de datos de historico de stock finalizada.")
    
    return fecha_procesos

# FUNCIONA PERFECTAMENTE
# Cargar datos en PostgreSQL
@task(name="cargar_datos_stock_PG")
def cargar_stock_articulos_PG():
    query4 = "SELECT * FROM [repl].[Historico_Stock_Sucursal]"
    table_name = "src.historico_stock_sucursal"
    chunk_size = 50000
    total_rows = 0

    data_sync = open_sql_conn()
    insert_sql = None

    with data_sync.connect() as connection, open_pg_conn() as conn:
        cur = conn.cursor()
        for i, chunk in enumerate(pd.read_sql(query4, connection, chunksize=chunk_size)):
            if insert_sql is None:
                columns = ', '.join(chunk.columns)
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            values_chunk = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values_chunk)
            conn.commit()
            total_rows += len(values_chunk)
            logger.info(f"Bloque {i+1}: Insertados {len(values_chunk)} registros")

        cur.close()
        conn.close()

    logger.info(f"Datos cargados en PostgreSQL → {table_name}, total {total_rows} registros")
    return total_rows

# Cargar datos en PostgreSQL
@task(name="replicar_matriz_stock_PG")
def replicar_matriz_stock_PG():
    query4 = "SELECT * FROM [repl].[T710_ESTADIS_STOCK]"
    table_name = "src.t710_estadis_stock"
    chunk_size = 50000
    total_rows = 0

    data_sync = open_sql_conn()
    insert_sql = None

    with data_sync.connect() as connection, open_pg_conn() as conn:
        cur = conn.cursor()
        for i, chunk in enumerate(pd.read_sql(query4, connection, chunksize=chunk_size)):
            if insert_sql is None:
                columns = ', '.join(chunk.columns)
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            values_chunk = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            execute_values(cur, insert_sql, values_chunk)
            conn.commit()
            total_rows += len(values_chunk)
            logger.info(f"Bloque {i+1}: Insertados {len(values_chunk)} registros")

        cur.close()
        conn.close()

    logger.info(f"Datos cargados en PostgreSQL → {table_name}, total {total_rows} registros")
    return total_rows

# Flujo principal        
@flow()
def capturar_stock_articulos(name="capturar_stock_articulos"):
    import time
    log = get_run_logger()
    log.info("Iniciando flujo de replicación de stock de artículos...")
    # try:
    #     resultado1 = replicar_estadistica_stock_dmz()
    #     time.sleep(10)
    #     resultado2 = cargar_historico_stock_articulos_dmz()
    #     time.sleep(10)
    #     resultado3 = cargar_stock_articulos_PG()
    #     log.info(f"Finalizado con {len(resultado3)} registros cargados en PostgreSQL.")
    # except Exception as e:
    #     log.error(f"Error durante el flujo: {e}")
    # log = get_run_logger()
    # log.info("Iniciando flujo de replicación de stock de artículos...")
    try:
        filas_art = replicar_matriz_stock_PG()
        log.info(f"Matriz Stock: {filas_art} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando matriz stock en PG: {e}")


if __name__ == "__main__":
    # Ejecutar el flujo directamente
    capturar_stock_articulos()
    print("--------------->  Flujo de replicación FINALIZADO.")
    

