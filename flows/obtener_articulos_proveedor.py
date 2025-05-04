
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

    
@task(name="cargar_articulos_proveedor_pg")
def cargar_articulos_proveedores(lista_ids):
    ids = ','.join(map(str, lista_ids))
    print(f"-> Generando datos para ID: {ids}")

    # ----------------------------------------------------------------
    # FILTRA solo PRODUCTOS HABILITADOS y Traer datos de STOCK y PENDIENTES desde PRODUCCIÓN
    # ----------------------------------------------------------------
    query = f"""
    SELECT A.[C_PROVEEDOR_PRIMARIO]
        ,S.[C_ARTICULO]
        ,S.[C_SUCU_EMPR]
        ,S.[I_PRECIO_VTA]
        ,S.[I_COSTO_ESTADISTICO]
        ,S.[Q_FACTOR_VTA_SUCU]
        ,S.[Q_BULTOS_PENDIENTE_OC]-- OJO esto está en BULTOS DIARCO
        ,S.[Q_PESO_PENDIENTE_OC]
        ,S.[Q_UNID_PESO_PEND_RECEP_TRANSF]
        ,ST.Q_UNID_ARTICULO AS Q_STOCK_UNIDADES-- Stock Cierre Dia Anterior
        ,ST.Q_PESO_ARTICULO AS Q_STOCK_PESO
        ,S.[M_OFERTA_SUCU]
        ,S.[M_HABILITADO_SUCU]
        ,S.[M_FOLDER]
        ,A.M_BAJA  --- Puede no ser necesaria al hacer inner
        ,S.[F_ULTIMA_VTA]
        ,S.[Q_VTA_ULTIMOS_15DIAS]-- OJO esto está en BULTOS DIARCO
        ,S.[Q_VTA_ULTIMOS_30DIAS]-- OJO esto está en BULTOS DIARCO
        ,S.[Q_TRANSF_PEND]-- OJO esto está en BULTOS DIARCO
        ,S.[Q_TRANSF_EN_PREP]-- OJO esto está en BULTOS DIARCO
        --- ,A.[N_ARTICULO]
        ,A.[C_FAMILIA]
        ,A.[C_RUBRO]
        ,A.[C_CLASIFICACION_COMPRA] -- ojo nombre erroneo en la contratabla
        ,(R.[Q_VENTA_30_DIAS] + R.[Q_VENTA_15_DIAS]) AS Q_VENTA_ACUM_30 -- OJO esto está en BULTOS DIARCO
        ,R.[Q_DIAS_CON_STOCK] -- Cantidad de dias para promediar venta diaria
        ,R.[Q_REPONER] -- OJO esto está en BULTOS DIARCO
        ,R.[Q_REPONER_INCLUIDO_SOBRE_STOCK]-- OJO esto está en BULTOS DIARCO (Venta Promedio * Comprar Para + Lead Time - STOCK - PEND, OC)
            --- Ojo la venta promerio excluye  las oferta para no alterar el promedio
        ,R.[Q_VENTA_DIARIA_NORMAL]-- OJO esto está en BULTOS DIARCO
        ,R.[Q_DIAS_STOCK]
        ,R.[Q_DIAS_SOBRE_STOCK]
        ,R.[Q_DIAS_ENTREGA_PROVEEDOR]
        ,AP.[Q_FACTOR_PROVEEDOR]
        ,AP.[U_PISO_PALETIZADO]
        ,AP.[U_ALTURA_PALETIZADO]
        ,CCP.[I_LISTA_CALCULADO]
        ,GETDATE() as Fecha_Procesado
        ,0 as Marca_Procesado
            
    FROM [DIARCOP001].[DiarcoP].[dbo].[T051_ARTICULOS_SUCURSAL] S
    INNER JOIN [DIARCOP001].[DiarcoP].[dbo].[T050_ARTICULOS] A
        ON A.[C_ARTICULO] = S.[C_ARTICULO]
    LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T060_STOCK] ST
        ON ST.C_ARTICULO = S.[C_ARTICULO] 
        AND ST.C_SUCU_EMPR = S.[C_SUCU_EMPR]

    LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T052_ARTICULOS_PROVEEDOR] AP
        ON A.[C_PROVEEDOR_PRIMARIO] = AP.[C_PROVEEDOR]
            AND S.[C_ARTICULO] = AP.[C_ARTICULO]
    LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T055_ARTICULOS_CONDCOMPRA_COSTOS] CCP
        ON A.[C_PROVEEDOR_PRIMARIO] = CCP.[C_PROVEEDOR]
            AND S.[C_ARTICULO] = CCP.[C_ARTICULO]
            AND S.[C_SUCU_EMPR] = CCP.[C_SUCU_EMPR]

    LEFT JOIN [DIARCOP001].[DiarcoP].[dbo].[T710_ESTADIS_REPOSICION] R
        ON R.[C_ARTICULO] = S.[C_ARTICULO]
        AND R.[C_SUCU_EMPR] = S.[C_SUCU_EMPR]

    WHERE S.[M_HABILITADO_SUCU] = 'S' -- Permitido Reponer
        AND A.M_BAJA = 'N'  -- Activo en Maestro Artículos
        AND A.[C_PROVEEDOR_PRIMARIO] IN ( {ids} )-- Solo del Proveedor
    
    ORDER BY S.[C_ARTICULO],S.[C_SUCU_EMPR];
    """
    # logger.info(f"---->  QUERY: {query}")
    
    df = pd.read_sql(query, data_sync)
    logger.info(f"{len(df)} filas leídas de los Proveedores {ids}")
    conn = open_pg_conn()
    cur = conn.cursor()
    table_name = f"src.Base_Forecast_Articulos"
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


@flow(name="capturar_articulos_proveedores")
def capturar_articulos_proveedores(lista_ids: list = [1074, 190, 20, 174]):
    log = get_run_logger()
    try:
        filas_art = cargar_articulos_proveedores.with_options(name="Carga Artículos").submit(lista_ids).result()
        log.info(f"Artículos: {filas_art} filas insertadas")
    except Exception as e:
        log.error(f"Error cargando artículos: {e}")

if __name__ == "__main__":
    capturar_articulos_proveedores()
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
