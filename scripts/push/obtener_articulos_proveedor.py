
# obtener_articulos_proveedor.py

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

# ====================== LOGGING ======================
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

# ====================== VARIABLES ======================
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

# ====================== CONEXIONES ======================
def open_sql_conn():
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
    return ", ".join([f"{col} {type_map.get(str(dtype), 'TEXT')}" for col, dtype in df.dtypes.items()])

# ====================== TASK PRINCIPAL ======================
@task(name="cargar_articulos_proveedor_pg")
def cargar_articulos_proveedores(lista_ids):
    if not lista_ids:
        logger.warning("⚠️ No se recibieron IDs de proveedor.")
        return pd.DataFrame()

    ids = ','.join(map(str, lista_ids))
    logger.info(f"-> Extrayendo artículos para proveedores: {ids}")

    query = f"""
        SELECT A.C_PROVEEDOR_PRIMARIO, A.C_COMPRADOR, S.C_ARTICULO, S.C_SUCU_EMPR, S.I_PRECIO_VTA,
            S.I_COSTO_ESTADISTICO, S.Q_FACTOR_VTA_SUCU, S.Q_BULTOS_PENDIENTE_OC, S.Q_PESO_PENDIENTE_OC,
            S.Q_UNID_PESO_PEND_RECEP_TRANSF, ST.Q_UNID_ARTICULO AS Q_STOCK_UNIDADES, ST.Q_PESO_ARTICULO AS Q_STOCK_PESO,
            A.M_VENDE_POR_PESO,
                CASE WHEN A.M_VENDE_POR_PESO = 'S' 
					THEN ST.Q_PESO_ARTICULO
					ELSE ST.Q_UNID_ARTICULO
				END AS Q_STOCK,
            S.M_OFERTA_SUCU, S.M_HABILITADO_SUCU, S.M_FOLDER, A.M_BAJA, S.F_ULTIMA_VTA, S.Q_VTA_ULTIMOS_15DIAS,
            S.Q_VTA_ULTIMOS_30DIAS, S.Q_TRANSF_PEND, S.Q_TRANSF_EN_PREP, A.C_FAMILIA, A.C_RUBRO, A.C_CLASIFICACION_COMPRA,
            (R.Q_VENTA_30_DIAS + R.Q_VENTA_15_DIAS) AS Q_VENTA_ACUM_30, R.Q_DIAS_CON_STOCK, R.Q_REPONER,
            R.Q_REPONER_INCLUIDO_SOBRE_STOCK, R.Q_VENTA_DIARIA_NORMAL, R.Q_DIAS_STOCK, R.Q_DIAS_SOBRE_STOCK,
            R.Q_DIAS_ENTREGA_PROVEEDOR, AP.Q_FACTOR_PROVEEDOR, AP.U_PISO_PALETIZADO, AP.U_ALTURA_PALETIZADO,
            CCP.I_LISTA_CALCULADO, GETDATE() as FECHA_PROCESADO, 0 as MARCA_PROCESADO
        FROM [repl].[T051_ARTICULOS_SUCURSAL] S
        INNER JOIN [repl].[T050_ARTICULOS] A ON A.C_ARTICULO = S.C_ARTICULO
        LEFT JOIN [repl].[T060_STOCK] ST ON ST.C_ARTICULO = S.C_ARTICULO AND ST.C_SUCU_EMPR = S.C_SUCU_EMPR
        LEFT JOIN [repl].[T052_ARTICULOS_PROVEEDOR] AP ON A.C_PROVEEDOR_PRIMARIO = AP.C_PROVEEDOR AND S.C_ARTICULO = AP.C_ARTICULO
        LEFT JOIN [repl].[T055_ARTICULOS_CONDCOMPRA_COSTOS] CCP ON A.C_PROVEEDOR_PRIMARIO = CCP.C_PROVEEDOR AND S.C_ARTICULO = CCP.C_ARTICULO AND S.C_SUCU_EMPR = CCP.C_SUCU_EMPR
        LEFT JOIN [repl].[T710_ESTADIS_REPOSICION] R ON R.C_ARTICULO = S.C_ARTICULO AND R.C_SUCU_EMPR = S.C_SUCU_EMPR
        WHERE S.M_HABILITADO_SUCU = 'S' AND A.M_BAJA = 'N' AND A.C_PROVEEDOR_PRIMARIO IN ({ids})
        ORDER BY S.C_ARTICULO, S.C_SUCU_EMPR
    """

    try:
        df = pd.read_sql(query, open_sql_conn())
        if df.empty:
            logger.warning("❗ No se recuperaron registros")
            return df
        df["FUENTE_ORIGEN"] = "DIARCOP001"
        df["FECHA_EXTRACCION"] = datetime.now()
        df["ESTADO_SINCRONIZACION"] = 0
        logger.info(f"✅ {len(df)} filas leídas")
    except Exception as e:
        logger.error(f"❌ Error durante la lectura de SQL Server: {e}")
        raise

    try:
        conn = open_pg_conn()
        cur = conn.cursor()
        table_name = "src.Base_Forecast_Articulos"
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        create_sql = f"CREATE TABLE {table_name} ({infer_postgres_types(df)})"
        cur.execute(create_sql)
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES %s"
        execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()
        logger.info(f"📦 Datos cargados en PostgreSQL → {table_name}")
    except Exception as e:
        logger.error(f"❌ Error al insertar en PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return df

@flow(name="capturar_articulos_proveedores")
def capturar_articulos_proveedores(lista_ids):
    log = get_run_logger()
    try:
        filas = cargar_articulos_proveedores.with_options(name="Carga Artículos").submit(lista_ids).result()
        log.info(f"✅ Artículos insertados: {len(filas)} registros")
    except Exception as e:
        log.error(f"❌ Error: {e}")

if __name__ == "__main__":
    ids = list(map(int, sys.argv[1:])) if len(sys.argv) > 1 else []
    capturar_articulos_proveedores(ids)
    logger.info("🏁 Proceso Finalizado.")
