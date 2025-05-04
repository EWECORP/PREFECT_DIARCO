
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

@task
def leer_configuracion(path):
    df = pd.read_excel(path).fillna("")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df.to_dict(orient="records")

@task(name="replicar_en_sqlserver")
def replicar_en_sqlserver(linked_server, base, schema, tabla, filtro, esquema_destino):
    nombre_destino = f"{esquema_destino}.{tabla}"
    nombre_origen = f"[{linked_server}].[{base}].[{schema}].[{tabla}]" if linked_server else f"[{base}].[{schema}].[{tabla}]"
    query = f"SELECT * INTO {nombre_destino} FROM {nombre_origen}"
    if filtro:
        query += f" WHERE {filtro}"

    logger.info(f"QUERY: {query}")
    raw_conn = data_sync.raw_connection()
    cursor = raw_conn.cursor()
    cursor.execute(f"IF OBJECT_ID('{nombre_destino}', 'U') IS NOT NULL DROP TABLE {nombre_destino}")
    cursor.execute(query)
    raw_conn.commit()
    cursor.close()
    raw_conn.close()
    logger.info(f"Tabla replicada localmente: {nombre_destino}")

@task(name="cargar_en_postgres_psycopg2")
def cargar_en_postgres_psycopg2(esquema_origen, esquema_destino, tabla):
    df = pd.read_sql(f"SELECT * FROM {esquema_origen}.{tabla}", data_sync)
    logger.info(f"{len(df)} filas leídas desde {esquema_origen}.{tabla}")

    conn = open_pg_conn()
    cur = conn.cursor()
    table_name = f"{esquema_destino}.{tabla.lower()}"
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

@task(name="actualizar_parametros")
def actualizar_resultados_en_excel(path_excel, resultados):
    try:
        df = pd.read_excel(path_excel)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        for col in ["filas", "estado_sql", "estado_pg"]:
            if col not in df.columns:
                df[col] = ""
        for resultado in resultados:
            mask = (
                (df["linkedserver"] == resultado["linked_server"]) &
                (df["base"] == resultado["base"]) &
                (df["schema"] == resultado["schema"]) &
                (df["tabla"] == resultado["tabla"]) &
                (df["esquemasql"] == resultado["esquema_sql"]) &
                (df["esquemapg"] == resultado["esquema_pg"])
            )
            df.loc[mask, "filas"] = resultado["filas"]
            df.loc[mask, "estado_sql"] = resultado["estado_sql"]
            df.loc[mask, "estado_pg"] = resultado["estado_pg"]
        df.to_excel(path_excel, index=False)
        logger.info("Archivo Excel actualizado correctamente.")
    except Exception as e:
        logger.error(f"Error actualizando el Excel de configuración: {e}")

@flow(name="replicacion_parametrizada_excel")
def replicacion_parametrizada_excel():
    log = get_run_logger()
    config_path = "./config/tablas_para_replicar.xlsx"
    registros = leer_configuracion(config_path)
    resumen = []

    for row in registros:
        if str(row.get("estado_pg", "")).strip().upper() == "OK":
            log.info(f"Saltando tabla ya procesada: {row['tabla']}")
            continue

        estado_sql = "PENDIENTE"
        estado_pg = "PENDIENTE"
        filas = 0

        try:
            replicar_en_sqlserver.with_options(name=f"SQL → {row['tabla']}").submit(
                row["linkedserver"], row["base"], row["schema"], row["tabla"],
                row["filtro"], row["esquemasql"]
            ).result()
            estado_sql = "OK"
        except Exception as e:
            estado_sql = f"ERROR: {str(e)[:100]}"
            log.error(f"Error en carga desde origen para {row['tabla']}: {e}")

        if estado_sql == "OK":
            try:
                df_pg = cargar_en_postgres_psycopg2.with_options(name=f"PG ← {row['tabla']}").submit(
                    row["esquemasql"], row["esquemapg"], row["tabla"]
                ).result()
                filas = len(df_pg)
                estado_pg = "OK"
            except Exception as e:
                estado_pg = f"ERROR: {str(e)[:100]}"
                log.error(f"Error en carga a Postgres para {row['tabla']}: {e}")

        resumen_row = {
            "linked_server": row["linkedserver"],
            "base": row["base"],
            "schema": row["schema"],
            "tabla": row["tabla"],
            "esquema_sql": row["esquemasql"],
            "esquema_pg": row["esquemapg"],
            "filas": filas,
            "estado_sql": estado_sql,
            "estado_pg": estado_pg
        }

        resumen.append(resumen_row)
        actualizar_resultados_en_excel.submit(config_path, [resumen_row])

    for r in resumen:
        log.info(
            f"Tabla: {r['tabla']} | Filas: {r['filas']} | "
            f"Esquema Origen: {r['esquema_sql']} | Esquema Destino: {r['esquema_pg']} | "
            f"Estado SQL: {r['estado_sql']} | Estado PG: {r['estado_pg']}"
        )

if __name__ == "__main__":
    replicacion_parametrizada_excel()
    logger.info("--------------->  Flujo de replicación FINALIZADO.")
