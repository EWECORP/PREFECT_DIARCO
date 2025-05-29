# utils/postgres.py

import pandas as pd
import paramiko

from prefect import flow, task, get_run_logger
import zipfile
import os
import psycopg2
from utils.postgres import PG_CONN_STR, PG_RAW_CONN

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker


@task
def descomprimir_archivo(nombre_zip):
    with zipfile.ZipFile(nombre_zip, 'r') as zipf:
        zipf.extractall()
    return nombre_zip.replace(".zip", ".csv")

@task
def validar_o_crear_tabla(schema: str, tabla: str, archivo_csv: str):
    logger = get_run_logger()
    engine = create_engine(PG_CONN_STR)
    df_csv = pd.read_csv(archivo_csv, delimiter='|', dtype=str, nrows=100)  # Sample

    with engine.connect() as conn:
        inspector = inspect(engine)
        if inspector.has_table(tabla, schema=schema):
            columnas_pg = [col['name'] for col in inspector.get_columns(tabla, schema=schema)]
            columnas_csv = df_csv.columns.tolist()

            if columnas_pg != columnas_csv:
                logger.warning(f"Estructura incompatible: eliminando tabla {schema}.{tabla}")
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tabla}" CASCADE'))
                df_csv.iloc[0:0].to_sql(tabla, engine, schema=schema, index=False)
            else:
                logger.info("Estructura de tabla válida, no se requiere recreación.")
        else:
            logger.info(f"Creando tabla {schema}.{tabla}")
            df_csv.iloc[0:0].to_sql(tabla, engine, schema=schema, index=False)

@task
def cargar_csv_postgres(csv_path, esquema, tabla):
    logger = get_run_logger()
    total_lineas = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1  # -1 por el header
    
    with psycopg2.connect(**PG_RAW_CONN) as conn: # type: ignore
        with conn.cursor() as cur:
            with open(csv_path, "r", encoding="utf-8") as f:
                next(f)  # Skip header
                cur.copy_expert(
                    sql=f"""
                        COPY "{esquema}"."{tabla}" 
                        FROM STDIN 
                        WITH CSV 
                        DELIMITER '|' 
                        NULL 'NULL' 
                        QUOTE '"' 
                        ESCAPE '"' 
                    """,
                    file=f
                )
        conn.commit()
    logger.info(f"✅ CSV cargado ({total_lineas} filas) en {esquema}.{tabla}")
    os.remove(csv_path)

@flow(name="importar_csv_pg")
def importar_csv_pg(esquema: str, tabla: str, nombre_zip: str):
    csv_file = descomprimir_archivo(f"/sftp/archivos/usr_diarco/orquestador/{nombre_zip}")
    validar_o_crear_tabla(esquema, tabla, csv_file)
    cargar_csv_postgres(csv_file, esquema, tabla)

if __name__ == "__main__":
    importar_csv_pg("repl", "T055_ARTICULOS_PARAM_STOCK", "repl_T055_ARTICULOS_PARAM_STOCK_20250527_150000.zip")
