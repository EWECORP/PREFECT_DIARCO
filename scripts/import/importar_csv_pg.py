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
        # Asumimos que hay un único archivo .csv dentro del ZIP
        nombres_archivos = zipf.namelist()
        archivo_csv_original = [f for f in nombres_archivos if f.lower().endswith(".csv")][0]
        
        zipf.extract(archivo_csv_original)  # Extrae con el nombre original
        
        # Renombrar a minúsculas
        archivo_csv_nuevo = archivo_csv_original.lower()
        if archivo_csv_original != archivo_csv_nuevo:
            os.rename(archivo_csv_original, archivo_csv_nuevo)

    return archivo_csv_nuevo


@task
def validar_o_crear_tabla(schema: str, tabla: str, archivo_csv: str):
    logger = get_run_logger()
    engine = create_engine(PG_CONN_STR)
    df_csv = pd.read_csv(archivo_csv, delimiter='|', dtype=str, nrows=100)

    tabla = tabla.lower()
    df_csv.columns = [col.lower() for col in df_csv.columns]

    with engine.connect() as conn:
        inspector = inspect(engine)
        if inspector.has_table(tabla, schema=schema):
            columnas_pg = [col['name'].lower() for col in inspector.get_columns(tabla, schema=schema)]
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
    tabla = tabla.lower()
    
    # Relee el archivo para verificar las columnas
    df_temp = pd.read_csv(csv_path, delimiter='|', dtype=str, nrows=1)
    columnas = [col.lower() for col in df_temp.columns]

    total_lineas = sum(1 for _ in open(csv_path, encoding='utf-8')) - 1
    
    with psycopg2.connect(**PG_RAW_CONN) as conn:
        with conn.cursor() as cur:
            with open(csv_path, "r", encoding="utf-8") as f:
                next(f)  # Skip header
                cur.copy_expert(
                    sql=f"""
                        COPY "{esquema}"."{tabla}" ({','.join(f'"{col}"' for col in columnas)})
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
    tabla = tabla.lower()
    csv_file = descomprimir_archivo(f"/sftp/archivos/usr_diarco/orquestador/{nombre_zip}")
    validar_o_crear_tabla(esquema, tabla, csv_file)
    cargar_csv_postgres(csv_file, esquema, tabla)

if __name__ == "__main__":
    importar_csv_pg("repl", "T055_ARTICULOS_PARAM_STOCK", "repl_T055_ARTICULOS_PARAM_STOCK_20250527_150000.zip")
