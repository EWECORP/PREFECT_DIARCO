# utils/postgres.py

import pandas as pd
import paramiko

from prefect import flow, task, get_run_logger
import zipfile
import os
import psycopg2
from datetime import datetime
import shutil  # Para mover archivos
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

PG_CONN_STR = "postgresql+psycopg2://postgres:aladelta10$@localhost:5432/diarco_data"

PG_RAW_CONN = {
    "host": "localhost",
    "port": "5432",
    "dbname": "diarco_data",
    "user": "postgres",
    "password": "aladelta10$"
}

# Directorio donde se encuentran los archivos CSV
directorio_archivos = '/sftp/archivos/usr_diarco/orquestador'
directorio_archivos_backup = '/sftp/archivos/usr_diarco/orquestador/backup'

# Función para mover un archivo después de procesarlo
def mover_archivo(archivo_origen, destino):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    nombre_archivo, extension = os.path.splitext(os.path.basename(archivo_origen))
    nuevo_nombre_archivo = f"{nombre_archivo}_{timestamp}{extension}"
    archivo_destino = os.path.join(destino, nuevo_nombre_archivo)
    
    shutil.move(archivo_origen, archivo_destino)
    print(f"Archivo {archivo_origen} movido a {archivo_destino}")

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
    # Permitir inferencia de tipos de datos
    df_csv = pd.read_csv(archivo_csv, delimiter='|', nrows=100)
    
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
    #👉 Los registros existentes no se reemplazan ni se modifican.
    #👉 Los nuevos registros se agregan al final de la tabla, como inserciones nuevas.
    
    with psycopg2.connect(**PG_RAW_CONN) as conn: # type: ignore
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
