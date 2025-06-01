from prefect import flow, task, get_run_logger
import zipfile
import os
import pandas as pd
import pyodbc
import sys
import psycopg2

# Obtener el directorio raíz del proyecto
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

print("📂 sys.path:")
for p in sys.path:
    print("  -", p)
    
# === Configuraciones generales ===
SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=10.54.200.92;"
    "DATABASE=data-sync;"
    "UID=data-sync;"
    "PWD=aladelta10$"
)

PG_CONN = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'diarco_data',
    'user': 'postgres',
    'password': 'your_password'
}

SFTP_CONFIG = {
    'host': '186.158.182.54',
    'port': 22,
    'username': 'usr_diarco',
    'password': 'diarco2024',
    'remote_path': './archivos/usr_diarco/orquestador'
}

OUTPUT_DIR = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

from utils.sftp import enviar_archivo_sftp

@task
def obtener_dataframe(esquema, tabla, filtro_sql):
    logger = get_run_logger()
    logger.info(f"Obteniendo datos de {esquema}.{tabla} con filtro: {filtro_sql}")
    query = f"SELECT * FROM {esquema}.{tabla}"
    if filtro_sql:
        query += f" WHERE {filtro_sql}"
    with pyodbc.connect(SQLSERVER_CONN_STR) as conn:
        df = pd.read_sql(query, conn) # type: ignore
    return df

@task
def exportar_y_comprimir(esquema, tabla, filtro_sql, nombre_zip):
    df = obtener_dataframe.fn(esquema, tabla, filtro_sql)
    nombre_csv = os.path.join(OUTPUT_DIR, nombre_zip.replace(".zip", ".csv"))
    logger = get_run_logger()
    logger.info(f"Exportando datos a {nombre_csv}")
    df.to_csv(nombre_csv, index=False, sep="|", na_rep="NULL")

    zip_path = os.path.join(OUTPUT_DIR, nombre_zip)
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(nombre_csv, arcname=os.path.basename(nombre_csv))
    os.remove(nombre_csv)
    return zip_path

@task
def enviar_por_sftp(zip_path):
    logger = get_run_logger()
    logger.info(f"Enviando archivo {zip_path} por SFTP a la ruta {SFTP_CONFIG['remote_path']}")
    enviar_archivo_sftp(zip_path, destino=SFTP_CONFIG['remote_path'], config=SFTP_CONFIG)
    os.remove(zip_path)

@flow(name="exportar_tabla_sql_sftp")
def exportar_tabla_sql_sftp(esquema: str, tabla: str, filtro_sql: str, nombre_zip: str):
    zip_generado = exportar_y_comprimir(esquema, tabla, filtro_sql, nombre_zip)
    enviar_por_sftp(zip_generado)

if __name__ == "__main__":
    exportar_tabla_sql_sftp("repl", "T055_ARTICULOS_PARAM_STOCK", "", "test.zip")


