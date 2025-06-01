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

def enviar_por_sftp(nombre_zip):  
    zip_path = os.path.join(OUTPUT_DIR, nombre_zip) 
    print(f"Enviando archivo {zip_path} por SFTP a la ruta {SFTP_CONFIG['remote_path']}")
    
    enviar_archivo_sftp(zip_path, destino=SFTP_CONFIG['remote_path'], config=SFTP_CONFIG)
    os.remove(zip_path)

def exportar_tabla_sql_sftp(esquema: str, tabla: str, filtro_sql: str, nombre_zip: str):
    enviar_por_sftp(nombre_zip)

if __name__ == "__main__":
    exportar_tabla_sql_sftp("repl", "T710_ESTADIS_OFERTA_FOLDER", "", "repl_T710_ESTADIS_OFERTA_FOLDER_20250601_114606.zip")


