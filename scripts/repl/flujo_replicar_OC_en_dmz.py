from prefect import flow, task, get_run_logger
import pyodbc
import os
import sys
from dotenv import dotenv_values

# Replicación Encadenada en DMZ
# UTILIZA Stores Procedures dentro del mismo SQL

#Variables del Entorno
ENV_PATH = os.environ.get("ETL_ENV_PATH", "E:/ETL/ETL_DIARCO/.env")  # Toma Producción si está definido, o la ruta por defecto E:\ETL\ETL_DIARCO\.env
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    print(f"Directorio actual: {os.getcwd()}")
    sys.exit(1)  
secrets = dotenv_values(ENV_PATH)
folder = f"{secrets['BASE_DIR']}/{secrets['FOLDER_DATOS']}"
folder_logs = f"{secrets['BASE_DIR']}/{secrets['FOLDER_LOG']}"
storage = f"{secrets['BASE_DIR']}/{secrets['STORAGE']}"


# Configuración de la conexión
def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={secrets['SQL_SERVER']};"
        f"DATABASE={secrets['SQL_DATABASE']};"
        f"UID={secrets['SQL_USER']};"
        f"PWD={secrets['SQL_PASSWORD']}"
    )

@task(name="Ejecutar SP Cabecera OC")
def ejecutar_sp_cabecera():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("EXEC repl.usp_replicar_T080_OC_CABE")
        cursor.commit()
    return "SP Cabecera ejecutado"

@task(name="Ejecutar SP Detalle OC")
def ejecutar_sp_detalle():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("EXEC repl.usp_replicar_T081_OC_DETA")
        cursor.commit()
    return "SP Detalle ejecutado"

@flow(name="Flujo Replicacion OC CAB + DET")
def flujo_replicacion_oc():
    logger = get_run_logger()
    r1 = ejecutar_sp_cabecera()
    
    logger.info(r1)
    if r1:
        r2 = ejecutar_sp_detalle()
        logger.info(r2)
        return r2

# Para correr el flujo manualmente
if __name__ == "__main__":
    flujo_replicacion_oc()
