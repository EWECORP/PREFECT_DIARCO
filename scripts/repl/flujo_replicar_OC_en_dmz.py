from prefect import flow, task, get_run_logger
import pyodbc
import os
import sys
from dotenv import dotenv_values

# Replicación Encadenada en DMZ
# UTILIZA Stores Procedures dentro del mismo SQL

# === Variables del entorno ===
ENV_PATH = os.environ.get("ETL_ENV_PATH", "E:/ETL/ETL_DIARCO/.env")
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    sys.exit(1)
secrets = dotenv_values(ENV_PATH)

# Validar variables críticas
for var in ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]:
    if var not in secrets:
        raise KeyError(f"Falta variable de entorno: {var}")

# === Conexión con autocommit activado ===
def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={secrets['SQL_SERVER']};"
        f"DATABASE={secrets['SQL_DATABASE']};"
        f"UID={secrets['SQL_USER']};"
        f"PWD={secrets['SQL_PASSWORD']}",
        autocommit=True
    )

# === Tareas Prefect con robustez ===
@task(name="Ejecutar SP Cabecera OC", retries=2, retry_delay_seconds=60, log_prints=True)
def ejecutar_sp_cabecera():
    logger = get_run_logger()
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            logger.info("🛠️ Ejecutando SP: repl.usp_replicar_T080_OC_CABE")
            cursor.execute("EXEC repl.usp_replicar_T080_OC_CABE")
        return "✅ SP Cabecera ejecutado correctamente"
    except Exception as e:
        logger.error(f"❌ Error ejecutando SP Cabecera: {str(e)}")
        raise

@task(name="Ejecutar SP Detalle OC", retries=2, retry_delay_seconds=60, log_prints=True)
def ejecutar_sp_detalle():
    logger = get_run_logger()
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            logger.info("🛠️ Ejecutando SP: repl.usp_replicar_T081_OC_DETA")
            cursor.execute("EXEC repl.usp_replicar_T081_OC_DETA")
        return "✅ SP Detalle ejecutado correctamente"
    except Exception as e:
        logger.error(f"❌ Error ejecutando SP Detalle: {str(e)}")
        raise

# === Flujo de replicación encadenada ===
@flow(name="Flujo Replicacion OC CAB + DET")
def flujo_replicacion_oc():
    logger = get_run_logger()
    r1 = ejecutar_sp_cabecera.submit().result()
    logger.info(r1)
    if r1:
        r2 = ejecutar_sp_detalle.submit().result()
        logger.info(r2)
        return r2

if __name__ == "__main__":
    flujo_replicacion_oc()
