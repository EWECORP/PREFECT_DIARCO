from prefect import flow, task, get_run_logger
import pandas as pd
import pyodbc
import os
from datetime import datetime

# Parámetros configurables
SERVER = "10.54.200.92"
DATABASE = "data-sync"
SP_NAME = "[dbo].[SP_CNX_T_4_PRODUCTOS_SUCU]"
OUTPUT_DIR = "/srv/EXPORTACIONES"
FILE_PREFIX = "productos_sucu"
CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes"

@task
def ejecutar_sp_y_exportar():
    logger = get_run_logger()
    logger.info("Conectando a SQL Server...")
    with pyodbc.connect(CONN_STR) as conn:
        df = pd.read_sql(f"EXEC {SP_NAME}", conn)

    if df.empty:
        logger.warning("El SP no devolvió registros. Proceso cancelado.")
        return

    fecha = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}_{fecha}.csv")

    logger.info(f"Exportando archivo a {output_file}")
    df.to_csv(output_file, sep='|', index=False, encoding='utf-8')

    logger.info("Archivo generado correctamente.")
    return output_file

@flow(name="Exportar Productos por Sucursal")
def flujo_exportar_productos_sucu():
    ejecutar_sp_y_exportar()

if __name__ == "__main__":
    flujo_exportar_productos_sucu()
