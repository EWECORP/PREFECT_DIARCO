from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
import pyodbc
import os
import sys
from dotenv import dotenv_values

# Replicación DMZ Completa
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


logger = get_run_logger()

# === Configuración de conexión SQL Server ===
def get_sqlserver_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={secrets['SQL_SERVER']};"
        f"DATABASE={secrets['SQL_DATABASE']};"
        f"UID={secrets['SQL_USER']};"
        f"PWD={secrets['SQL_PASSWORD']}"
    )
    return pyodbc.connect(conn_str)

# === Task genérica para ejecutar un procedimiento SQL Server ===
@task(retries=3, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10))
def ejecutar_sp(nombre_sp: str):
#    logger = get_run_logger()
    inicio = datetime.now()
    logger.info(f"⏳ Ejecutando procedimiento: {nombre_sp}")

    try:
        conn = get_sqlserver_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {nombre_sp}")
        conn.commit()
        cursor.close()
        conn.close()

        duracion = (datetime.now() - inicio).total_seconds()
        logger.info(f"✅ {nombre_sp} ejecutado correctamente en {duracion:.2f} segundos.")
        return f"✅ {nombre_sp} ejecutado"

    except Exception as e:
        logger.error(f"❌ Error al ejecutar {nombre_sp}: {str(e)}")
        raise

@flow(name="Flujo Replicacion DMZ COMPLETA")
def sync_DMZ_completo():
    ejecutar_sp("repl.usp_replicar_T000_GESTION_COMPRA_PROVEEDOR_DETA_DIA_ANT")    
    ejecutar_sp("repl.usp_replicar_T000_SNC_PLAN_SEMANA_VIGENTE_DIA_ANT")
    ejecutar_sp("repl.usp_replicar_T020_PROVEEDOR")
    ejecutar_sp("repl.usp_replicar_T020_PROVEEDOR_GESTION_COMPRA")
    ejecutar_sp("repl.usp_replicar_T021_PROV_COMPROB")
    ejecutar_sp("repl.usp_replicar_T050_ARTICULOS")
    ejecutar_sp("repl.usp_replicar_T051_ARTICULOS_SUCURSAL")
    ejecutar_sp("repl.usp_replicar_T052_ARTICULOS_PROVEEDOR")
    ejecutar_sp("repl.usp_replicar_T055_ART_SUCU_PROV_DIAS_ENTREGA")
    ejecutar_sp("repl.usp_replicar_T055_ARTICULOS_CONDCOMPRA_COSTOS")
    ejecutar_sp("repl.usp_replicar_T055_ARTICULOS_PARAM_STOCK")
    ejecutar_sp("repl.usp_replicar_T055_LEAD_TIME_B2_SUCURSALES")
    ejecutar_sp("repl.usp_replicar_T060_STOCK")    
    ejecutar_sp("repl.usp_replicar_T079_SNC_CUOTAS_CABE")
    ejecutar_sp("repl.usp_replicar_T079_SNC_CUOTAS_DETA")    
    ejecutar_sp("repl.usp_replicar_T085_ARTICULOS_EAN_EDI")
    ejecutar_sp("repl.usp_replicar_T090_COMPETENCIA")
    ejecutar_sp("repl.usp_replicar_T091_COMPETENCIA_PRECIOS_CABE")
    ejecutar_sp("repl.usp_replicar_T091_COMPETENCIA_PRECIOS_DETA")
    ejecutar_sp("repl.usp_replicar_T100_EMPRESA_SUC")
    ejecutar_sp("repl.usp_replicar_T114_RUBROS")
    ejecutar_sp("repl.usp_replicar_T117_COMPRADORES")
    ejecutar_sp("repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO")  # TARDA MUCHO !!!! Solo 2025 se está extrayendo
    ejecutar_sp("repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO_BARRIO")  # NUEVA INCLUSIÖN
    ejecutar_sp("repl.usp_replicar_T710_ESTADIS_OFERTA_FOLDER")  # TARDA MUCHO !!!! Solo 2025 se está extrayendo
    ejecutar_sp("repl.usp_replicar_T710_ESTADIS_REPOSICION")  # TARDA MUCHO !!!! Solo 2025 se está extrayendo
    ejecutar_sp("repl.usp_replicar_T710_ESTADIS_STOCK")  # TARDA MUCHO !!!! Solo 2025 se está extrayendo
    # r1= ejecutar_sp("repl.usp_replicar_T080_OC_CABE")   # Se pasan a Script Independiente porque van encadenados
    # if r1:
    #     ejecutar_sp("repl.usp_replicar_T081_OC_DETA")   # OJO Tiene JOIN con CABECERA


if __name__ == "__main__":
    sync_DMZ_completo()
    logger.info("--------------->  Flujo de replicación a DMZ FINALIZADO.")
