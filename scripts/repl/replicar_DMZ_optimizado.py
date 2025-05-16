
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
import pyodbc
import os
import sys
from dotenv import dotenv_values

# === Variables de entorno ===
ENV_PATH = os.environ.get("ETL_ENV_PATH", "E:/ETL/ETL_DIARCO/.env")
if not os.path.exists(ENV_PATH):
    print(f"El archivo .env no existe en la ruta: {ENV_PATH}")
    sys.exit(1)
secrets = dotenv_values(ENV_PATH)

# === Conexión a SQL Server ===
def get_sqlserver_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={secrets['SQL_SERVER']};"
        f"DATABASE={secrets['SQL_DATABASE']};"
        f"UID={secrets['SQL_USER']};"
        f"PWD={secrets['SQL_PASSWORD']}"
    )
    return pyodbc.connect(conn_str)

# === Task para ejecutar un procedimiento almacenado ===
@task(retries=2, retry_delay_seconds=60, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=10))
def ejecutar_sp(nombre_sp: str):
    logger = get_run_logger()
    inicio = datetime.now()
    logger.info(f"🛠️ Ejecutando SP: {nombre_sp}")
    try:
        conn = get_sqlserver_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {nombre_sp}")
        conn.commit()
        cursor.close()
        conn.close()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info(f"✅ {nombre_sp} ejecutado en {duracion:.2f}s")
    except Exception as e:
        logger.error(f"❌ Error en {nombre_sp}: {str(e)}")
        raise

# === Flujo de replicación completo, con paralelismo ===
@flow(name="Flujo Replicacion DMZ Optimizado")
def sync_dmz_optimizado():
    logger = get_run_logger()

    # === SPs rápidos en paralelo ===
    batch_rapido = [
        "repl.usp_replicar_T020_PROVEEDOR",
        "repl.usp_replicar_T050_ARTICULOS",
        "repl.usp_replicar_T100_EMPRESA_SUC",
        "repl.usp_replicar_T114_RUBROS",
        "repl.usp_replicar_T117_COMPRADORES"
    ]
    resultados = [ejecutar_sp.submit(sp) for sp in batch_rapido]
    [r.result() for r in resultados]

    # === SPs críticos (con dependencias) en serie ===
    ejecutar_sp("repl.usp_replicar_T051_ARTICULOS_SUCURSAL")
    ejecutar_sp("repl.usp_replicar_T052_ARTICULOS_PROVEEDOR")
    ejecutar_sp("repl.usp_replicar_T060_STOCK")

    # === Largos y pesados ===
    for sp in [
        "repl.usp_replicar_T710_ESTADIS_REPOSICION",
        "repl.usp_replicar_T710_ESTADIS_STOCK",
        "repl.usp_replicar_T710_ESTADIS_OFERTA_FOLDER",
        "repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO",
    ]:
        ejecutar_sp(sp)

    # === Planes, condiciones, snc ===
    grupo_condiciones = [
        "repl.usp_replicar_T021_PROV_COMPROB",
        "repl.usp_replicar_T055_ARTICULOS_PARAM_STOCK",
        "repl.usp_replicar_T055_LEAD_TIME_B2_SUCURSALES",
        "repl.usp_replicar_T085_ARTICULOS_EAN_EDI"
    ]
    [ejecutar_sp.submit(sp) for sp in grupo_condiciones]

    # === Tableros Metabase Varios ===
    grupo_tableros = [
        "repl.usp_replicar_T020_PROVEEDOR_GESTION_COMPRA" ,
        "repl.usp_replicar_T055_ART_SUCU_PROV_DIAS_ENTREGA",
        "repl.usp_replicar_T055_ARTICULOS_CONDCOMPRA_COSTOS",
        "repl.usp_replicar_T000_SNC_PLAN_SEMANA_VIGENTE_DIA_ANT",
        "repl.usp_replicar_T079_SNC_CUOTAS_CABE",
        "repl.usp_replicar_T079_SNC_CUOTAS_DETA",
        "repl.usp_replicar_T000_GESTION_COMPRA_PROVEEDOR_DETA_DIA_ANT"
    ]
    [ejecutar_sp.submit(sp) for sp in grupo_tableros]

    # === Competencia ===
    for sp in [
        "repl.usp_replicar_T090_COMPETENCIA",
        "repl.usp_replicar_T091_COMPETENCIA_PRECIOS_CABE",
        "repl.usp_replicar_T091_COMPETENCIA_PRECIOS_DETA"
    ]:
        ejecutar_sp(sp)

    logger.info("🚀 Replicación DMZ Optimizada Finalizada")

if __name__ == "__main__":
    sync_dmz_optimizado()
