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

# Validar variables necesarias
for var in ["SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD"]:
    if var not in secrets:
        raise KeyError(f"⚠️ Falta la variable de entorno: {var}")

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
    conn = None
    cursor = None
    try:
        conn = get_sqlserver_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {nombre_sp}")
        conn.commit()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info(f"✅ {nombre_sp} ejecutado en {duracion:.2f}s")
    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.warning(
                    f"⚠️ No se pudo revertir la transacción de {nombre_sp}: "
                    f"{rollback_error}"
                )
        logger.error(f"❌ Error en {nombre_sp}: {str(e)}")
        raise
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def esperar_resultados(resultados):
    """Espera todas las tareas y devuelve los nombres de las que fallaron."""
    fallidos = []
    for nombre_sp, resultado in resultados:
        try:
            resultado.result()
        except Exception as error:
            fallidos.append((nombre_sp, error))
    return fallidos


def ejecutar_en_serie_continuando(procedimientos):
    """Ejecuta todos los SP en orden, sin cortar la lista ante una falla."""
    fallidos = []
    for nombre_sp in procedimientos:
        try:
            ejecutar_sp(nombre_sp)
        except Exception as error:
            fallidos.append((nombre_sp, error))
    return fallidos

# === Flujo de replicación completo, con paralelismo y control ===
@flow(name="Flujo Replicacion DMZ Optimizado")
def sync_dmz_optimizado():
    logger = get_run_logger()
    fallidos = []

    # === SPs rápidos en paralelo ===
    logger.info("⏳ Ejecutando BATCH-RAPIDO - Grupo 1/6 ")
    batch_rapido = [
        "repl.usp_replicar_T020_PROVEEDOR",
        "repl.usp_replicar_T050_ARTICULOS",
        "repl.usp_replicar_T100_EMPRESA_SUC",
        "repl.usp_replicar_T114_RUBROS",
        "repl.usp_replicar_T117_COMPRADORES",
        "repl.usp_replicar_T020_PROVEEDOR_DIAS_ENTREGA_CABE",
        "repl.usp_replicar_T020_PROVEEDOR_DIAS_ENTREGA_DETA",
        "repl.usp_replicar_T874_PRECARGA_CONNEXA_HIST",
        "repl.usp_replicar_T_COMPETENCIA_DETALLE"
    ]
    resultados = [(sp, ejecutar_sp.submit(sp)) for sp in batch_rapido]
    fallidos.extend(esperar_resultados(resultados))

    # === SPs críticos (con dependencias) en serie ===
    logger.info("⏳ Ejecutando SP Encadenados - Grupo 2/6 ")
    fallidos.extend(ejecutar_en_serie_continuando([
        "repl.usp_replicar_T051_ARTICULOS_SUCURSAL",
        "repl.usp_replicar_T051_ARTICULOS_SUCURSAL_BARRIO",
        "repl.usp_replicar_T052_ARTICULOS_PROVEEDOR",
        "repl.usp_replicar_T060_STOCK",
        "repl.usp_replicar_M_3_ARTICULOS",
        "repl.usp_replicar_T080_OC_PENDIENTES",
        "repl.usp_replicar_T080_OC_CABE",
        "repl.usp_replicar_T081_OC_DETA"
    ]))

    # === Largos y pesados ===
    logger.info("⏳ Ejecutando ESTADISTICAS PESADAS - Grupo 3/6 ")
    fallidos.extend(ejecutar_en_serie_continuando([
        "repl.usp_replicar_T710_ESTADIS_REPOSICION",
        "repl.usp_replicar_T710_ESTADIS_STOCK",
        "repl.usp_replicar_T710_ESTADIS_OFERTA_FOLDER",
        "repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO",
        "repl.usp_replicar_T702_EST_VTAS_POR_ARTICULO_BARRIO"
    ]))

    # === Planes, condiciones, snc ===
    logger.info("⏳ Replicando ARTICULOS y PARAMETROS - Grupo 4/6 ")
    grupo_condiciones = [
        "repl.usp_replicar_T021_PROV_COMPROB",
        "repl.usp_replicar_T055_ARTICULOS_PARAM_STOCK",
        "repl.usp_replicar_T055_LEAD_TIME_B2_SUCURSALES",  
        "repl.usp_replicar_T230_FACTURADOR_NEGOCIOS_ESPECIALES_POR_CANTIDAD",
        "repl.usp_replicar_T085_ARTICULOS_EAN_EDI"
    ]
    resultados_cond = [(sp, ejecutar_sp.submit(sp)) for sp in grupo_condiciones]
    fallidos.extend(esperar_resultados(resultados_cond))

    # === Tableros Metabase Varios ===
    logger.info("⏳ Replicando INFO TABLEROS - Grupo 5/6 ")
    grupo_tableros = [
        "repl.usp_replicar_T020_PROVEEDOR_GESTION_COMPRA",
        "repl.usp_replicar_T055_ART_SUCU_PROV_DIAS_ENTREGA",
        "repl.usp_replicar_T055_ARTICULOS_CONDCOMPRA_COSTOS",
        "repl.usp_replicar_T000_SNC_PLAN_SEMANA_VIGENTE_DIA_ANT",
        "repl.usp_replicar_T079_SNC_CUOTAS_CABE",
        "repl.usp_replicar_T079_SNC_CUOTAS_DETA",
        "repl.usp_replicar_T804_HIST_MARCA_LISTO_PARA_VENTA",
        "repl.usp_replicar_T000_GESTION_COMPRA_PROVEEDOR_DETA_DIA_ANT",
        "repl.usp_replicar_USO_CONNEXA",
    ]
    resultados_tabs = [(sp, ejecutar_sp.submit(sp)) for sp in grupo_tableros]
    fallidos.extend(esperar_resultados(resultados_tabs))

    # === Competencia ===
    logger.info("⏳ Replicando COMPETENCIA - Grupo 6/6 ")
    fallidos.extend(ejecutar_en_serie_continuando([
        "repl.usp_replicar_T090_COMPETENCIA",
        "repl.usp_replicar_T091_COMPETENCIA_PRECIOS_CABE",
        "repl.usp_replicar_T091_COMPETENCIA_PRECIOS_DETA"
    ]))

    logger.info("✅ Replicación Precios de Competencia")
    
    fallidos.extend(ejecutar_en_serie_continuando([
        "repl.usp_replicar_T710_ESTADIS_PRECIOS"
    ]))

    if fallidos:
        resumen = "; ".join(
            f"{nombre_sp}: {error}" for nombre_sp, error in fallidos
        )
        logger.error(
            f"❌ Replicación finalizada con {len(fallidos)} SP fallido(s): {resumen}"
        )
        raise RuntimeError(
            f"La replicación terminó con {len(fallidos)} SP fallido(s): "
            f"{', '.join(nombre_sp for nombre_sp, _ in fallidos)}"
        )
    
    logger.info("✅ Replicación DMZ Optimizada Finalizada")

if __name__ == "__main__":
    sync_dmz_optimizado()
