from datetime import date, timedelta
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
import os


def get_pg_engine():
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


@task(log_prints=True)
def ejecutar_sp_promos(fecha_desde: date, fecha_hasta: date, actualizar_base_original: bool = True):
    logger = get_run_logger()
    engine = get_pg_engine()

    logger.info(f"Procesando promociones BVE desde {fecha_desde} hasta {fecha_hasta}")

    sql = text("""
        CALL datamart.sp_procesar_promos_mes(
            :fecha_desde,
            :fecha_hasta,
            :actualizar_base_original
        );
    """)

    with engine.begin() as conn:
        conn.execute(sql, {
            "fecha_desde": fecha_desde,
            "fecha_hasta": fecha_hasta,
            "actualizar_base_original": actualizar_base_original
        })

    logger.info("Stored procedure ejecutado correctamente.")


@task(log_prints=True)
def validar_ultimo_log():
    logger = get_run_logger()
    engine = get_pg_engine()

    sql = text("""
        SELECT
            id,
            estado,
            mes_desde,
            mes_hasta,
            baseline_registros,
            ventas_enriquecidas_registros,
            promos_fuertes_registros,
            duracion_segundos,
            mensaje,
            error_detalle
        FROM datamart.dm_bve_proceso_log
        WHERE proceso = 'sp_procesar_promos_mes'
        ORDER BY id DESC
        LIMIT 1;
    """)

    with engine.connect() as conn:
        row = conn.execute(sql).mappings().first()

    if row is None:
        raise RuntimeError("No se encontró log del proceso.")

    logger.info(f"Último log: {dict(row)}")

    if row["estado"] != "OK":
        raise RuntimeError(f"Proceso con estado {row['estado']}: {row['error_detalle']}")

    return dict(row)


@flow(name="Procesar Promociones Base Ventas Extendida")
def procesar_promos_bve(
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
    actualizar_base_original: bool = True
):
    hoy = date.today()

    if fecha_desde is None:
        fecha_desde = hoy.replace(day=1)

    if fecha_hasta is None:
        fecha_hasta = hoy + timedelta(days=1)

    ejecutar_sp_promos(fecha_desde, fecha_hasta, actualizar_base_original)
    log = validar_ultimo_log()

    return log


if __name__ == "__main__":
    procesar_promos_bve()