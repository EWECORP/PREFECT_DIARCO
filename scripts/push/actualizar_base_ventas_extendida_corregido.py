# actualizar_base_ventas_extendida.py
import os
import sys
import time
import logging
from datetime import date, datetime, timedelta
from typing import Optional, Tuple

import psycopg2 as pg2
from psycopg2 import OperationalError, InterfaceError
from psycopg2.extensions import connection as PGConnection
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

# =========================
# Logging compartido
# =========================
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)

if not logger.handlers:
    file_handler = logging.FileHandler("logs/replicacion_psycopg2.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# =========================
# Cargar .env
# =========================
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# =========================
# Parámetros operativos
# =========================
STATEMENT_TIMEOUT_MS = int(os.getenv("BVE_STATEMENT_TIMEOUT_MS", "0"))
LOCK_TIMEOUT_MS = int(os.getenv("BVE_LOCK_TIMEOUT_MS", "300000"))
RETRY_ATTEMPTS = int(os.getenv("BVE_RETRY_ATTEMPTS", "3"))
RETRY_SLEEP_SECONDS = int(os.getenv("BVE_RETRY_SLEEP_SECONDS", "8"))
UPDATE_BATCH_DAYS = int(os.getenv("BVE_UPDATE_BATCH_DAYS", "31"))
APP_NAME = os.getenv("BVE_APP_NAME", "actualizar_base_ventas_extendida")

# =========================
# Conexión PostgreSQL
# =========================
def open_pg_conn(autocommit: bool = False) -> PGConnection:
    conn = pg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        application_name=APP_NAME,
        connect_timeout=30,
    )
    conn.autocommit = autocommit
    return conn


def configure_session(conn: PGConnection) -> None:
    with conn.cursor() as cur:
        cur.execute("SET client_min_messages TO WARNING;")
        cur.execute("SET idle_in_transaction_session_timeout TO '10min';")
        cur.execute("SET lock_timeout = %s;", (f"{LOCK_TIMEOUT_MS}ms",))
        if STATEMENT_TIMEOUT_MS > 0:
            cur.execute("SET statement_timeout = %s;", (f"{STATEMENT_TIMEOUT_MS}ms",))
        else:
            cur.execute("SET statement_timeout = '0';")
    conn.commit()


# =========================
# SQL helpers
# =========================
def exec_sql(cur, sql: str, params=None, log_prefix: str = ""):
    if log_prefix:
        logger.info(f"{log_prefix} -> ejecutando SQL...")
    cur.execute(sql, params)


def fetch_one(cur, sql: str, params=None):
    cur.execute(sql, params)
    return cur.fetchone()


def execute_with_retry(fn, description: str, retries: int = RETRY_ATTEMPTS):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"{description} | intento {attempt}/{retries}")
            return fn()
        except (OperationalError, InterfaceError) as exc:
            last_error = exc
            logger.exception(f"{description} | error de conexión/intento {attempt}: {exc}")
            if attempt >= retries:
                raise
            time.sleep(RETRY_SLEEP_SECONDS)
        except Exception:
            raise
    if last_error:
        raise last_error


# =========================
# Utilidades de rango
# =========================
def month_start(d: date) -> date:
    return d.replace(day=1)


def next_month(d: date) -> date:
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def iter_month_ranges(start_date: date, end_date: date):
    current = month_start(start_date)
    while current <= end_date:
        following = next_month(current)
        batch_from = max(current, start_date)
        batch_to = min(following - timedelta(days=1), end_date)
        yield batch_from, batch_to
        current = following


# =========================
# SQL blocks base
# =========================
SQL_TRUNCATE_STG = "TRUNCATE TABLE src.base_ventas_extendida__stg;"

SQL_INSERT_STG = """
WITH max_dest AS (
    SELECT COALESCE(MAX(fecha), DATE '1900-01-01') AS max_fecha
    FROM src.base_ventas_extendida
),
params AS (
    SELECT (max_fecha - (%s || ' days')::interval)::date AS fecha_desde
    FROM max_dest
)
INSERT INTO src.base_ventas_extendida__stg(
    fecha, codigo_articulo, sucursal, precio, unidades, importe_vendido,
    con_stock, venta_especial, promo_normal, promo_fuerte,
    precio_prefijado, factor_precio,
    costo, familia, rubro, subrubro, c_proveedor_primario,
    nombre_articulo, clasificacion, fecha_procesado, marca_procesado
)

-- ============================
-- 1) VENTAS < 300
-- ============================
SELECT
    V.f_venta::date,
    V.c_articulo::bigint,
    V.c_sucu_empr::int,
    V.i_precio_venta::numeric(18,6),
    V.q_unidades_vendidas::numeric(18,6),
    V.i_vendido::numeric(18,6),
    TRUE,
    FALSE,
    FALSE,
    FALSE,
    NULL::numeric AS precio_prefijado,
	NULL::numeric AS factor_precio,

    V.i_precio_costo::numeric(18,6),
    V.c_familia::int,
    A.c_rubro::int,
    A.c_subrubro_1::int,
    A.c_proveedor_primario::int,
    TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(A.n_articulo, CHR(9), ''), CHR(13), ''), CHR(10), '')),
    A.c_clasificacion_compra::int,
    CURRENT_TIMESTAMP,
    0
FROM src.t702_est_vtas_por_articulo V
LEFT JOIN src.t050_articulos A
       ON V.c_articulo = A.c_articulo
CROSS JOIN params p
WHERE V.f_venta::date >= p.fecha_desde
  AND A.m_baja = 'N'
  AND V.c_sucu_empr < 300

UNION ALL

-- ============================
-- 2) VENTAS >= 300
-- ============================
SELECT
    V.f_venta::date,
    V.c_articulo::bigint,
    V.c_sucu_empr::int,
    V.i_precio_venta::numeric(18,6),
    V.q_unidades_vendidas::numeric(18,6),
    V.i_vendido::numeric(18,6),
    TRUE,
    FALSE,
    FALSE,
    FALSE,
	NULL::numeric AS precio_prefijado,
	NULL::numeric AS factor_precio,

    V.i_precio_costo::numeric(18,6),
    V.c_familia::int,
    A.c_rubro::int,
    A.c_subrubro_1::int,
    A.c_proveedor_primario::int,
    TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(A.n_articulo, CHR(9), ''), CHR(13), ''), CHR(10), '')),
    A.c_clasificacion_compra::int,
    CURRENT_TIMESTAMP,
    0
FROM src.t702_est_vtas_por_articulo_dbarrio V
LEFT JOIN src.t050_articulos A
       ON V.c_articulo = A.c_articulo
CROSS JOIN params p
WHERE V.f_venta::date >= p.fecha_desde
  AND A.m_baja = 'N'
  AND V.c_sucu_empr >= 300;
"""

SQL_CREATE_STG_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_bve_stg_fecha_art_suc_stg ON src.base_ventas_extendida__stg (fecha, codigo_articulo, sucursal);
CREATE INDEX IF NOT EXISTS idx_bve_stg_art_suc_fecha_stg ON src.base_ventas_extendida__stg (codigo_articulo, sucursal, fecha);
"""

SQL_ANALYZE_STG = "ANALYZE src.base_ventas_extendida__stg;"

SQL_GET_STG_RANGE = """
SELECT MIN(fecha)::date AS fecha_min, MAX(fecha)::date AS fecha_max, COUNT(*)::bigint AS filas
FROM src.base_ventas_extendida__stg;
"""

SQL_CREATE_TMP_OFERTAS = """
CREATE TEMP TABLE tmp_bve_ofertas ON COMMIT DROP AS
WITH stg_keys AS (
    SELECT DISTINCT codigo_articulo, sucursal, fecha
    FROM src.base_ventas_extendida__stg
    WHERE fecha >= %(fecha_desde)s AND fecha < %(fecha_hasta)s
),
oferta AS (
    SELECT
        ofe.c_articulo,
        ofe.c_sucu_empr,
        make_date(ofe.c_anio::int, ofe.c_mes::int, u.d::int) AS fecha,
        NULLIF(BTRIM(UPPER(u.val::text)), '') AS flag
    FROM src.t710_estadis_oferta_folder ofe
    CROSS JOIN LATERAL unnest(ARRAY[
        ofe.m_oferta_dia1,  ofe.m_oferta_dia2,  ofe.m_oferta_dia3,  ofe.m_oferta_dia4,  ofe.m_oferta_dia5,
        ofe.m_oferta_dia6,  ofe.m_oferta_dia7,  ofe.m_oferta_dia8,  ofe.m_oferta_dia9,  ofe.m_oferta_dia10,
        ofe.m_oferta_dia11, ofe.m_oferta_dia12, ofe.m_oferta_dia13, ofe.m_oferta_dia14, ofe.m_oferta_dia15,
        ofe.m_oferta_dia16, ofe.m_oferta_dia17, ofe.m_oferta_dia18, ofe.m_oferta_dia19, ofe.m_oferta_dia20,
        ofe.m_oferta_dia21, ofe.m_oferta_dia22, ofe.m_oferta_dia23, ofe.m_oferta_dia24, ofe.m_oferta_dia25,
        ofe.m_oferta_dia26, ofe.m_oferta_dia27, ofe.m_oferta_dia28, ofe.m_oferta_dia29, ofe.m_oferta_dia30,
        ofe.m_oferta_dia31
    ]) WITH ORDINALITY AS u(val, d)
    WHERE u.d <= EXTRACT(DAY FROM (date_trunc('month', make_date(ofe.c_anio::int, ofe.c_mes::int, 1))
                                    + INTERVAL '1 month - 1 day'))
      AND make_date(ofe.c_anio::int, ofe.c_mes::int, 1) >= date_trunc('month', %(fecha_desde)s::date)
      AND make_date(ofe.c_anio::int, ofe.c_mes::int, 1) < date_trunc('month', %(fecha_hasta)s::date) + INTERVAL '1 month'
)
SELECT
    o.c_articulo,
    o.c_sucu_empr,
    o.fecha,
    o.flag
FROM oferta o
JOIN stg_keys s
  ON s.codigo_articulo = o.c_articulo
 AND s.sucursal        = o.c_sucu_empr
 AND s.fecha           = o.fecha;
"""

SQL_INDEX_TMP_OFERTAS = """
CREATE INDEX idx_tmp_bve_ofertas_key ON tmp_bve_ofertas (c_articulo, c_sucu_empr, fecha);
ANALYZE tmp_bve_ofertas;
"""

SQL_UPDATE_OFERTAS_BATCH = """
UPDATE src.base_ventas_extendida__stg b
SET promo_normal = (o.flag = 'S'),
    promo_fuerte = (o.flag = 'F')
FROM tmp_bve_ofertas o
WHERE b.fecha >= %(fecha_desde)s
  AND b.fecha < %(fecha_hasta)s
  AND b.codigo_articulo = o.c_articulo
  AND b.sucursal        = o.c_sucu_empr
  AND b.fecha           = o.fecha;
"""

SQL_CREATE_TMP_PRECIOS = """
CREATE TEMP TABLE tmp_bve_precios ON COMMIT DROP AS
WITH stg_keys AS (
    SELECT DISTINCT codigo_articulo, sucursal, fecha
    FROM src.base_ventas_extendida__stg
    WHERE fecha >= %(fecha_desde)s AND fecha < %(fecha_hasta)s
),
precios AS (
    SELECT
        ep.c_articulo,
        ep.c_sucu_empr,
        make_date(ep.c_anio::int, ep.c_mes::int, u.d::int) AS fecha,
        u.val::numeric(18,6) AS precio_prefijado
    FROM src.t710_estadis_precios ep
    CROSS JOIN LATERAL unnest(ARRAY[
        ep.i_precio_vta_1,  ep.i_precio_vta_2,  ep.i_precio_vta_3,  ep.i_precio_vta_4,  ep.i_precio_vta_5,
        ep.i_precio_vta_6,  ep.i_precio_vta_7,  ep.i_precio_vta_8,  ep.i_precio_vta_9,  ep.i_precio_vta_10,
        ep.i_precio_vta_11, ep.i_precio_vta_12, ep.i_precio_vta_13, ep.i_precio_vta_14, ep.i_precio_vta_15,
        ep.i_precio_vta_16, ep.i_precio_vta_17, ep.i_precio_vta_18, ep.i_precio_vta_19, ep.i_precio_vta_20,
        ep.i_precio_vta_21, ep.i_precio_vta_22, ep.i_precio_vta_23, ep.i_precio_vta_24, ep.i_precio_vta_25,
        ep.i_precio_vta_26, ep.i_precio_vta_27, ep.i_precio_vta_28, ep.i_precio_vta_29, ep.i_precio_vta_30,
        ep.i_precio_vta_31
    ]) WITH ORDINALITY AS u(val, d)
    WHERE u.val IS NOT NULL
      AND u.d <= EXTRACT(DAY FROM (date_trunc('month', make_date(ep.c_anio::int, ep.c_mes::int, 1))
                                    + INTERVAL '1 month - 1 day'))
      AND make_date(ep.c_anio::int, ep.c_mes::int, 1) >= date_trunc('month', %(fecha_desde)s::date)
      AND make_date(ep.c_anio::int, ep.c_mes::int, 1) < date_trunc('month', %(fecha_hasta)s::date) + INTERVAL '1 month'
)
SELECT
    p.c_articulo,
    p.c_sucu_empr,
    p.fecha,
    p.precio_prefijado
FROM precios p
JOIN stg_keys s
  ON s.codigo_articulo = p.c_articulo
 AND s.sucursal        = p.c_sucu_empr
 AND s.fecha           = p.fecha;
"""

SQL_INDEX_TMP_PRECIOS = """
CREATE INDEX idx_tmp_bve_precios_key ON tmp_bve_precios (c_articulo, c_sucu_empr, fecha);
ANALYZE tmp_bve_precios;
"""

SQL_UPDATE_PRECIOS_BATCH = """
UPDATE src.base_ventas_extendida__stg b
SET precio_prefijado = p.precio_prefijado
FROM tmp_bve_precios p
WHERE b.fecha >= %(fecha_desde)s
  AND b.fecha < %(fecha_hasta)s
  AND b.codigo_articulo = p.c_articulo
  AND b.sucursal        = p.c_sucu_empr
  AND b.fecha           = p.fecha;
"""

SQL_UPDATE_FACTOR_PRECIO = """
UPDATE src.base_ventas_extendida__stg
SET factor_precio =
    CASE
        WHEN precio_prefijado IS NULL OR precio_prefijado = 0 THEN 1
        ELSE ROUND(precio / precio_prefijado, 6)
    END
WHERE fecha >= %s AND fecha < %s;
"""

SQL_DEDUP_STG = """
WITH ranked AS (
  SELECT ctid,
         ROW_NUMBER() OVER (
            PARTITION BY fecha, codigo_articulo, sucursal, precio
            ORDER BY fecha_procesado DESC, ctid DESC
         ) AS rn
  FROM src.base_ventas_extendida__stg
)
DELETE FROM src.base_ventas_extendida__stg s
USING ranked r
WHERE s.ctid = r.ctid
  AND r.rn > 1;
"""

SQL_UPSERT_DEST = """
INSERT INTO src.base_ventas_extendida AS d
SELECT *
FROM src.base_ventas_extendida__stg s
ON CONFLICT (fecha, codigo_articulo, sucursal, precio)
DO UPDATE
SET unidades             = EXCLUDED.unidades,
    importe_vendido      = EXCLUDED.importe_vendido,
    con_stock            = EXCLUDED.con_stock,
    venta_especial       = EXCLUDED.venta_especial,
    promo_normal         = EXCLUDED.promo_normal,
    promo_fuerte         = EXCLUDED.promo_fuerte,
    precio_prefijado     = EXCLUDED.precio_prefijado,
    factor_precio        = EXCLUDED.factor_precio,
    costo                = EXCLUDED.costo,
    familia              = EXCLUDED.familia,
    rubro                = EXCLUDED.rubro,
    subrubro             = EXCLUDED.subrubro,
    c_proveedor_primario = EXCLUDED.c_proveedor_primario,
    nombre_articulo      = EXCLUDED.nombre_articulo,
    clasificacion        = EXCLUDED.clasificacion,
    fecha_procesado      = EXCLUDED.fecha_procesado,
    marca_procesado      = EXCLUDED.marca_procesado;
"""

SQL_COUNT_STG = "SELECT COUNT(*) FROM src.base_ventas_extendida__stg;"


# =========================
# Tareas auxiliares
# =========================
def _get_stg_range() -> Tuple[Optional[date], Optional[date], int]:
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                fecha_min, fecha_max, filas = fetch_one(cur, SQL_GET_STG_RANGE) # type: ignore
            conn.commit()
            return fecha_min, fecha_max, int(filas or 0)

    return execute_with_retry(_run, "Obtener rango STG") # type: ignore


def _execute_monthly_batches(build_sql: str, index_sql: str, update_sql: str, description: str):
    fecha_min, fecha_max, filas = _get_stg_range()
    if not fecha_min or not fecha_max or filas == 0:
        logger.info(f"{description} | STG sin filas. Se omite proceso.")
        return

    logger.info(f"{description} | rango STG: {fecha_min} a {fecha_max} | filas={filas:,}")

    for batch_from, batch_to in iter_month_ranges(fecha_min, fecha_max):
        batch_to_exclusive = batch_to + timedelta(days=1)
        params = {"fecha_desde": batch_from, "fecha_hasta": batch_to_exclusive}

        def _run_batch():
            with open_pg_conn() as conn:
                configure_session(conn)
                with conn.cursor() as cur:
                    logger.info(f"{description} | lote {batch_from} -> {batch_to}")
                    exec_sql(cur, build_sql, params=params, log_prefix=f"{description} | crear tmp {batch_from}..{batch_to}")
                    exec_sql(cur, index_sql, log_prefix=f"{description} | index/analyze tmp {batch_from}..{batch_to}")
                    exec_sql(cur, update_sql, params=params, log_prefix=f"{description} | update {batch_from}..{batch_to}")
                conn.commit()

        execute_with_retry(_run_batch, f"{description} [{batch_from}..{batch_to}]")


# =========================
# Prefect tasks
# =========================
@task(name="Truncar_Staging")
def t_truncate_stg():
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                exec_sql(cur, SQL_TRUNCATE_STG, log_prefix="Truncar STG")
            conn.commit()
        logger.info("STG truncada.")

    execute_with_retry(_run, "Truncar STG")


@task(name="Insertar_STG_desde_fuentes")
def t_insert_stg(window_days: int = 14):
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                exec_sql(cur, SQL_INSERT_STG, params=(window_days,), log_prefix=f"Insert STG (ventana={window_days} días)")
                exec_sql(cur, SQL_CREATE_STG_INDEXES, log_prefix="Crear índices STG")
                exec_sql(cur, SQL_ANALYZE_STG, log_prefix="ANALYZE STG")
            conn.commit()
        logger.info("Carga STG completada.")

    execute_with_retry(_run, f"Insertar STG ventana={window_days}")


@task(name="Enriquecer_Ofertas")
def t_update_ofertas():
    _execute_monthly_batches(
        build_sql=SQL_CREATE_TMP_OFERTAS,
        index_sql=SQL_INDEX_TMP_OFERTAS,
        update_sql=SQL_UPDATE_OFERTAS_BATCH,
        description="Actualizar OFERTAS",
    )
    logger.info("STG enriquecida con OFERTAS.")


@task(name="Enriquecer_Precios_Prefijados")
def t_update_precios_prefijados():
    _execute_monthly_batches(
        build_sql=SQL_CREATE_TMP_PRECIOS,
        index_sql=SQL_INDEX_TMP_PRECIOS,
        update_sql=SQL_UPDATE_PRECIOS_BATCH,
        description="Actualizar PRECIOS PREFIJADOS",
    )
    logger.info("STG enriquecida con PRECIOS PREFIJADOS.")


@task(name="Calcular_Factor_Precio")
def t_update_factor_precio():
    fecha_min, fecha_max, filas = _get_stg_range()
    if not fecha_min or not fecha_max or filas == 0:
        logger.info("Calcular FACTOR_PRECIO | STG sin filas. Se omite proceso.")
        return

    for batch_from, batch_to in iter_month_ranges(fecha_min, fecha_max):
        batch_to_exclusive = batch_to + timedelta(days=1)

        def _run():
            with open_pg_conn() as conn:
                configure_session(conn)
                with conn.cursor() as cur:
                    exec_sql(
                        cur,
                        SQL_UPDATE_FACTOR_PRECIO,
                        params=(batch_from, batch_to_exclusive),
                        log_prefix=f"Calcular FACTOR_PRECIO {batch_from}..{batch_to}",
                    )
                conn.commit()

        execute_with_retry(_run, f"Calcular FACTOR_PRECIO [{batch_from}..{batch_to}]")

    logger.info("STG actualizada con FACTOR_PRECIO.")


@task(name="Deduplicar_STG")
def t_dedup_stg():
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                exec_sql(cur, SQL_DEDUP_STG, log_prefix="De-dup STG")
                exec_sql(cur, SQL_ANALYZE_STG, log_prefix="ANALYZE STG post de-dup")
            conn.commit()
        logger.info("De-duplicación en STG finalizada.")

    execute_with_retry(_run, "Deduplicar STG")


@task(name="Upsert_Destino")
def t_upsert_destino():
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                exec_sql(cur, SQL_UPSERT_DEST, log_prefix="Upsert DESTINO")
            conn.commit()
        logger.info("Upsert a DESTINO completado.")

    execute_with_retry(_run, "Upsert destino")


@task(name="Analyze_Post_Carga")
def t_analyze_post():
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                exec_sql(cur, "ANALYZE src.base_ventas_extendida;", log_prefix="ANALYZE destino")
                exec_sql(cur, SQL_ANALYZE_STG, log_prefix="ANALYZE STG")
            conn.commit()
        logger.info("ANALYZE ejecutado en DESTINO y STG.")

    execute_with_retry(_run, "Analyze post carga")


@task(name="DQ_Chequeos_Basicos")
def t_dq_basicos():
    def _run():
        with open_pg_conn() as conn:
            configure_session(conn)
            with conn.cursor() as cur:
                cnt_stg = fetch_one(cur, SQL_COUNT_STG)[0] # pyright: ignore[reportOptionalSubscript]
                fecha_min, fecha_max, _ = fetch_one(cur, SQL_GET_STG_RANGE) # type: ignore
            conn.commit()
            return int(cnt_stg or 0), fecha_min, fecha_max

    cnt_stg, fecha_min, fecha_max = execute_with_retry(_run, "DQ básicos") # type: ignore
    logger.info(
        "DQ: filas en STG tras enriquecimiento = %s | fecha_min=%s | fecha_max=%s",
        f"{cnt_stg:,}",
        fecha_min,
        fecha_max,
    )
    return {"stg_rows": cnt_stg, "fecha_min": str(fecha_min), "fecha_max": str(fecha_max)}


# =========================
# Prefect flow
# =========================
@flow(name="actualizar_base_ventas_extendida")
def actualizar_base_ventas_extendida(window_days: int = 14, analyze: bool = True):
    """
    Mantiene src.base_ventas_extendida con staging y upsert idempotente.
    Pasos:
      1) Truncar STG
      2) Insertar STG desde fuentes (ventana de re-proceso = window_days)
      3) Enriquecer: ofertas, precios prefijados
      4) Calcular factor_precio
      5) De-duplicar STG
      6) Upsert a destino
      7) DQ básico + ANALYZE opcional
    Mejoras incorporadas:
      1) Reintentos automáticos ante cortes de conexión.
      2) Configuración de sesión (statement_timeout / lock_timeout / application_name).
      3) Enriquecimiento de ofertas y precios prefijados por lotes mensuales.
      4) Materialización previa en tablas temporales filtradas por las claves reales de STG.
      5) Índices y ANALYZE sobre STG y temporales para mejorar el plan.
      6) Cálculo de factor_precio también particionado por mes.
    """
    log = get_run_logger()
    log.info(
        "[INICIO] actualizar_base_ventas_extendida | ventana=%s días | retry=%s | timeout_ms=%s",
        window_days,
        RETRY_ATTEMPTS,
        STATEMENT_TIMEOUT_MS,
    )

    t_truncate_stg.submit().result()
    t_insert_stg.with_options(name="Insertar STG").submit(window_days).result()
    t_update_ofertas.submit().result()
    t_update_precios_prefijados.submit().result()
    t_update_factor_precio.submit().result()
    t_dedup_stg.submit().result()
    t_upsert_destino.submit().result()
    dq = t_dq_basicos.submit().result()
    if analyze:
        t_analyze_post.submit().result()

    log.info(f"[FIN] actualizar_base_ventas_extendida | DQ={dq}")


# =========================
# CLI
# =========================
if __name__ == "__main__":
    # Uso:
    #   python actualizar_base_ventas_extendida_corregido.py
    #   python actualizar_base_ventas_extendida_corregido.py 30 true
    wdays = int(sys.argv[1]) if len(sys.argv) >= 2 else 14
    do_analyze = True
    if len(sys.argv) >= 3:
        arg = sys.argv[2].strip().lower()
        do_analyze = arg in ("true", "1", "yes", "y", "t")

    actualizar_base_ventas_extendida(window_days=wdays, analyze=do_analyze)
    logger.info("---------------> Flujo actualizar_base_ventas_extendida FINALIZADO.")
