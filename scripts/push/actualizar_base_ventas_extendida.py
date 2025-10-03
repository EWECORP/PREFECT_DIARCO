# actualizar_base_ventas_extendida.py
import os
import sys
import logging
from datetime import datetime
import psycopg2 as pg2
from psycopg2.extras import execute_values
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine
from dotenv import load_dotenv

# =========================
# Logging compartido (su estilo)
# =========================
logger = logging.getLogger("replicacion_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
os.makedirs("logs", exist_ok=True)
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
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# =========================
# Conexiones (mantiene su firma)
# =========================
def open_sql_conn():
    # Útil si en el futuro desean leer desde SQL Server en esta misma rutina
    print(f"Conectando a SQL Server: {SQL_SERVER}")
    print(f"Conectando a SQL Server: {SQL_DATABASE}")
    return create_engine(
        f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
    )

def open_pg_conn():
    return pg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

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

# =========================
# SQL blocks (paramétricos)
# =========================
SQL_TRUNCATE_STG = """
TRUNCATE TABLE src.base_ventas_extendida__stg;
"""

SQL_INSERT_STG = """
WITH max_dest AS (
    SELECT COALESCE(MAX(fecha), DATE '1900-01-01') AS max_fecha
    FROM src.base_ventas_extendida
),
params AS (
    SELECT (max_fecha - INTERVAL %s)::date AS fecha_desde
    FROM max_dest
)
INSERT INTO src.base_ventas_extendida__stg(
    fecha, codigo_articulo, sucursal, precio, unidades, importe_vendido,
    con_stock, venta_especial, promo_normal, promo_fuerte,
    precio_prefijado, factor_precio,
    costo, familia, rubro, subrubro, c_proveedor_primario,
    nombre_articulo, clasificacion, fecha_procesado, marca_procesado
)
SELECT
    V.f_venta::date                                       AS fecha,
    V.c_articulo::bigint                                  AS codigo_articulo,
    V.c_sucu_empr::int                                    AS sucursal,
    V.i_precio_venta::numeric(18,6)                       AS precio,
    V.q_unidades_vendidas::numeric(18,6)                  AS unidades,
    V.i_vendido::numeric(18,6)                            AS importe_vendido,
    TRUE  AS con_stock,
    FALSE AS venta_especial,
    FALSE AS promo_normal,
    FALSE AS promo_fuerte,
    NULL  AS precio_prefijado,
    NULL  AS factor_precio,
    V.i_precio_costo::numeric(18,6)                       AS costo,
    V.c_familia::int                                     AS familia,
    A.c_rubro::int                                       AS rubro,
    A.c_subrubro_1::int                                  AS subrubro,
    A.c_proveedor_primario::int                           AS c_proveedor_primario,
    TRIM(BOTH FROM REPLACE(REPLACE(REPLACE(A.n_articulo, CHR(9), ''), CHR(13), ''), CHR(10), ''))
                                                            AS nombre_articulo,
    A.c_clasificacion_compra::int                         AS clasificacion,
    CURRENT_TIMESTAMP                                     AS fecha_procesado,
    0                                                     AS marca_procesado
FROM src.t702_est_vtas_por_articulo V
LEFT JOIN src.t050_articulos A
        ON V.c_articulo = A.c_articulo
CROSS JOIN params p
WHERE V.f_venta::date >= p.fecha_desde
    AND A.m_baja = 'N';
"""

SQL_UPDATE_OFERTAS = """
WITH oferta AS (
    SELECT
        ofe.c_articulo,
        ofe.c_sucu_empr,
        make_date(ofe.c_anio::int, ofe.c_mes::int, u.d::int) AS fecha,
        NULLIF(BTRIM(UPPER(u.val::text)), '') AS flag
    FROM src.t710_estadis_oferta_folder AS ofe
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
)
UPDATE src.base_ventas_extendida__stg b
SET promo_normal = (of.flag = 'S'),
    promo_fuerte = (of.flag = 'F')
FROM oferta of
WHERE b.codigo_articulo = of.c_articulo
    AND b.sucursal        = of.c_sucu_empr
    AND b.fecha           = of.fecha;
"""

SQL_UPDATE_PRECIOS_PREFIJADOS = """
WITH precios AS (
    SELECT
        ep.c_articulo,
        ep.c_sucu_empr,
        make_date(ep.c_anio::int, ep.c_mes::int, u.d::int) AS fecha,
        u.val::numeric AS precio_prefijado
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
    WHERE u.d <= EXTRACT(DAY FROM (date_trunc('month', make_date(ep.c_anio::int, ep.c_mes::int, 1))
                                    + INTERVAL '1 month - 1 day'))
)
UPDATE src.base_ventas_extendida__stg b
SET precio_prefijado = p.precio_prefijado
FROM precios p
WHERE b.codigo_articulo = p.c_articulo
    AND b.sucursal        = p.c_sucu_empr
    AND b.fecha           = p.fecha;
"""

SQL_UPDATE_FACTOR_PRECIO = """
UPDATE src.base_ventas_extendida__stg
SET factor_precio =
    CASE
        WHEN precio_prefijado IS NULL OR precio_prefijado = 0 THEN 1
        ELSE ROUND(precio / precio_prefijado, 6)
    END;
"""

SQL_DEDUP_STG = """
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (
        PARTITION BY fecha, codigo_articulo, sucursal, precio
        ORDER BY fecha_procesado
    ) AS rn
    FROM src.base_ventas_extendida__stg
)
DELETE FROM src.base_ventas_extendida__stg s
USING ranked r
WHERE s.fecha = r.fecha
  AND s.codigo_articulo = r.codigo_articulo
  AND s.sucursal = r.sucursal
  AND s.precio = r.precio
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

SQL_ANALYZE = """
ANALYZE src.base_ventas_extendida__stg;
"""

SQL_COUNT_STG = "SELECT COUNT(*) FROM src.base_ventas_extendida__stg;"
SQL_COUNT_UPSERTED_HINT = """
-- Opcional: contar por rango temporal reciente en destino, si se quiere medir el impacto.
"""

# =========================
# Prefect tasks
# =========================
@task(name="Truncar_Staging")
def t_truncate_stg():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_TRUNCATE_STG, log_prefix="Truncar STG")
        conn.commit()
    logger.info("STG truncada.")

@task(name="Insertar_STG_desde_fuentes")
def t_insert_stg(window_days: int = 14):
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            # Pasamos '14 days' como intervalo parametrizado
            intervalo = f"{window_days} days"
            exec_sql(cur, SQL_INSERT_STG, params=(intervalo,), log_prefix=f"Insert STG (ventana={intervalo})")
        conn.commit()
    logger.info("Carga STG completada.")

@task(name="Enriquecer_Ofertas")
def t_update_ofertas():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_UPDATE_OFERTAS, log_prefix="Actualizar OFERTAS")
        conn.commit()
    logger.info("STG enriquecida con OFERTAS.")

@task(name="Enriquecer_Precios_Prefijados")
def t_update_precios_prefijados():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_UPDATE_PRECIOS_PREFIJADOS, log_prefix="Actualizar PRECIOS PREFIJADOS")
        conn.commit()
    logger.info("STG enriquecida con PRECIOS PREFIJADOS.")

@task(name="Calcular_Factor_Precio")
def t_update_factor_precio():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_UPDATE_FACTOR_PRECIO, log_prefix="Calcular FACTOR_PRECIO")
        conn.commit()
    logger.info("STG actualizada con FACTOR_PRECIO.")

@task(name="Deduplicar_STG")
def t_dedup_stg():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_DEDUP_STG, log_prefix="De-dup STG")
        conn.commit()
    logger.info("De-duplicación en STG finalizada.")

@task(name="Upsert_Destino")
def t_upsert_destino():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_UPSERT_DEST, log_prefix="Upsert DESTINO")
        conn.commit()
    logger.info("Upsert a DESTINO completado.")

@task(name="Analyze_Post_Carga")
def t_analyze_post():
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            exec_sql(cur, SQL_ANALYZE, log_prefix="ANALYZE post-carga")
        conn.commit()
    logger.info("ANALYZE ejecutado en STG.")

@task(name="DQ_Chequeos_Basicos")
def t_dq_basicos():
    # Ejemplo mínimo: contar filas en STG tras enriquecimientos
    with open_pg_conn() as conn:
        with conn.cursor() as cur:
            cnt_stg = fetch_one(cur, SQL_COUNT_STG)[0] # type: ignore
    logger.info(f"DQ: filas en STG tras enriquecimiento = {cnt_stg:,}")
    return {"stg_rows": cnt_stg}

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
    """
    log = get_run_logger()
    log.info(f"[INICIO] actualizar_base_ventas_extendida | ventana={window_days} días")

    # 1
    t_truncate_stg.submit().result()

    # 2
    t_insert_stg.with_options(name="Insertar STG").submit(window_days).result()

    # 3
    t_update_ofertas.submit().result()
    t_update_precios_prefijados.submit().result()

    # 4
    t_update_factor_precio.submit().result()

    # 5
    t_dedup_stg.submit().result()

    # 6
    t_upsert_destino.submit().result()

    # 7
    dq = t_dq_basicos.submit().result()
    if analyze:
        t_analyze_post.submit().result()

    log.info(f"[FIN] actualizar_base_ventas_extendida | DQ={dq}")

# =========================
# CLI
# =========================
if __name__ == "__main__":
    # Uso:
    #   python actualizar_base_ventas_extendida.py            -> ventana default 14
    #   python actualizar_base_ventas_extendida.py 7 false    -> 7 días, sin ANALYZE
    wdays = int(sys.argv[1]) if len(sys.argv) >= 2 else 14
    do_analyze = True
    if len(sys.argv) >= 3:
        arg = sys.argv[2].strip().lower()
        do_analyze = (arg in ("true", "1", "yes", "y", "t"))
    actualizar_base_ventas_extendida(window_days=wdays, analyze=do_analyze)
    logger.info("--------------->  Flujo actualizar_base_ventas_extendida FINALIZADO.")
