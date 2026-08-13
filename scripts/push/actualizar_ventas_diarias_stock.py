import os
from time import perf_counter

from dotenv import load_dotenv

from etl_chunk_utils import open_pg_conn, setup_script_logger


load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

logger = setup_script_logger(
    "actualizar_ventas_diarias_stock",
    "actualizar_ventas_diarias_stock.log",
)


UPSERT_VENTAS_DIARIAS_SQL = """
INSERT INTO datamart.dm_ventas_diarias (
    fecha,
    codigo_articulo,
    sucursal,
    unidades_total,
    importe_total
)
SELECT
    fecha::date,
    codigo_articulo,
    sucursal,
    SUM(unidades),
    SUM(importe_vendido)
FROM src.base_ventas_extendida
WHERE fecha >= CURRENT_DATE - 1
  AND fecha < CURRENT_DATE
GROUP BY fecha::date, codigo_articulo, sucursal
ON CONFLICT (fecha, codigo_articulo, sucursal)
DO UPDATE SET
    unidades_total = EXCLUDED.unidades_total,
    importe_total = EXCLUDED.importe_total;
"""


UPDATE_STOCK_SQL = """
WITH stock_keys AS (
    SELECT DISTINCT codigo_articulo, codigo_sucursal
    FROM src.base_stock_sucursal
),
agg AS (
    SELECT
        stock.codigo_articulo,
        stock.codigo_sucursal,
        COALESCE(
            SUM(v.unidades_total) FILTER (
                WHERE v.fecha >= CURRENT_DATE - 15
                  AND v.fecha <= CURRENT_DATE - 1
            ),
            0
        ) AS venta_unidades_1q,
        COALESCE(
            SUM(v.unidades_total) FILTER (
                WHERE v.fecha >= CURRENT_DATE - 30
                  AND v.fecha < CURRENT_DATE - 15
            ),
            0
        ) AS venta_unidades_2q,
        COALESCE(SUM(v.unidades_total), 0) AS venta_mes_unidades
    FROM stock_keys AS stock
    LEFT JOIN datamart.dm_ventas_diarias AS v
      ON v.codigo_articulo = stock.codigo_articulo
     AND v.sucursal = stock.codigo_sucursal
     AND v.fecha >= CURRENT_DATE - 30
     AND v.fecha <= CURRENT_DATE - 1
    GROUP BY stock.codigo_articulo, stock.codigo_sucursal
)
UPDATE src.base_stock_sucursal AS dst
SET
    venta_unidades_1q = agg.venta_unidades_1q,
    venta_unidades_2q = agg.venta_unidades_2q,
    venta_mes_unidades = agg.venta_mes_unidades,
    venta_mes_valorizada = agg.venta_mes_unidades * dst.precio_costo
FROM agg
WHERE dst.codigo_articulo = agg.codigo_articulo
  AND dst.codigo_sucursal = agg.codigo_sucursal;
"""


def open_pg_conn_local():
    config = {
        "PG_HOST": PG_HOST,
        "PG_PORT": PG_PORT,
        "PG_DB": PG_DB,
        "PG_USER": PG_USER,
        "PG_PASSWORD": PG_PASSWORD,
    }
    faltantes = [nombre for nombre, valor in config.items() if valor is None]
    if faltantes:
        raise RuntimeError(
            "Faltan variables de conexión PostgreSQL: " + ", ".join(faltantes)
        )
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)


def actualizar_ventas_diarias_stock():
    started_at = perf_counter()
    logger.info("Inicio actualización de ventas diarias y acumulados de stock")

    try:
        with open_pg_conn_local() as conn, conn.cursor() as cur:
            cur.execute("SELECT CURRENT_DATE - 1")
            fecha_procesada = cur.fetchone()[0]

            cur.execute(UPSERT_VENTAS_DIARIAS_SQL)
            ventas_afectadas = cur.rowcount
            if ventas_afectadas == 0:
                logger.warning(
                    "No se encontraron ventas para consolidar | fecha=%s",
                    fecha_procesada,
                )
            else:
                logger.info(
                    "Ventas del último día consolidadas | fecha=%s | filas=%s",
                    fecha_procesada,
                    ventas_afectadas,
                )

            cur.execute(UPDATE_STOCK_SQL)
            stock_afectado = cur.rowcount
            conn.commit()

        elapsed = perf_counter() - started_at
        logger.info(
            "Actualización finalizada OK | ventas_afectadas=%s | "
            "stock_actualizado=%s | duracion=%.2fs",
            ventas_afectadas,
            stock_afectado,
            elapsed,
        )
        return {
            "ventas_afectadas": ventas_afectadas,
            "stock_actualizado": stock_afectado,
            "seconds": elapsed,
        }
    except Exception:
        logger.exception("Error actualizando ventas diarias y acumulados de stock")
        raise


if __name__ == "__main__":
    actualizar_ventas_diarias_stock()
