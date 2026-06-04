# obtener_base_stock.py

import ast
import os
import sys
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from etl_chunk_utils import (
    build_sql_server_engine,
    coerce_datetime_column,
    coerce_float_column,
    coerce_int_column,
    coerce_string_column,
    open_pg_conn,
    replace_table_from_query_chunks,
    setup_script_logger,
)


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

SP_NAME = "[dbo].[SP_BASE_STOCK_EXTEND]"
TABLE_DESTINO = "src.base_stock_sucursal"
READ_CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE_BASE_STOCK", "20000"))

logger = setup_script_logger(
    "obtener_base_stock",
    "replicacion_base_stock.log",
)

sql_engine = build_sql_server_engine(
    SQL_SERVER,
    SQL_USER,
    SQL_PASSWORD,
    SQL_DATABASE,
)


def open_pg_conn_local():
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)


ESQUEMA_BASE_STOCK = {
    "codigo_articulo": "INTEGER",
    "codigo_sucursal": "INTEGER",
    "codigo_proveedor": "INTEGER",
    "precio_venta": "DOUBLE PRECISION",
    "precio_costo": "DOUBLE PRECISION",
    "factor_venta": "INTEGER",
    "ultimo_ingreso": "DOUBLE PRECISION",
    "fecha_ultimo_ingreso": "TIMESTAMP",
    "fecha_ultima_venta": "TIMESTAMP",
    "m_vende_por_peso": "VARCHAR",
    "venta_unidades_1q": "DOUBLE PRECISION",
    "venta_unidades_2q": "DOUBLE PRECISION",
    "venta_mes_unidades": "DOUBLE PRECISION",
    "venta_mes_valorizada": "DOUBLE PRECISION",
    "dias_stock": "DOUBLE PRECISION",
    "fecha_stock": "TIMESTAMP",
    "stock": "DOUBLE PRECISION",
    "transfer_pendiente": "DOUBLE PRECISION",
    "pedido_pendiente": "DOUBLE PRECISION",
    "transito_pendiente": "DOUBLE PRECISION",
    "transfer_pendiente_fecha": "TIMESTAMP",
    "pedido_pendiente_fecha": "TIMESTAMP",
    "transito_pendiente_fecha": "TIMESTAMP",
    "promocion": "INTEGER",
    "lote": "VARCHAR",
    "validez_lote": "TIMESTAMP",
    "stock_reserva": "DOUBLE PRECISION",
    "validez_promocion": "INTEGER",
    "q_dias_stock": "INTEGER",
    "q_dias_sobre_stock": "INTEGER",
    "i_lista_calculado": "DOUBLE PRECISION",
    "pedido_sgm": "DOUBLE PRECISION",
    "importe_minimo": "DOUBLE PRECISION",
    "bultos_minimo": "DOUBLE PRECISION",
    "dias_preparacion": "INTEGER",
    "fuente_origen": "VARCHAR",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER",
}


def transformar_chunk(df, chunk_index, task_logger):
    df["FUENTE_ORIGEN"] = "SP_BASE_STOCK_DMZ"
    df["FECHA_EXTRACCION"] = datetime.now()
    df["ESTADO_SINCRONIZACION"] = 0

    int_columns = [
        "Codigo_Articulo",
        "Codigo_Sucursal",
        "Codigo_Proveedor",
        "Factor_Venta",
        "Promocion",
        "Validez_Promocion",
        "Q_DIAS_STOCK",
        "Q_DIAS_SOBRE_STOCK",
        "Dias_Preparacion",
        "ESTADO_SINCRONIZACION",
    ]
    for column in int_columns:
        coerce_int_column(df, column, task_logger)

    float_columns = [
        "Precio_Venta",
        "Precio_Costo",
        "Ultimo_Ingreso",
        "Venta_Unidades_1Q",
        "Venta_Unidades_2Q",
        "Venta_Mes_Unidades",
        "Venta_Mes_Valorizada",
        "Dias_Stock",
        "Stock",
        "Transfer_Pendiente",
        "Pedido_Pendiente",
        "Transito_Pendiente",
        "Stock_Reserva",
        "I_LISTA_CALCULADO",
        "Pedido_SGM",
        "Importe_Minimo",
        "Bultos_Minimo",
    ]
    for column in float_columns:
        coerce_float_column(df, column, task_logger)

    datetime_columns = [
        "Fecha_Ultimo_Ingreso",
        "Fecha_Ultima_Venta",
        "Fecha_Stock",
        "Transfer_Pendiente_fecha",
        "Pedido_Pendiente_fecha",
        "Transito_Pendiente_fecha",
        "Validez_Lote",
        "FECHA_EXTRACCION",
    ]
    for column in datetime_columns:
        coerce_datetime_column(df, column, task_logger)

    string_columns = [
        ("M_Vende_Por_Peso", False),
        ("Lote", True),
    ]
    for column, strip in string_columns:
        coerce_string_column(df, column, task_logger, strip=strip)

    task_logger.info(
        "Chunk %s transformado para base_stock_sucursal | filas=%s",
        chunk_index,
        len(df),
    )
    return df


@task(name="cargar_base_stock_sucursal_pg")
def cargar_base_stock_sucursal_pg():
    task_logger = get_run_logger()
    metrics = replace_table_from_query_chunks(
        query=f"EXEC {SP_NAME}",
        sql_engine=sql_engine,
        pg_conn_factory=open_pg_conn_local,
        table_name=TABLE_DESTINO,
        schema_dict=ESQUEMA_BASE_STOCK,
        transform_chunk=transformar_chunk,
        logger=task_logger,
        read_chunk_size=READ_CHUNK_SIZE,
    )
    return metrics


@task(name="asegurar_indices_base_stock")
def asegurar_indices_base_stock():
    task_logger = get_run_logger()
    ddl = """
        CREATE INDEX IF NOT EXISTS idx_bss_sucu_art
            ON src.base_stock_sucursal (codigo_sucursal, codigo_articulo);
        CREATE INDEX IF NOT EXISTS idx_bss_art
            ON src.base_stock_sucursal (codigo_articulo);
        CREATE INDEX IF NOT EXISTS idx_bpv_sucu_art
            ON src.base_productos_vigentes (c_sucu_empr, c_articulo);
        CREATE INDEX IF NOT EXISTS idx_bpv_codcd_art
            ON src.base_productos_vigentes (cod_cd, c_articulo);
    """
    try:
        with open_pg_conn_local() as conn, conn.cursor() as cur:
            cur.execute(ddl)
            conn.commit()
        task_logger.info("Indices verificados y listos para los ajustes de stock.")
    except Exception as exc:
        task_logger.error("Error creando indices de apoyo: %s", exc)
        raise


AJUSTE_SQL = """
WITH despachos_cd AS (
    SELECT
        p.cod_cd,
        s.codigo_articulo,
        SUM(s.transfer_pendiente) * -1 AS despachos
    FROM src.base_stock_sucursal AS s
    JOIN src.base_productos_vigentes AS p
      ON s.codigo_sucursal = p.c_sucu_empr
     AND s.codigo_articulo = p.c_articulo
    WHERE p.cod_cd = %s
    GROUP BY p.cod_cd, s.codigo_articulo
    HAVING SUM(s.transfer_pendiente) > 0
)
UPDATE src.base_stock_sucursal AS s
SET transfer_pendiente = s.transfer_pendiente + d.despachos
FROM despachos_cd AS d
WHERE s.codigo_articulo = d.codigo_articulo
  AND s.codigo_sucursal = %s;
"""


@task(name="ajustar_transferencias_cd")
def ajustar_transferencias_cd(mapeo_cd=(("41CD", 41), ("82CD", 82))):
    task_logger = get_run_logger()
    total_afectadas = 0
    try:
        with open_pg_conn_local() as conn, conn.cursor() as cur:
            for cod_cd, sucursal_cd in mapeo_cd:
                cur.execute(AJUSTE_SQL, (cod_cd, sucursal_cd))
                afectadas = cur.rowcount if cur.rowcount is not None else 0
                total_afectadas += afectadas
                task_logger.info(
                    "Ajuste de transferencias aplicado | cod_cd=%s | sucursal=%s | filas=%s",
                    cod_cd,
                    sucursal_cd,
                    afectadas,
                )
            conn.commit()
        task_logger.info(
            "Ajustes por CD completados | filas_actualizadas=%s",
            total_afectadas,
        )
        return total_afectadas
    except Exception as exc:
        task_logger.error("Error ajustando transferencias por CD: %s", exc)
        raise


@flow(name="obtener_base_stock_sucursal", persist_result=False)
def capturar_base_stock(lista_ids: Optional[list] = None):
    del lista_ids
    flow_logger = get_run_logger()
    try:
        load_result = cargar_base_stock_sucursal_pg.with_options(
            name="Carga Base Stock Sucursal",
        ).submit().result()

        asegurar_indices_base_stock.with_options(
            name="Asegurar Indices Base Stock",
        ).submit().result()

        adjusted_rows = ajustar_transferencias_cd.with_options(
            name="Ajustar Transferencias Pendientes CD",
        ).submit().result()

        flow_logger.info(
            "Flujo completado | tabla=%s | filas=%s | chunks=%s | filas_ajustadas=%s | duracion_carga=%.2fs",
            TABLE_DESTINO,
            load_result["rows"],
            load_result["chunks"],
            adjusted_rows,
            load_result["seconds"],
        )
    except Exception as exc:
        flow_logger.error("Error general en el flujo de base_stock: %s", exc)
        raise


if __name__ == "__main__":
    args = sys.argv[1:]
    lista_ids = ast.literal_eval(args[0]) if args else None
    capturar_base_stock(lista_ids=lista_ids)
    logger.info("Proceso finalizado: obtener_base_stock")
