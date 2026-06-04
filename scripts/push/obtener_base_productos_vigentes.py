# obtener_base_productos_vigentes.py

import ast
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from etl_chunk_utils import (
    build_sql_server_engine,
    coerce_datetime_column,
    coerce_float_column,
    coerce_int_column,
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

SP_NAME = "[dbo].[SP_BASE_PRODUCTOS_DMZ]"
TABLE_DESTINO = "src.base_productos_vigentes"
READ_CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE_BASE_PRODUCTOS", "25000"))

logger = setup_script_logger(
    "obtener_base_productos_vigentes",
    "replicacion_base_productos_vigentes.log",
)

sql_engine = build_sql_server_engine(
    SQL_SERVER,
    SQL_USER,
    SQL_PASSWORD,
    SQL_DATABASE,
)


def open_pg_conn_local():
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)


ESQUEMA_BASE_PRODUCTOS = {
    "c_sucu_empr": "INTEGER",
    "c_articulo": "INTEGER",
    "c_proveedor_primario": "INTEGER",
    "abastecimiento": "INTEGER",
    "cod_cd": "VARCHAR",
    "habilitado": "INTEGER",
    "fecha_registro": "TIMESTAMP",
    "fecha_baja": "TIMESTAMP",
    "q_peso_unit_art": "DOUBLE PRECISION",
    "m_vende_por_peso": "INTEGER",
    "unid_transferencia": "INTEGER",
    "q_unid_transferencia": "INTEGER",
    "pedido_min": "DOUBLE PRECISION",
    "frente_lineal": "INTEGER",
    "capacid_gondola": "INTEGER",
    "stock_minimo": "DOUBLE PRECISION",
    "cod_comprador": "INTEGER",
    "promocion": "INTEGER",
    "active_for_purchase": "INTEGER",
    "active_for_sale": "INTEGER",
    "active_on_mix": "INTEGER",
    "delivered_id": "VARCHAR",
    "product_base_id": "VARCHAR",
    "own_production": "INTEGER",
    "q_factor_compra": "INTEGER",
    "full_capacity_pallet": "INTEGER",
    "number_of_layers": "INTEGER",
    "number_of_boxes_per_layer": "DOUBLE PRECISION",
    "fuente_origen": "VARCHAR",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER",
}


def transformar_chunk(df, chunk_index, task_logger):
    df["FUENTE_ORIGEN"] = "SP_BASE_PRODUCTOS_SUCURSAL"
    df["FECHA_EXTRACCION"] = datetime.now()
    df["ESTADO_SINCRONIZACION"] = 0

    int_columns = [
        "C_SUCU_EMPR",
        "C_ARTICULO",
        "C_PROVEEDOR_PRIMARIO",
        "ABASTECIMIENTO",
        "HABILITADO",
        "M_VENDE_POR_PESO",
        "UNID_TRANSFERENCIA",
        "Q_UNID_TRANSFERENCIA",
        "FRENTE_LINEAL",
        "CAPACID_GONDOLA",
        "COD_COMPRADOR",
        "PROMOCION",
        "ACTIVE_FOR_PURCHASE",
        "ACTIVE_FOR_SALE",
        "ACTIVE_ON_MIX",
        "OWN_PRODUCTION",
        "Q_FACTOR_COMPRA",
        "FULL_CAPACITY_PALLET",
        "NUMBER_OF_LAYERS",
        "ESTADO_SINCRONIZACION",
    ]
    for column in int_columns:
        coerce_int_column(df, column, task_logger)

    float_columns = [
        "Q_PESO_UNIT_ART",
        "PEDIDO_MIN",
        "STOCK_MINIMO",
        "NUMBER_OF_BOXES_PER_LAYER",
    ]
    for column in float_columns:
        coerce_float_column(df, column, task_logger)

    datetime_columns = [
        "FECHA_REGISTRO",
        "FECHA_BAJA",
        "FECHA_EXTRACCION",
    ]
    for column in datetime_columns:
        coerce_datetime_column(df, column, task_logger)

    task_logger.info(
        "Chunk %s transformado para base_productos_vigentes | filas=%s",
        chunk_index,
        len(df),
    )
    return df


@task(name="cargar_base_productos_pg")
def cargar_base_productos():
    task_logger = get_run_logger()
    metrics = replace_table_from_query_chunks(
        query=f"EXEC {SP_NAME}",
        sql_engine=sql_engine,
        pg_conn_factory=open_pg_conn_local,
        table_name=TABLE_DESTINO,
        schema_dict=ESQUEMA_BASE_PRODUCTOS,
        transform_chunk=transformar_chunk,
        logger=task_logger,
        read_chunk_size=READ_CHUNK_SIZE,
    )
    return metrics


@task(name="eliminar_duplicados_base_productos_vigentes")
def eliminar_duplicados():
    task_logger = get_run_logger()
    query_delete_returning = """
        WITH cte AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY c_sucu_empr, c_articulo, c_proveedor_primario
                       ORDER BY fecha_extraccion DESC NULLS LAST
                   ) AS rn
            FROM src.base_productos_vigentes
        )
        DELETE FROM src.base_productos_vigentes b
        USING cte
        WHERE b.ctid = cte.ctid
          AND cte.rn > 1
        RETURNING 1;
    """

    try:
        with open_pg_conn_local() as conn:
            with conn.cursor() as cur:
                cur.execute(query_delete_returning)
                eliminados = cur.rowcount
            conn.commit()
        task_logger.info(
            "Depuración completada en base_productos_vigentes | duplicados_eliminados=%s",
            eliminados,
        )
        return eliminados
    except Exception as exc:
        task_logger.error("Error al eliminar duplicados: %s", exc)
        raise


@flow(name="obtener_base_productos_vigentes", persist_result=False)
def capturar_base_articulos():
    flow_logger = get_run_logger()
    try:
        load_result = cargar_base_productos.with_options(
            name="Carga Base Productos Vigentes",
        ).submit().result()

        deleted_rows = eliminar_duplicados.with_options(
            name="Eliminar Duplicados Base Productos Vigentes",
        ).submit().result()

        flow_logger.info(
            "Flujo completado | tabla=%s | filas=%s | chunks=%s | duplicados_eliminados=%s | duracion_carga=%.2fs",
            TABLE_DESTINO,
            load_result["rows"],
            load_result["chunks"],
            deleted_rows,
            load_result["seconds"],
        )
    except Exception as exc:
        flow_logger.error("Error general en el flujo de base_productos_vigentes: %s", exc)
        raise


if __name__ == "__main__":
    args = sys.argv[1:]
    _ = ast.literal_eval(args[0]) if args else None
    capturar_base_articulos()
    logger.info("Proceso finalizado: obtener_base_productos_vigentes")
