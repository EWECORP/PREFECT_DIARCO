# obtener_base_productos_transito.py

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

SP_NAME = "[dbo].[SP_BASE_PRODUCTOS_EN_TRANSITO]"
TABLE_DESTINO = "src.base_productos_en_transito"
READ_CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE_PRODUCTOS_TRANSITO", "25000"))

logger = setup_script_logger(
    "obtener_base_productos_transito",
    "replicacion_base_productos_transito.log",
)

sql_engine = build_sql_server_engine(
    SQL_SERVER,
    SQL_USER,
    SQL_PASSWORD,
    SQL_DATABASE,
)


def open_pg_conn_local():
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)


ESQUEMA_BASE_PRODUCTOS_TRANSITO = {
    "f_alta_sist": "TIMESTAMP",
    "c_sucu_orig": "INTEGER",
    "c_sucu_dest": "INTEGER",
    "c_articulo": "INTEGER",
    "n_articulo": "VARCHAR(60)",
    "q_unid_peso_transf": "DOUBLE PRECISION",
    "q_unid_peso_recep": "DOUBLE PRECISION",
    "q_unid_transito": "DOUBLE PRECISION",
    "vnsucursalorig": "VARCHAR(50)",
    "vnsucursaldest": "VARCHAR(50)",
    "vpreciovtadestd": "DOUBLE PRECISION",
    "k_coef_iva": "NUMERIC(4,3)",
    "q_factor_vta_sucu": "INTEGER",
    "i_costo_estadistico": "DOUBLE PRECISION",
    "fuente_origen": "VARCHAR(100)",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER",
}


def transformar_chunk(df, chunk_index, task_logger):
    df.columns = [str(column).lower() for column in df.columns]
    df["fuente_origen"] = "SP_BASE_PRODUCTOS_EN_TRANSITO"
    df["fecha_extraccion"] = datetime.now()
    df["estado_sincronizacion"] = 0

    for column in ["f_alta_sist", "fecha_extraccion"]:
        coerce_datetime_column(df, column, task_logger)

    for column in ["c_sucu_orig", "c_sucu_dest", "c_articulo", "q_factor_vta_sucu", "estado_sincronizacion"]:
        coerce_int_column(df, column, task_logger)

    for column in [
        "q_unid_peso_transf",
        "q_unid_peso_recep",
        "q_unid_transito",
        "vpreciovtadestd",
        "k_coef_iva",
        "i_costo_estadistico",
    ]:
        coerce_float_column(df, column, task_logger)

    for column in ["n_articulo", "vnsucursalorig", "vnsucursaldest", "fuente_origen"]:
        coerce_string_column(df, column, task_logger, strip=True)

    task_logger.info(
        "Chunk %s transformado para productos en transito | filas=%s",
        chunk_index,
        len(df),
    )
    return df


@task(name="cargar_base_productos_transito_pg")
def cargar_base_productos_transito_pg():
    task_logger = get_run_logger()
    metrics = replace_table_from_query_chunks(
        query=f"EXEC {SP_NAME}",
        sql_engine=sql_engine,
        pg_conn_factory=open_pg_conn_local,
        table_name=TABLE_DESTINO,
        schema_dict=ESQUEMA_BASE_PRODUCTOS_TRANSITO,
        transform_chunk=transformar_chunk,
        logger=task_logger,
        read_chunk_size=READ_CHUNK_SIZE,
    )
    return metrics


@flow(name="obtener_base_productos_transito", persist_result=False)
def capturar_base_productos_transito(lista_ids: Optional[list] = None):
    del lista_ids
    flow_logger = get_run_logger()
    try:
        load_result = cargar_base_productos_transito_pg.with_options(
            name="Carga Base Productos en Transito",
        ).submit().result()

        flow_logger.info(
            "Flujo completado | tabla=%s | filas=%s | chunks=%s | duracion=%.2fs",
            TABLE_DESTINO,
            load_result["rows"],
            load_result["chunks"],
            load_result["seconds"],
        )
    except Exception as exc:
        flow_logger.error("Error general en el flujo de productos en transito: %s", exc)
        raise


if __name__ == "__main__":
    args = sys.argv[1:]
    lista_ids = ast.literal_eval(args[0]) if args else None
    capturar_base_productos_transito(lista_ids=lista_ids)
    logger.info("Proceso finalizado: obtener_base_productos_transito")
