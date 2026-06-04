# obtener_base_transferencias_pendientes.py

import os
import sys

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from etl_chunk_utils import (
    build_sql_server_engine,
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

TABLE_DESTINO = "src.base_transferencias_pendientes"
READ_CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE_TRANSFERENCIAS", "30000"))

logger = setup_script_logger(
    "obtener_base_transferencias_pendientes",
    "replicacion_base_transferencias_pendientes.log",
)

sql_engine = build_sql_server_engine(
    SQL_SERVER,
    SQL_USER,
    SQL_PASSWORD,
    SQL_DATABASE,
)


def open_pg_conn_local():
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)


ESQUEMA_TRANSFERENCIAS_PENDIENTES = {
    "c_sucu_orig": "INTEGER",
    "c_sucu_dest": "INTEGER",
    "c_articulo": "INTEGER",
    "q_pendiente": "DOUBLE PRECISION",
    "fuente_origen": "VARCHAR",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER",
}


QUERY = """
    SELECT
        [C_SUCU_ORIG],
        [C_SUCU_DEST],
        [C_ARTICULO],
        [q_pendiente],
        GETDATE() AS [FECHA_EXTRACCION]
    FROM [DIARCOP001].[DiarcoP].[dbo].[v_transferencias_pendientes]
    WHERE [q_pendiente] > 0;
"""


def transformar_chunk(df, chunk_index, task_logger):
    df["FUENTE_ORIGEN"] = "v_transferencias_pendientes"
    df["ESTADO_SINCRONIZACION"] = 0

    for column in ["C_SUCU_ORIG", "C_SUCU_DEST", "C_ARTICULO", "ESTADO_SINCRONIZACION"]:
        coerce_int_column(df, column, task_logger)

    coerce_float_column(df, "q_pendiente", task_logger)

    task_logger.info(
        "Chunk %s transformado para transferencias pendientes | filas=%s",
        chunk_index,
        len(df),
    )
    return df


@task(name="cargar_transferencias_pendientes_pg")
def cargar_transferencias_pendientes_pg():
    task_logger = get_run_logger()
    metrics = replace_table_from_query_chunks(
        query=QUERY,
        sql_engine=sql_engine,
        pg_conn_factory=open_pg_conn_local,
        table_name=TABLE_DESTINO,
        schema_dict=ESQUEMA_TRANSFERENCIAS_PENDIENTES,
        transform_chunk=transformar_chunk,
        logger=task_logger,
        read_chunk_size=READ_CHUNK_SIZE,
    )
    return metrics


@flow(name="capturar_transferencias_pendientes", persist_result=False)
def capturar_transferencias_pendientes():
    flow_logger = get_run_logger()
    try:
        load_result = cargar_transferencias_pendientes_pg.with_options(
            name="Carga Transferencias Pendientes",
        ).submit().result()

        flow_logger.info(
            "Flujo completado | tabla=%s | filas=%s | chunks=%s | duracion=%.2fs",
            TABLE_DESTINO,
            load_result["rows"],
            load_result["chunks"],
            load_result["seconds"],
        )
    except Exception as exc:
        flow_logger.error("Error general en el flujo de transferencias pendientes: %s", exc)
        raise


if __name__ == "__main__":
    _ = sys.argv[1:]
    capturar_transferencias_pendientes()
    logger.info("Proceso finalizado: obtener_base_transferencias_pendientes")
