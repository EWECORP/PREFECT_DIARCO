# obtener_oc_demoradas_proveedor.py

import os
import sys

from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from etl_chunk_utils import (
    build_sql_server_engine,
    coerce_datetime_column,
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

TABLE_DESTINO = "src.base_forecast_oc_demoradas"
READ_CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE_OC_DEMORADAS", "20000"))

logger = setup_script_logger(
    "obtener_oc_demoradas_proveedor",
    "replicacion_oc_demoradas_proveedor.log",
)

sql_engine = build_sql_server_engine(
    SQL_SERVER, # type: ignore
    SQL_USER, # type: ignore
    SQL_PASSWORD, # type: ignore
    SQL_DATABASE, # type: ignore
)


def open_pg_conn_local():
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD) # pyright: ignore[reportArgumentType]


ESQUEMA_OC_DEMORADAS = {
    "c_oc": "BIGINT",
    "u_prefijo_oc": "VARCHAR",
    "u_sufijo_oc": "VARCHAR",
    "u_dias_limite_entrega": "INTEGER",
    "fecha_limite": "TIMESTAMP",
    "demora": "INTEGER",
    "codigo_proveedor": "INTEGER",
    "codigo_sucursal": "INTEGER",
    "c_sucu_destino": "INTEGER",
    "c_sucu_destino_alt": "INTEGER",
    "c_situac": "INTEGER",
    "f_situac": "TIMESTAMP",
    "f_alta_sist": "TIMESTAMP",
    "f_emision": "TIMESTAMP",
    "f_entrega": "TIMESTAMP",
    "c_usuario_operador": "VARCHAR",
    "fuente_origen": "VARCHAR",
    "fecha_extraccion": "TIMESTAMP",
    "estado_sincronizacion": "INTEGER",
}


QUERY = """
    SELECT
        [C_OC],
        [U_PREFIJO_OC],
        [U_SUFIJO_OC],
        [U_DIAS_LIMITE_ENTREGA],
        [FECHA_LIMITE],
        DATEDIFF(DAY, [FECHA_LIMITE], GETDATE()) AS [Demora],
        [C_PROVEEDOR] AS [Codigo_Proveedor],
        [C_SUCU_COMPRA] AS [Codigo_Sucursal],
        [C_SUCU_DESTINO],
        [C_SUCU_DESTINO_ALT],
        [C_SITUAC],
        [F_SITUAC],
        [F_ALTA_SIST],
        [F_EMISION],
        [F_ENTREGA],
        [C_USUARIO_OPERADOR],
        GETDATE() AS [FECHA_EXTRACCION]
    FROM [repl].[T080_OC_CABE]
    WHERE [FECHA_LIMITE] < GETDATE()
      AND [C_SITUAC] = 1;
"""


def transformar_chunk(df, chunk_index, task_logger):
    df["FUENTE_ORIGEN"] = "T080_OC_CABE"
    df["ESTADO_SINCRONIZACION"] = 0

    int_columns = [
        "C_OC",
        "U_DIAS_LIMITE_ENTREGA",
        "Demora",
        "Codigo_Proveedor",
        "Codigo_Sucursal",
        "C_SUCU_DESTINO",
        "C_SUCU_DESTINO_ALT",
        "C_SITUAC",
        "ESTADO_SINCRONIZACION",
    ]
    for column in int_columns:
        coerce_int_column(df, column, task_logger)

    for column in [
        "FECHA_LIMITE",
        "F_SITUAC",
        "F_ALTA_SIST",
        "F_EMISION",
        "F_ENTREGA",
        "FECHA_EXTRACCION",
    ]:
        coerce_datetime_column(df, column, task_logger)

    for column in ["U_PREFIJO_OC", "U_SUFIJO_OC", "C_USUARIO_OPERADOR"]:
        coerce_string_column(df, column, task_logger, strip=True)

    task_logger.info(
        "Chunk %s transformado para OC demoradas | filas=%s",
        chunk_index,
        len(df),
    )
    return df


@task(name="cargar_oc_demoradas_proveedores_pg")
def cargar_oc_demoradas_proveedores_pg():
    task_logger = get_run_logger()
    metrics = replace_table_from_query_chunks(
        query=QUERY,
        sql_engine=sql_engine,
        pg_conn_factory=open_pg_conn_local,
        table_name=TABLE_DESTINO,
        schema_dict=ESQUEMA_OC_DEMORADAS,
        transform_chunk=transformar_chunk,
        logger=task_logger, # pyright: ignore[reportArgumentType]
        read_chunk_size=READ_CHUNK_SIZE,
    )
    return metrics


@flow(name="capturar_oc_demoradas_proveedores", persist_result=False)
def capturar_oc_demoradas_proveedores():
    flow_logger = get_run_logger()
    try:
        load_result = cargar_oc_demoradas_proveedores_pg.with_options(
            name="Carga OC Demoradas Proveedores",
        ).submit().result()

        flow_logger.info(
            "Flujo completado | tabla=%s | filas=%s | chunks=%s | duracion=%.2fs",
            TABLE_DESTINO,
            load_result["rows"],
            load_result["chunks"],
            load_result["seconds"],
        )
    except Exception as exc:
        flow_logger.error("Error general en el flujo de OC demoradas: %s", exc)
        raise


if __name__ == "__main__":
    _ = sys.argv[1:]
    capturar_oc_demoradas_proveedores()
    logger.info("Proceso finalizado: obtener_oc_demoradas_proveedor")
