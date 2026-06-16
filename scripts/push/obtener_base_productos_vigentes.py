# obtener_base_productos_vigentes.py

import ast
import os
import re
import sys
from datetime import datetime
from time import perf_counter

import pandas as pd
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

from etl_chunk_utils import (
    align_dataframe_to_schema,
    build_sql_server_engine,
    coerce_datetime_column,
    coerce_float_column,
    coerce_int_column,
    copy_dataframe_to_postgres,
    create_table_statement,
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
DEFAULT_TABLE_DESTINO = os.getenv(
    "BASE_PRODUCTOS_TARGET_TABLE",
    "src.base_productos_vigentes",
).strip()
READ_CHUNK_SIZE = int(os.getenv("ETL_CHUNK_SIZE_BASE_PRODUCTOS", "25000"))
DEFAULT_BASE_PRODUCTOS_SOURCE_MODE = os.getenv(
    "BASE_PRODUCTOS_SOURCE_MODE",
    "sqlserver_sp",
).strip().lower()
DEFAULT_PG_STOCK_TABLE = os.getenv(
    "BASE_PRODUCTOS_PG_STOCK_TABLE",
    "src.t060_stock",
).strip()
DEFAULT_PG_HIST_VIGENCIA_TABLE = os.getenv(
    "BASE_PRODUCTOS_PG_HIST_VIGENCIA_TABLE",
    "src.t804_hist_marca_listo_para_venta",
).strip()
DEFAULT_PG_MARCA_BARRIO_TABLE = os.getenv(
    "BASE_PRODUCTOS_PG_MARCA_BARRIO_TABLE",
    "src.t051_articulos_sucursal_barrio",
).strip()
DEFAULT_PG_SUC_EXCLUIDAS_TABLE = os.getenv(
    "BASE_PRODUCTOS_PG_SUC_EXCLUIDAS_TABLE",
    "src.sucursales_excluidas",
).strip()
ENABLE_EXPERIMENTAL_PG_SRC = os.getenv(
    "BASE_PRODUCTOS_ENABLE_EXPERIMENTAL_PG_SRC",
    "0",
).strip() == "1"

logger = setup_script_logger(
    "obtener_base_productos_vigentes",
    "replicacion_base_productos_vigentes.log",
)

_sql_engine = None


def get_sql_engine():
    global _sql_engine
    if _sql_engine is None:
        _sql_engine = build_sql_server_engine(
            SQL_SERVER, # pyright: ignore[reportArgumentType]
            SQL_USER, # pyright: ignore[reportArgumentType]
            SQL_PASSWORD, # pyright: ignore[reportArgumentType]
            SQL_DATABASE, # pyright: ignore[reportArgumentType]
        )
    return _sql_engine


def open_pg_conn_local():
    return open_pg_conn(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD) # pyright: ignore[reportArgumentType]


def assert_sql_table_name(table_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", table_name):
        raise ValueError(
            "Nombre de tabla invalido para target_table / BASE_PRODUCTOS_TARGET_TABLE. "
            "Usar formato schema.tabla o tabla."
        )
    return table_name


def resolve_runtime_options(payload):
    source_mode = DEFAULT_BASE_PRODUCTOS_SOURCE_MODE
    table_destino = DEFAULT_TABLE_DESTINO

    if isinstance(payload, dict):
        if payload.get("mode") is not None:
            source_mode = str(payload["mode"]).strip().lower()
        if payload.get("target_table") is not None:
            table_destino = str(payload["target_table"]).strip()
    elif isinstance(payload, str):
        source_mode = payload.strip().lower()
    elif payload is not None:
        raise ValueError(
            "Argumento no soportado. Usar string con el modo o dict con "
            "{'mode': '...', 'target_table': 'schema.tabla'}."
        )

    return source_mode, assert_sql_table_name(table_destino)


def get_pg_source_tables() -> dict[str, str]:
    return {
        "stock": assert_sql_table_name(DEFAULT_PG_STOCK_TABLE),
        "hist_vigencia": assert_sql_table_name(DEFAULT_PG_HIST_VIGENCIA_TABLE),
        "marca_barrio": assert_sql_table_name(DEFAULT_PG_MARCA_BARRIO_TABLE),
        "suc_excluidas": assert_sql_table_name(DEFAULT_PG_SUC_EXCLUIDAS_TABLE),
    }


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

TMP_SCHEMA_SURTIDO = {"c_articulo": "INTEGER"}
TMP_SCHEMA_HIST_VIGENCIA = {
    "c_sucu_empr": "INTEGER",
    "c_articulo": "INTEGER",
    "fecha_alta_hist": "TIMESTAMP",
    "fecha_baja_hist": "TIMESTAMP",
}
TMP_SCHEMA_MARCA_BARRIO = {
    "c_sucu_empr": "INTEGER",
    "c_articulo": "INTEGER",
    "m_habilitado_sucu": "VARCHAR(1)",
}
TMP_SCHEMA_SUC_EXCLUIDAS = {"c_sucu_empr": "INTEGER"}

SQL_QUERY_SURTIDO = """
SELECT DISTINCT
    CAST(st.C_ARTICULO AS INT) AS c_articulo
FROM repl.T060_STOCK st
"""

SQL_QUERY_HIST_VIGENCIA = """
SELECT
    CAST(hist.C_SUCU_EMPR AS INT) AS c_sucu_empr,
    CAST(hist.C_ARTICULO AS INT) AS c_articulo,
    MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'S' THEN hist.F_ALTA_SIST END) AS fecha_alta_hist,
    MAX(CASE WHEN hist.M_LISTO_PARA_VENTA_ACT = 'N' THEN hist.F_ALTA_SIST END) AS fecha_baja_hist
FROM repl.T804_HIST_MARCA_LISTO_PARA_VENTA hist
INNER JOIN (
    SELECT DISTINCT C_ARTICULO
    FROM repl.T060_STOCK
) surtido
    ON surtido.C_ARTICULO = hist.C_ARTICULO
GROUP BY hist.C_SUCU_EMPR, hist.C_ARTICULO
"""

SQL_QUERY_MARCA_BARRIO = """
SELECT
    CAST(mb.C_SUCU_EMPR AS INT) AS c_sucu_empr,
    CAST(mb.C_ARTICULO AS INT) AS c_articulo,
    CAST(mb.M_HABILITADO_SUCU AS VARCHAR(1)) AS m_habilitado_sucu
FROM repl.T051_ARTICULOS_SUCURSAL_BARRIO mb
INNER JOIN (
    SELECT DISTINCT C_ARTICULO
    FROM repl.T060_STOCK
) surtido
    ON surtido.C_ARTICULO = mb.C_ARTICULO
"""

SQL_QUERY_SUC_EXCLUIDAS = """
SELECT
    CAST(ex.C_SUCU_EMPR AS INT) AS c_sucu_empr
FROM dbo.SUCURSALES_EXCLUIDAS ex
"""

PG_QUERY_SURTIDO_TEMPLATE = """
SELECT DISTINCT
    st.c_articulo::integer AS c_articulo
FROM {stock_table} st
"""

PG_QUERY_HIST_VIGENCIA_TEMPLATE = """
SELECT
    hist.c_sucu_empr::integer AS c_sucu_empr,
    hist.c_articulo::integer AS c_articulo,
    MAX(CASE WHEN hist.m_listo_para_venta_act = 'S' THEN hist.f_alta_sist END) AS fecha_alta_hist,
    MAX(CASE WHEN hist.m_listo_para_venta_act = 'N' THEN hist.f_alta_sist END) AS fecha_baja_hist
FROM {hist_vigencia_table} hist
INNER JOIN (
    SELECT DISTINCT c_articulo
    FROM {stock_table}
) surtido
    ON surtido.c_articulo = hist.c_articulo
GROUP BY hist.c_sucu_empr, hist.c_articulo
"""

PG_QUERY_MARCA_BARRIO_TEMPLATE = """
SELECT
    mb.c_sucu_empr::integer AS c_sucu_empr,
    mb.c_articulo::integer AS c_articulo,
    mb.m_habilitado_sucu::varchar(1) AS m_habilitado_sucu
FROM {marca_barrio_table} mb
INNER JOIN (
    SELECT DISTINCT c_articulo
    FROM {stock_table}
) surtido
    ON surtido.c_articulo = mb.c_articulo
"""

PG_QUERY_SUC_EXCLUIDAS_TEMPLATE = """
SELECT
    ex.c_sucu_empr::integer AS c_sucu_empr
FROM {suc_excluidas_table} ex
"""

PG_INSERT_BASE_PRODUCTOS_HYBRID_TEMPLATE = """
WITH vigencia AS (
    SELECT
        suc.c_sucu_empr::integer AS c_sucu_empr,
        suc.c_articulo::integer AS c_articulo,
        CASE
            WHEN hv.fecha_alta_hist IS NOT NULL
             AND hv.fecha_alta_hist > suc.f_alta
                THEN hv.fecha_alta_hist
            ELSE suc.f_alta
        END AS fecha_registro,
        CASE
            WHEN hv.fecha_baja_hist IS NOT NULL
             AND hv.fecha_baja_hist >= CASE
                 WHEN hv.fecha_alta_hist IS NOT NULL
                  AND hv.fecha_alta_hist > suc.f_alta
                     THEN hv.fecha_alta_hist
                 ELSE suc.f_alta
             END
                THEN hv.fecha_baja_hist
            ELSE NULL
        END AS fecha_baja
    FROM src.t051_articulos_sucursal suc
    INNER JOIN tmp_bp_surtido sur
        ON sur.c_articulo = suc.c_articulo
    INNER JOIN src.t050_articulos art
        ON art.c_articulo = suc.c_articulo
       AND art.m_baja = 'N'
    LEFT JOIN tmp_bp_hist_vigencia hv
        ON hv.c_sucu_empr = suc.c_sucu_empr
       AND hv.c_articulo = suc.c_articulo
    WHERE suc.c_sucu_empr <> 300
)
INSERT INTO {table_name} (
    c_sucu_empr,
    c_articulo,
    c_proveedor_primario,
    abastecimiento,
    cod_cd,
    habilitado,
    fecha_registro,
    fecha_baja,
    q_peso_unit_art,
    m_vende_por_peso,
    unid_transferencia,
    q_unid_transferencia,
    pedido_min,
    frente_lineal,
    capacid_gondola,
    stock_minimo,
    cod_comprador,
    promocion,
    active_for_purchase,
    active_for_sale,
    active_on_mix,
    delivered_id,
    product_base_id,
    own_production,
    q_factor_compra,
    full_capacity_pallet,
    number_of_layers,
    number_of_boxes_per_layer,
    fuente_origen,
    fecha_extraccion,
    estado_sincronizacion
)
SELECT
    suc.c_sucu_empr::integer AS c_sucu_empr,
    suc.c_articulo::integer AS c_articulo,
    art.c_proveedor_primario::integer AS c_proveedor_primario,
    suc.c_sistematica::integer AS abastecimiento,
    CASE
        WHEN suc.c_sistematica = 0 THEN
            CASE WHEN suc.c_sucu_empr < 300 THEN '41CD' ELSE '82CD' END
        WHEN suc.c_sistematica = 1 THEN LPAD(suc.c_sucu_empr::text, 3, '0')
        WHEN suc.c_sistematica = 2 THEN 'XDOC'
        WHEN suc.c_sistematica = 3 THEN '82CD'
        ELSE NULL
    END AS cod_cd,
    CASE
        WHEN (
            (suc.c_sucu_empr < 300 AND suc.m_habilitado_sucu = 'N')
            OR
            (suc.c_sucu_empr > 300 AND mb.m_habilitado_sucu = 'N')
        )
        THEN 0 ELSE 1
    END AS habilitado,
    v.fecha_registro,
    COALESCE(v.fecha_baja, TIMESTAMP '2099-12-31') AS fecha_baja,
    art.q_peso_unit_art::double precision AS q_peso_unit_art,
    CASE WHEN art.m_vende_por_peso = 'S' THEN 1 ELSE 0 END AS m_vende_por_peso,
    0 AS unid_transferencia,
    1 AS q_unid_transferencia,
    1::double precision AS pedido_min,
    1 AS frente_lineal,
    1 AS capacid_gondola,
    1::double precision AS stock_minimo,
    art.c_comprador::integer AS cod_comprador,
    CASE WHEN suc.m_oferta_sucu = 'N' THEN 0 ELSE 1 END AS promocion,
    CASE
        WHEN (
            art.m_a_dar_de_baja = 'S'
            OR art.c_clasificacion_compra = 4
            OR (suc.c_sucu_empr < 300 AND suc.m_habilitado_sucu = 'N')
            OR (suc.c_sucu_empr > 300 AND mb.m_habilitado_sucu = 'N')
        )
        THEN 0 ELSE 1
    END AS active_for_purchase,
    CASE WHEN suc.m_listo_para_venta_sucu = 'N' THEN 0 ELSE 1 END AS active_for_sale,
    CASE
        WHEN art.c_familia = 4 OR art.m_a_dar_de_baja = 'S' THEN 0
        ELSE 1
    END AS active_on_mix,
    CASE
        WHEN suc.c_sistematica = 0
            THEN CASE WHEN suc.c_sucu_empr < 300 THEN '41CD' ELSE '82' END
        WHEN suc.c_sistematica = 1
            THEN prov.c_proveedor::text
        ELSE NULL
    END AS delivered_id,
    '' AS product_base_id,
    0 AS own_production,
    prov.q_factor_proveedor::integer AS q_factor_compra,
    (prov.u_piso_paletizado * prov.u_altura_paletizado)::integer AS full_capacity_pallet,
    prov.u_altura_paletizado::integer AS number_of_layers,
    CASE
        WHEN art.m_vende_por_peso = 'N'
            THEN prov.u_piso_paletizado::double precision
        ELSE prov.q_factor_proveedor::double precision
    END AS number_of_boxes_per_layer,
    '{source_label}' AS fuente_origen,
    CURRENT_TIMESTAMP AS fecha_extraccion,
    0 AS estado_sincronizacion
FROM src.t051_articulos_sucursal suc
INNER JOIN tmp_bp_surtido sur
    ON sur.c_articulo = suc.c_articulo
INNER JOIN src.t050_articulos art
    ON art.c_articulo = suc.c_articulo
LEFT JOIN tmp_bp_marca_barrio mb
    ON mb.c_sucu_empr = suc.c_sucu_empr
   AND mb.c_articulo = suc.c_articulo
LEFT JOIN src.t052_articulos_proveedor prov
    ON prov.c_articulo = art.c_articulo
   AND prov.c_proveedor = art.c_proveedor_primario
INNER JOIN src.t100_empresa_suc suc_mae
    ON suc_mae.c_sucu_empr = suc.c_sucu_empr
   AND suc_mae.m_sucu_virtual = 'N'
INNER JOIN vigencia v
    ON v.c_sucu_empr = suc.c_sucu_empr
   AND v.c_articulo = suc.c_articulo
WHERE suc.c_sucu_empr NOT IN (
    SELECT ex.c_sucu_empr
    FROM tmp_bp_sucursales_excluidas ex
)
  AND art.m_a_dar_de_baja <> 'S'
  AND suc.m_habilitado_sucu = 'S'
"""


def transformar_chunk(df, chunk_index, task_logger):
    df["FUENTE_ORIGEN"] = "SP_BASE_PRODUCTOS_VIGNTS"
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


def create_temp_table_statement(schema_dict, table_name):
    return (
        create_table_statement(schema_dict, table_name)
        .replace("CREATE TABLE", "CREATE TEMP TABLE", 1)
        + " ON COMMIT DROP"
    )


def load_sqlserver_query_into_pg_temp(
    *,
    pg_cur,
    table_name,
    schema_dict,
    query,
    task_logger,
    chunk_size,
):
    pg_cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_cur.execute(create_temp_table_statement(schema_dict, table_name))

    total_rows = 0
    total_chunks = 0
    for chunk_index, raw_chunk in enumerate(
        pd.read_sql(query, get_sql_engine(), chunksize=chunk_size),
        start=1,
    ):
        aligned = align_dataframe_to_schema(raw_chunk, schema_dict)
        copy_dataframe_to_postgres(pg_cur, table_name, aligned)
        total_rows += len(aligned)
        total_chunks += 1

    task_logger.info(
        "Temp cargada desde SQL Server | tabla=%s | filas=%s | chunks=%s",
        table_name,
        total_rows,
        total_chunks,
    )
    return total_rows, total_chunks


def load_pg_query_into_temp(
    *,
    pg_cur,
    table_name,
    schema_dict,
    query,
    task_logger,
):
    pg_cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    pg_cur.execute(create_temp_table_statement(schema_dict, table_name))
    columns_sql = ", ".join(f'"{column}"' for column in schema_dict)
    pg_cur.execute(f"INSERT INTO {table_name} ({columns_sql}) {query}")
    total_rows = pg_cur.rowcount if pg_cur.rowcount is not None else 0
    task_logger.info(
        "Temp cargada desde PostgreSQL | tabla=%s | filas=%s",
        table_name,
        total_rows,
    )
    return total_rows


def crear_indices_temporales(pg_cur):
    pg_cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tmp_bp_surtido_art
            ON tmp_bp_surtido (c_articulo);
        CREATE INDEX IF NOT EXISTS idx_tmp_bp_hist_vigencia_sucu_art
            ON tmp_bp_hist_vigencia (c_sucu_empr, c_articulo);
        CREATE INDEX IF NOT EXISTS idx_tmp_bp_marca_barrio_sucu_art
            ON tmp_bp_marca_barrio (c_sucu_empr, c_articulo);
        CREATE INDEX IF NOT EXISTS idx_tmp_bp_suc_excluidas_sucu
            ON tmp_bp_sucursales_excluidas (c_sucu_empr);
        """
    )


def cargar_base_productos_hibrida_impl(task_logger, table_destino: str):
    started_at = perf_counter()
    sql_chunks = 0

    with open_pg_conn_local() as conn:
        try:
            with conn.cursor() as cur:
                rows, chunks = load_sqlserver_query_into_pg_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_surtido",
                    schema_dict=TMP_SCHEMA_SURTIDO,
                    query=SQL_QUERY_SURTIDO,
                    task_logger=task_logger,
                    chunk_size=READ_CHUNK_SIZE,
                )
                sql_chunks += chunks

                _, chunks = load_sqlserver_query_into_pg_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_hist_vigencia",
                    schema_dict=TMP_SCHEMA_HIST_VIGENCIA,
                    query=SQL_QUERY_HIST_VIGENCIA,
                    task_logger=task_logger,
                    chunk_size=READ_CHUNK_SIZE,
                )
                sql_chunks += chunks

                _, chunks = load_sqlserver_query_into_pg_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_marca_barrio",
                    schema_dict=TMP_SCHEMA_MARCA_BARRIO,
                    query=SQL_QUERY_MARCA_BARRIO,
                    task_logger=task_logger,
                    chunk_size=READ_CHUNK_SIZE,
                )
                sql_chunks += chunks

                _, chunks = load_sqlserver_query_into_pg_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_sucursales_excluidas",
                    schema_dict=TMP_SCHEMA_SUC_EXCLUIDAS,
                    query=SQL_QUERY_SUC_EXCLUIDAS,
                    task_logger=task_logger,
                    chunk_size=1000,
                )
                sql_chunks += chunks

                crear_indices_temporales(cur)

                cur.execute(f"DROP TABLE IF EXISTS {table_destino} CASCADE")
                cur.execute(create_table_statement(ESQUEMA_BASE_PRODUCTOS, table_destino))
                task_logger.info("Tabla destino recreada en modo hybrid: %s", table_destino)

                cur.execute(
                    PG_INSERT_BASE_PRODUCTOS_HYBRID_TEMPLATE.format(
                        table_name=table_destino,
                        source_label="BASE_PRODUCTOS_HYBRID_SRC",
                    )
                )
                inserted_rows = cur.rowcount if cur.rowcount is not None else 0

            conn.commit()
        except Exception:
            conn.rollback()
            task_logger.exception(
                "Se revirtió la transacción de carga hybrid sobre %s",
                table_destino,
            )
            raise

    elapsed = perf_counter() - started_at
    task_logger.info(
        "Carga hybrid finalizada | tabla_destino=%s | filas=%s | chunks_sqlserver=%s | duracion=%.2fs",
        table_destino,
        inserted_rows,
        sql_chunks,
        elapsed,
    )
    return {"rows": inserted_rows, "chunks": sql_chunks, "seconds": elapsed}


def cargar_base_productos_pg_src_impl(task_logger, table_destino: str):
    started_at = perf_counter()
    source_tables = get_pg_source_tables()

    with open_pg_conn_local() as conn:
        try:
            with conn.cursor() as cur:
                rows_surtido = load_pg_query_into_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_surtido",
                    schema_dict=TMP_SCHEMA_SURTIDO,
                    query=PG_QUERY_SURTIDO_TEMPLATE.format(
                        stock_table=source_tables["stock"],
                    ),
                    task_logger=task_logger,
                )

                load_pg_query_into_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_hist_vigencia",
                    schema_dict=TMP_SCHEMA_HIST_VIGENCIA,
                    query=PG_QUERY_HIST_VIGENCIA_TEMPLATE.format(
                        hist_vigencia_table=source_tables["hist_vigencia"],
                        stock_table=source_tables["stock"],
                    ),
                    task_logger=task_logger,
                )

                load_pg_query_into_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_marca_barrio",
                    schema_dict=TMP_SCHEMA_MARCA_BARRIO,
                    query=PG_QUERY_MARCA_BARRIO_TEMPLATE.format(
                        marca_barrio_table=source_tables["marca_barrio"],
                        stock_table=source_tables["stock"],
                    ),
                    task_logger=task_logger,
                )

                load_pg_query_into_temp(
                    pg_cur=cur,
                    table_name="tmp_bp_sucursales_excluidas",
                    schema_dict=TMP_SCHEMA_SUC_EXCLUIDAS,
                    query=PG_QUERY_SUC_EXCLUIDAS_TEMPLATE.format(
                        suc_excluidas_table=source_tables["suc_excluidas"],
                    ),
                    task_logger=task_logger,
                )

                crear_indices_temporales(cur)

                cur.execute(f"DROP TABLE IF EXISTS {table_destino} CASCADE")
                cur.execute(create_table_statement(ESQUEMA_BASE_PRODUCTOS, table_destino))
                task_logger.info("Tabla destino recreada en modo pg_src: %s", table_destino)

                cur.execute(
                    PG_INSERT_BASE_PRODUCTOS_HYBRID_TEMPLATE.format(
                        table_name=table_destino,
                        source_label="BASE_PRODUCTOS_PG_SRC",
                    )
                )
                inserted_rows = cur.rowcount if cur.rowcount is not None else 0

            conn.commit()
        except Exception:
            conn.rollback()
            task_logger.exception(
                "Se revirtió la transacción de carga pg_src sobre %s",
                table_destino,
            )
            raise

    elapsed = perf_counter() - started_at
    task_logger.info(
        "Carga pg_src finalizada | tabla_destino=%s | filas=%s | surtido=%s | duracion=%.2fs",
        table_destino,
        inserted_rows,
        rows_surtido,
        elapsed,
    )
    return {"rows": inserted_rows, "chunks": 0, "seconds": elapsed}


@task(name="cargar_base_productos_pg")
def cargar_base_productos(
    source_mode: str = DEFAULT_BASE_PRODUCTOS_SOURCE_MODE,
    table_destino: str = DEFAULT_TABLE_DESTINO,
):
    task_logger = get_run_logger()
    table_destino = assert_sql_table_name(table_destino)

    if source_mode == "sqlserver_sp":
        task_logger.info(
            "Carga base_productos_vigentes en modo sqlserver_sp | sp=%s | tabla_destino=%s",
            SP_NAME,
            table_destino,
        )
        return replace_table_from_query_chunks(
            query=f"EXEC {SP_NAME}",
            sql_engine=get_sql_engine(),
            pg_conn_factory=open_pg_conn_local,
            table_name=table_destino,
            schema_dict=ESQUEMA_BASE_PRODUCTOS,
            transform_chunk=transformar_chunk,
            logger=task_logger, # pyright: ignore[reportArgumentType]
            read_chunk_size=READ_CHUNK_SIZE,
        )

    if source_mode == "hybrid_src":
        task_logger.info(
            "Carga base_productos_vigentes en modo hybrid_src | src=PostgreSQL + remanentes SQL Server | tabla_destino=%s",
            table_destino,
        )
        return cargar_base_productos_hibrida_impl(task_logger, table_destino)

    if source_mode == "pg_src":
        if not ENABLE_EXPERIMENTAL_PG_SRC:
            raise RuntimeError(
                "El modo pg_src esta preparado pero deshabilitado por ahora. "
                "Requiere sincronizar en PostgreSQL las fuentes pendientes: "
                "src.t804_hist_marca_listo_para_venta y "
                "src.t051_articulos_sucursal_barrio. "
                "Para pruebas controladas, configurar "
                "BASE_PRODUCTOS_ENABLE_EXPERIMENTAL_PG_SRC=1."
            )
        task_logger.info(
            "Carga base_productos_vigentes en modo pg_src | src=PostgreSQL puro | tabla_destino=%s",
            table_destino,
        )
        return cargar_base_productos_pg_src_impl(task_logger, table_destino)

    raise ValueError(
        "BASE_PRODUCTOS_SOURCE_MODE / mode invalido. Valores soportados: "
        "'sqlserver_sp', 'hybrid_src', 'pg_src'."
    )


@task(name="eliminar_duplicados_base_productos_vigentes")
def eliminar_duplicados(table_destino: str = DEFAULT_TABLE_DESTINO):
    task_logger = get_run_logger()
    table_destino = assert_sql_table_name(table_destino)
    query_delete_returning = f"""
        WITH cte AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY c_sucu_empr, c_articulo, c_proveedor_primario
                       ORDER BY fecha_extraccion DESC NULLS LAST
                   ) AS rn
            FROM {table_destino}
        )
        DELETE FROM {table_destino} b
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
            "Depuración completada en %s | duplicados_eliminados=%s",
            table_destino,
            eliminados,
        )
        return eliminados
    except Exception as exc:
        task_logger.error("Error al eliminar duplicados: %s", exc)
        raise


@flow(name="obtener_base_productos_vigentes", persist_result=False)
def capturar_base_articulos(
    source_mode: str = DEFAULT_BASE_PRODUCTOS_SOURCE_MODE,
    table_destino: str = DEFAULT_TABLE_DESTINO,
):
    flow_logger = get_run_logger()
    table_destino = assert_sql_table_name(table_destino)
    try:
        load_result = cargar_base_productos.with_options(
            name="Carga Base Productos Vigentes",
        ).submit(source_mode=source_mode, table_destino=table_destino).result()

        deleted_rows = eliminar_duplicados.with_options(
            name="Eliminar Duplicados Base Productos Vigentes",
        ).submit(table_destino=table_destino).result()

        flow_logger.info(
            "Flujo completado | tabla=%s | modo=%s | filas=%s | chunks=%s | duplicados_eliminados=%s | duracion_carga=%.2fs",
            table_destino,
            source_mode,
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
    payload = ast.literal_eval(args[0]) if args else None
    source_mode, table_destino = resolve_runtime_options(payload)
    capturar_base_articulos(source_mode=source_mode, table_destino=table_destino)
    logger.info("Proceso finalizado: obtener_base_productos_vigentes")
