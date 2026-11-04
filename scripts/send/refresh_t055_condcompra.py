"""Refresh the SGM purchase-cost table into diarco_data without CDC.

The destination is replaced only after the complete SQL Server snapshot has
been copied and checked in a PostgreSQL transaction. Any failure rolls back
the replacement and leaves the previous src table available.
"""

from __future__ import annotations

import io
import logging
import os

import psycopg2
import pyodbc
from dotenv import load_dotenv
from psycopg2 import sql


log = logging.getLogger(__name__)
SOURCE_TABLE = "[repl].[T055_ARTICULOS_CONDCOMPRA_COSTOS]"
TARGET_TABLE = "src.t055_articulos_condcompra_costos"
METADATA_COLUMNS = frozenset({
    "fuente_origen", "fecha_extraccion", "cdc_lsn", "estado_sincronizacion"
})


def _copy_value(value: object) -> str:
    if value is None:
        return r"\N"
    return (str(value).replace("\\", "\\\\").replace("\t", r"\t")
            .replace("\n", r"\n").replace("\r", r"\r"))


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return value


def refresh_t055_condcompra(*, batch_size: int = 5000, logger=None) -> dict[str, int]:
    """Copy into a temporary table, then atomically replace src.T055."""
    if batch_size <= 0:
        raise ValueError("batch_size debe ser positivo")
    output = logger or log
    load_dotenv()
    driver = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
    sql_conn = pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={_require_env('SQL_SERVER')};"
        f"DATABASE={_require_env('SQL_DATABASE')};UID={_require_env('SQL_USER')};"
        f"PWD={_require_env('SQL_PASSWORD')}",
        timeout=0,
    )
    pg_conn = None
    try:
        pg_conn = psycopg2.connect(
            host=_require_env("PG_HOST"), port=_require_env("PG_PORT"),
            dbname=_require_env("PG_DB"), user=_require_env("PG_USER"),
            password=_require_env("PG_PASSWORD"),
            application_name="etl_diarco_scheduled_t055_refresh",
        )
        source = sql_conn.cursor()
        source.execute(f"SELECT TOP 0 * FROM {SOURCE_TABLE}")
        available = {column[0].lower() for column in source.description}
        with pg_conn.cursor() as target:
            target.execute("""SELECT column_name FROM information_schema.columns
                WHERE table_schema='src' AND table_name='t055_articulos_condcompra_costos'
                ORDER BY ordinal_position""")
            columns = [row[0] for row in target.fetchall()]
            if not columns:
                raise RuntimeError(f"No existe {TARGET_TABLE}")
            data_columns = [col for col in columns if col not in METADATA_COLUMNS]
            missing = set(data_columns) - available
            if missing:
                raise RuntimeError(f"Columnas T055 faltantes en SGM: {sorted(missing)}")
            quoted = ", ".join(f"[{col.replace(']', ']]')}]" for col in data_columns)
            source.execute(f"SELECT {quoted} FROM {SOURCE_TABLE}")
            target.execute("""CREATE TEMP TABLE t055_scheduled_stage
                (LIKE src.t055_articulos_condcompra_costos INCLUDING DEFAULTS)
                ON COMMIT DROP""")
            copy_sql = sql.SQL("COPY t055_scheduled_stage ({}) FROM STDIN WITH (FORMAT text, NULL '\\N')").format(
                sql.SQL(", ").join(sql.Identifier(col) for col in data_columns)
            ).as_string(pg_conn)
            copied = 0
            while batch := source.fetchmany(batch_size):
                stream = io.StringIO()
                for row in batch:
                    stream.write("\t".join(_copy_value(value) for value in row) + "\n")
                stream.seek(0)
                target.copy_expert(copy_sql, stream)
                copied += len(batch)
                if copied % 100000 < len(batch):
                    output.info("T055 copiada a staging: %s filas", copied)
            target.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
            previous = target.fetchone()[0]
            if copied < 1_000_000 or (previous and copied < previous * 0.8):
                raise RuntimeError(
                    f"T055 incompleta: fuente={copied}, destino anterior={previous}"
                )
            target.execute("SET LOCAL lock_timeout = '5s'")
            target.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
            insert_cols = [*data_columns, "fuente_origen", "fecha_extraccion", "estado_sincronizacion"]
            target.execute(sql.SQL("""INSERT INTO {} ({})
                SELECT {}, %s, now() AT TIME ZONE 'America/Argentina/Buenos_Aires', 1
                FROM t055_scheduled_stage""").format(
                    sql.Identifier("src", "t055_articulos_condcompra_costos"),
                    sql.SQL(", ").join(sql.Identifier(col) for col in insert_cols),
                    sql.SQL(", ").join(sql.Identifier(col) for col in data_columns),
                ), ("scheduled:repl.T055_ARTICULOS_CONDCOMPRA_COSTOS",))
            if target.rowcount != copied:
                raise RuntimeError(f"T055: copia final {target.rowcount} != {copied}")
        pg_conn.commit()
        result = {"previous_rows": previous, "refreshed_rows": copied}
        output.info("T055 refrescada: %s", result)
        return result
    except BaseException:
        if pg_conn is not None:
            pg_conn.rollback()
        raise
    finally:
        if pg_conn is not None:
            pg_conn.close()
        sql_conn.close()
